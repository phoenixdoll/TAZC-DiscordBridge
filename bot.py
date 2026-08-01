"""
TAZC Discord Radio Bridge.

Bridges ONE Project Zomboid radio frequency (100.0 MHz, filtered on the Lua
side -- see TAZC_DiscordBridge.lua) to ONE Discord channel, in both
directions, via two plain files on the game server reached over SFTP:

  outbox.txt  (game -> Discord)  "<unix-seconds>|<displayName>|<discordMessageId-or-empty>|<message>"
  inbox.txt   (Discord -> game)  "<displayName>|<discordMessageId>|<message>"

A Discord-origin line's id round-trips through the Lua side unchanged and
comes back on the matching outbox line, purely so this bot can delete the
original plain-text Discord message once it posts the in-character,
packet-loss-corrupted version -- avoids having both the raw message and its
"radio" echo sitting in the channel at once. Requires the bot to have
**Manage Messages** permission in that channel (Send Messages alone is not
enough to delete someone else's message).

This process owns nothing about PZ's radio/packet-loss logic -- that all
happens Lua-side. This script only: polls outbox.txt and posts new lines to
Discord, and appends new Discord messages from the configured channel to
inbox.txt.
"""

import asyncio
import logging
import os
import threading
from pathlib import PurePosixPath

import discord
import paramiko
from discord.ext import tasks
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("tazc-bridge")

# ============================================================================
# CONFIGURATION (from .env -- see .env.example)
# ============================================================================

DISCORD_BOT_TOKEN = os.environ["DISCORD_BOT_TOKEN"]
DISCORD_CHANNEL_ID = int(os.environ["DISCORD_CHANNEL_ID"])

SFTP_HOST = os.environ["SFTP_HOST"]
SFTP_PORT = int(os.environ.get("SFTP_PORT", "22"))
SFTP_USERNAME = os.environ["SFTP_USERNAME"]
SFTP_KEY_PATH = os.environ.get("SFTP_KEY_PATH") or None
SFTP_PASSWORD = os.environ.get("SFTP_PASSWORD") or None

ZOMBOID_DATA_PATH = os.environ["ZOMBOID_DATA_PATH"].rstrip("/")
# Must match TAZC_DiscordBridge.OUTBOX_FILE / .INBOX_FILE (relative paths,
# resolved by PZ's Lua sandbox under Zomboid/Lua/).
REMOTE_OUTBOX = f"{ZOMBOID_DATA_PATH}/Lua/TAZC/discordbridge/outbox.txt"
REMOTE_INBOX = f"{ZOMBOID_DATA_PATH}/Lua/TAZC/discordbridge/inbox.txt"

MAX_MESSAGE_LENGTH = int(os.environ.get("MAX_MESSAGE_LENGTH", "500"))
POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "3"))


# ============================================================================
# SFTP
# ============================================================================

class SftpBridge:
    """
    Thin wrapper around one paramiko SFTP connection, with reconnect on
    failure.

    paramiko's SSHClient/SFTPClient is NOT safe for concurrent use from
    multiple threads. Every public method here is called via
    run_in_executor(None, ...), which dispatches to asyncio's default
    ThreadPoolExecutor -- if two calls land close together (e.g. two
    Discord messages a few seconds apart, or a poll_outbox tick overlapping
    an on_message write), they run on DIFFERENT worker threads at the same
    time. Without a lock, concurrent access into the same paramiko
    connection can deadlock inside paramiko's own internals with no
    timeout, silently wedging that thread forever -- previously showed up
    as the whole bridge going quiet after working fine once. _lock
    serializes every public method below so concurrent callers queue up
    instead of colliding.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._client: paramiko.SSHClient | None = None
        self._sftp: paramiko.SFTPClient | None = None

    def _connect(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connect_kwargs = {"hostname": SFTP_HOST, "port": SFTP_PORT, "username": SFTP_USERNAME}
        if SFTP_KEY_PATH:
            connect_kwargs["key_filename"] = SFTP_KEY_PATH
        elif SFTP_PASSWORD:
            connect_kwargs["password"] = SFTP_PASSWORD
        else:
            raise RuntimeError("Set either SFTP_KEY_PATH or SFTP_PASSWORD in .env")
        client.connect(**connect_kwargs)
        self._client = client
        self._sftp = client.open_sftp()
        log.info("SFTP connected to %s", SFTP_HOST)

    def _ensure_connected(self):
        log.debug("_ensure_connected: start")
        if self._sftp is not None:
            try:
                self._sftp.listdir(".")
                log.debug("_ensure_connected: existing connection alive")
                return
            except Exception:
                log.warning("SFTP connection appears dead, reconnecting")
                self.close()
        self._connect()
        log.info("_ensure_connected: (re)connected")

    def _mkdirs(self, remote_dir: str):
        """paramiko has no mkdir -p; walk the path, ignoring already-exists.
        Plain string splitting, not PurePosixPath.parts -- that yields a
        leading '/' as its own part for an absolute path, which double-
        slashes every segment built from it ('//server-data', not
        '/server-data'), and paths starting with exactly two slashes have
        unspecified behaviour on POSIX systems."""
        segments = [p for p in remote_dir.split("/") if p]
        current = ""
        for segment in segments:
            current += "/" + segment
            try:
                self._sftp.mkdir(current)
            except IOError:
                pass  # already exists

    def read_and_clear(self, remote_path: str) -> list[str]:
        """Read every non-empty line from remote_path, then truncate it.
        Returns [] if the file doesn't exist yet (Lua hasn't written it)."""
        with self._lock:
            self._ensure_connected()
            try:
                with self._sftp.open(remote_path, "r") as f:
                    lines = [line.rstrip("\n").rstrip("\r") for line in f.readlines()]
                    lines = [line for line in lines if line != ""]
            except IOError:
                return []

            if lines:
                with self._sftp.open(remote_path, "w"):
                    pass  # truncate

            return lines

    def append_line(self, remote_path: str, line: str):
        with self._lock:
            self._append_line_locked(remote_path, line)

    def _append_line_locked(self, remote_path: str, line: str):
        log.info("append_line: start path=%s", remote_path)
        self._ensure_connected()
        remote_dir = str(PurePosixPath(remote_path).parent)
        try:
            log.info("append_line: attempting open(a)")
            with self._sftp.open(remote_path, "a") as f:
                f.write(line + "\n")
            log.info("append_line: write succeeded on first attempt")
        except IOError as e:
            # Most likely the directory doesn't exist yet (fresh server,
            # never had a 100MHz transmission to auto-create it). Create it
            # and retry once.
            log.info("append_line: first open failed (%r), mkdirs then retry", e)
            self._mkdirs(remote_dir)
            log.info("append_line: mkdirs done, retrying open(a)")
            with self._sftp.open(remote_path, "a") as f:
                f.write(line + "\n")
            log.info("append_line: write succeeded on retry")

    def close(self):
        if self._sftp is not None:
            try:
                self._sftp.close()
            except Exception:
                pass
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
        self._sftp = None
        self._client = None


# ============================================================================
# DISCORD BOT
# ============================================================================

intents = discord.Intents.default()
intents.message_content = True  # required to read message text; must also
                                 # be enabled in the Developer Portal


class TazcBridgeClient(discord.Client):
    def __init__(self):
        super().__init__(intents=intents)
        self.sftp = SftpBridge()

    async def setup_hook(self):
        self.poll_outbox.start()

    async def on_ready(self):
        log.info("Logged in as %s", self.user)

    async def on_message(self, message: discord.Message):
        log.info("on_message: channel=%s (configured=%s) author=%s content=%r",
                  message.channel.id, DISCORD_CHANNEL_ID, message.author, message.content)
        if message.author.bot:
            return
        if message.channel.id != DISCORD_CHANNEL_ID:
            return

        text = message.content.strip()
        if not text:
            return

        if len(text) > MAX_MESSAGE_LENGTH:
            await message.reply(
                f"Too long for radio -- max {MAX_MESSAGE_LENGTH} characters "
                f"(this server's TAZC_Config.MaxMessageLength), yours was {len(text)}. "
                f"Not sent."
            )
            return

        display_name = message.author.display_name
        line = f"{_escape_field(display_name)}|{message.id}|{_escape_field(text)}"

        try:
            await asyncio.get_event_loop().run_in_executor(
                None, self.sftp.append_line, REMOTE_INBOX, line
            )
        except Exception:
            log.exception("Failed to write inbox line")
            await message.add_reaction("\N{WARNING SIGN}")
            return

        await message.add_reaction("\N{ANTENNA WITH BARS}")

    @tasks.loop(seconds=POLL_INTERVAL_SECONDS)
    async def poll_outbox(self):
        try:
            lines = await asyncio.get_event_loop().run_in_executor(
                None, self.sftp.read_and_clear, REMOTE_OUTBOX
            )
        except Exception:
            log.exception("Failed to poll outbox")
            return

        if not lines:
            return

        channel = self.get_channel(DISCORD_CHANNEL_ID)
        if channel is None:
            log.error("Configured channel %s not found/visible to bot", DISCORD_CHANNEL_ID)
            return

        for line in lines:
            parsed = _parse_outbox_line(line)
            if parsed is None:
                log.warning("Malformed outbox line skipped: %r", line)
                continue
            _timestamp, display_name, discord_message_id, message_text = parsed

            if discord_message_id is not None:
                try:
                    original = await channel.fetch_message(discord_message_id)
                    await original.delete()
                except discord.NotFound:
                    pass  # already gone -- nothing to clean up
                except discord.Forbidden:
                    log.warning(
                        "Missing Manage Messages permission in channel %s; "
                        "cannot delete original message %s",
                        DISCORD_CHANNEL_ID, discord_message_id,
                    )
                except Exception:
                    log.exception("Failed to delete original message %s", discord_message_id)

            # Discord renders TAZC's existing *word* static markers as
            # italic automatically -- no extra formatting needed on them.
            await channel.send(f"**{display_name}:** {message_text}")

    @poll_outbox.before_loop
    async def before_poll_outbox(self):
        await self.wait_until_ready()

    async def close(self):
        self.poll_outbox.cancel()
        self.sftp.close()
        await super().close()


def _escape_field(value: str) -> str:
    """Mirror TAZC_DiscordBridge.lua's cleanField: strip control chars and
    the '|' field separator so this side can't corrupt the line format
    either."""
    cleaned = "".join(ch if ch.isprintable() else " " for ch in value)
    return cleaned.replace("|", "/")


def _parse_outbox_line(line: str):
    parts = line.split("|", 3)
    if len(parts) != 4:
        return None
    timestamp_str, display_name, message_id_str, message_text = parts
    try:
        timestamp = int(timestamp_str)
    except ValueError:
        timestamp = 0
    try:
        message_id = int(message_id_str) if message_id_str else None
    except ValueError:
        message_id = None
    return timestamp, display_name, message_id, message_text


def main():
    client = TazcBridgeClient()
    client.run(DISCORD_BOT_TOKEN)


if __name__ == "__main__":
    main()
