"""
TAZC Discord Radio Bridge.

Bridges ONE Project Zomboid radio frequency (100.0 MHz, filtered on the Lua
side -- see TAZC_DiscordBridge.lua) to ONE Discord channel, in both
directions, via two plain files on the game server reached over SFTP:

  outbox.txt  (game -> Discord)  "<unix-seconds>|<displayName>|<message>"
  inbox.txt   (Discord -> game)  "<displayName>|<message>"

This process owns nothing about PZ's radio/packet-loss logic -- that all
happens Lua-side. This script only: polls outbox.txt and posts new lines to
Discord, and appends new Discord messages from the configured channel to
inbox.txt.
"""

import asyncio
import logging
import os
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
    failure. Not thread-safe -- only ever called from the bot's single
    asyncio event loop via run_in_executor, never concurrently.
    """

    def __init__(self):
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
        if self._sftp is not None:
            try:
                self._sftp.listdir(".")
                return
            except Exception:
                log.warning("SFTP connection appears dead, reconnecting")
                self.close()
        self._connect()

    def _mkdirs(self, remote_dir: str):
        """paramiko has no mkdir -p; walk the path, ignoring already-exists."""
        parts = PurePosixPath(remote_dir).parts
        current = ""
        for part in parts:
            current = current + "/" + part if current else part
            if not current.startswith("/"):
                current = "/" + current
            try:
                self._sftp.mkdir(current)
            except IOError:
                pass  # already exists

    def read_and_clear(self, remote_path: str) -> list[str]:
        """Read every non-empty line from remote_path, then truncate it.
        Returns [] if the file doesn't exist yet (Lua hasn't written it)."""
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
        self._ensure_connected()
        remote_dir = str(PurePosixPath(remote_path).parent)
        try:
            with self._sftp.open(remote_path, "a") as f:
                f.write(line + "\n")
        except IOError:
            # Most likely the directory doesn't exist yet (fresh server,
            # never had a 100MHz transmission to auto-create it). Create it
            # and retry once.
            self._mkdirs(remote_dir)
            with self._sftp.open(remote_path, "a") as f:
                f.write(line + "\n")

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
        line = f"{_escape_field(display_name)}|{_escape_field(text)}"

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
            _timestamp, display_name, message_text = parsed
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
    parts = line.split("|", 2)
    if len(parts) != 3:
        return None
    timestamp_str, display_name, message_text = parts
    try:
        timestamp = int(timestamp_str)
    except ValueError:
        timestamp = 0
    return timestamp, display_name, message_text


def main():
    client = TazcBridgeClient()
    client.run(DISCORD_BOT_TOKEN)


if __name__ == "__main__":
    main()
