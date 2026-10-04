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

Optionally (see CHARACTERS_FILE_PATH) enforces that whoever posts has their
approved character's name somewhere in their Discord nickname before
relaying, unless they hold a NAME_CHECK_BYPASS_ROLE_IDS role -- otherwise
the message is deleted and the author DMed why (see handle_name_mismatch).
Reads WhitelistManager's characters.json, written by its /approvecharacter
command, and falls back to accounts.json + the live status.txt snapshot
for accounts characters.json doesn't cover (see check_character_name) --
only works when both bots run on the same machine. Gated behind
NAME_CHECK_ENFORCE (default off): while off, every check still runs and
logs its verdict, but never actually blocks anyone -- a dry run to gauge
the false-positive rate before enforcing for real.
"""

import asyncio
import json
import logging
import os
import threading
from pathlib import Path, PurePosixPath

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

# Optional: enforce that whoever posts in the 100MHz channel has their
# approved character's name somewhere in their Discord display name (see
# check_character_name below), unless they hold one of these role IDs.
# Leave CHARACTERS_FILE_PATH unset to disable this check entirely -- every
# message relays unconditionally either way, same as before this feature
# existed.
CHARACTERS_FILE_PATH = os.environ.get("CHARACTERS_FILE_PATH") or None
NAME_CHECK_BYPASS_ROLE_IDS = {
    int(r) for r in os.environ.get("NAME_CHECK_BYPASS_ROLE_IDS", "").split(",") if r.strip()
}
# Pinged in the channel if a blocked poster's DMs are closed (so the warning
# has to go somewhere visible instead of silently vanishing). Optional --
# leave unset to just post the fallback notice without pinging anyone.
SENIOR_STAFF_ROLE_ID = os.environ.get("SENIOR_STAFF_ROLE_ID") or None

# Fallback for when characters.json has no entry (or a stale one) for the
# poster: WhitelistManager's accounts.json (Discord ID -> PZ login
# username, written only by staff running /whitelist -- not guessable or
# spoofable by typing a nickname) tells us THIS account's real username,
# and the live status snapshot (same file /players reads, see
# REMOTE_STATUS_FILE) tells us that username's CURRENT in-game character
# name. Deliberately scoped to the poster's own account only -- a fallback
# that searched the whole online player list for ANY matching character
# name would let someone impersonate another player's character just by
# copying their name into their own nickname.
ACCOUNTS_FILE_PATH = os.environ.get("ACCOUNTS_FILE_PATH") or None
# Must match WhitelistManager's REMOTE_STATUS_FILE (same file /players,
# /time, /weather read) -- same ZOMBOID_DATA_PATH as the outbox/inbox files
# above, just a different one TAZC_Bridge.lua also writes.
REMOTE_STATUS_FILE = f"{ZOMBOID_DATA_PATH}/Lua/TAZC/discordbridge/status.txt"

# Safety switch for rolling this whole feature out: while False (the
# default), every check below still runs and logs what it WOULD have done,
# but never actually deletes a message or blocks anyone -- every message
# relays exactly as it did before this feature existed. Flip to True only
# after reviewing a few days of these logs and confirming the false-positive
# rate (e.g. from Unicode-mangled usernames) is acceptable.
NAME_CHECK_ENFORCE = os.environ.get("NAME_CHECK_ENFORCE", "false").strip().lower() == "true"


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

    def read_status(self, remote_path: str) -> list[str]:
        """Same read as read_and_clear, but non-destructive -- never
        truncates. WhitelistManager already owns clearing this exact file
        as part of its own /players polling; if this bot also cleared it,
        the two would race and steal each other's updates. Lua appends
        rather than overwrites, so a plain read here is always safe."""
        with self._lock:
            self._ensure_connected()
            try:
                with self._sftp.open(remote_path, "r") as f:
                    lines = [line.rstrip("\n").rstrip("\r") for line in f.readlines()]
                    return [line for line in lines if line != ""]
            except IOError:
                return []

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
# CHARACTER NAME CHECK
# ============================================================================

def character_name_matches(character_name: str, display_name: str) -> bool:
    """True if ANY single word of character_name (case-insensitive) appears
    in display_name -- e.g. approved character "John Reed" matches a
    Discord nickname of just "Reed" or just "John", not only the full
    "John Reed" together. Deliberately lenient: a short nickname
    (surname-only, first-name-only) is common and shouldn't get blocked
    just for not spelling out the whole character name."""
    words = [w for w in character_name.lower().split() if w]
    display_lower = display_name.lower()
    return any(word in display_lower for word in words)


def load_characters() -> dict:
    """Discord ID -> approved character name, written by WhitelistManager's
    /approvecharacter (characters.json, same VPS -- plain local file read,
    no SFTP needed, unlike the PZ server files above which live on a
    different host). Re-read fresh on every message rather than cached,
    since it's small and this only runs on an actual post to the channel --
    no reason to risk serving a stale approval after WhitelistManager writes
    a new one. Returns {} if the check is disabled (CHARACTERS_FILE_PATH
    unset) or the file doesn't exist yet."""
    if not CHARACTERS_FILE_PATH:
        return {}
    try:
        return json.loads(Path(CHARACTERS_FILE_PATH).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def load_accounts() -> dict:
    """Discord ID -> {"username":..., "steam_id":...} (or a legacy bare
    username string), written by WhitelistManager's /whitelist. This is the
    fallback's only source of "what PZ account does this Discord ID
    actually own" -- deliberately NOT derived from anything typed into a
    nickname, since that's exactly what could be spoofed. Returns {} if
    ACCOUNTS_FILE_PATH is unset or the file doesn't exist yet."""
    if not ACCOUNTS_FILE_PATH:
        return {}
    try:
        return json.loads(Path(ACCOUNTS_FILE_PATH).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def account_username(entry) -> "str | None":
    """Mirrors WhitelistManager's account_username -- accounts.json entries
    are either a bare username string (legacy) or {"username":...,
    "steam_id":...}."""
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        return entry.get("username")
    return None


def get_live_character_name(status_lines: list, username: str) -> "str | None":
    """Parses the same status.txt snapshot /players reads (see
    build_player_status_lookup in WhitelistManager/bot.py, mirrored here)
    and returns username's current in-game character name, or None if
    they're not in it (offline, or the snapshot doesn't have them yet) or
    are currently masked (an in-character anonymity feature -- a masked
    account deliberately carries no name anywhere, so this fallback simply
    finds nothing for them, same as if they were offline).

    status_lines is whatever SftpBridge.read_status returned -- one JSON
    object per line, newest last; only the last line that actually parses
    is used (defends against a torn/partial trailing line).
    """
    status = None
    for line in reversed(status_lines):
        try:
            status = json.loads(line)
            break
        except json.JSONDecodeError:
            continue
    if not status:
        return None

    entries = status.get("players")
    if not isinstance(entries, list):
        return None

    username_lower = username.lower()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        if (entry.get("username") or "").lower() != username_lower:
            continue
        if entry.get("masked"):
            return None
        return entry.get("characterName") or None
    return None


async def handle_name_mismatch(message: discord.Message, character_name: "str | None"):
    """Blocks a 100MHz post whose Discord nickname doesn't contain their
    approved character's name: deletes the message (so it's never
    ambiguous whether it went out over the radio) and DMs the author why.
    If DMs are closed (common, not an error), falls back to a visible
    notice in the channel itself, pinging Senior Staff if configured --
    the warning has to reach someone rather than vanish silently.
    """
    log.info(
        "on_message: blocking %s -- character_name on file=%r, display_name=%r",
        message.author, character_name, message.author.display_name,
    )
    try:
        await message.delete()
    except discord.NotFound:
        pass
    except discord.Forbidden:
        log.warning("Missing Manage Messages permission; could not delete mismatched message from %s", message.author)
    except Exception:
        log.exception("Failed to delete mismatched message from %s", message.author)

    dm_text = (
        "Your message in the 100MHz channel wasn't sent -- your Discord nickname doesn't include your "
        "approved character's name, so it can't be verified as actually being your character speaking.\n\n"
        "Update your Discord nickname to include your character's name, then try again. If that doesn't fix "
        "it, please open a ticket."
    )
    dm_delivered = False
    try:
        await message.author.send(dm_text)
        dm_delivered = True
    except discord.HTTPException as exc:
        # Same closed-DMs gotcha as elsewhere in this codebase: error code
        # 50007 ("Cannot send messages to this user") doesn't always come
        # back as discord.Forbidden, so both must be treated as "DMs
        # closed," not a real failure. Anything else is unexpected.
        if getattr(exc, "code", None) != 50007:
            log.exception("Unexpected error DMing %s about a blocked 100MHz message", message.author)

    if not dm_delivered:
        fallback = (
            f"⚠️ A message from {message.author.mention} was blocked (Discord nickname doesn't match their "
            "approved character) -- their DMs are closed, so posting the warning here instead."
        )
        if SENIOR_STAFF_ROLE_ID:
            fallback += f" <@&{SENIOR_STAFF_ROLE_ID}>"
        try:
            await message.channel.send(fallback)
        except Exception:
            log.exception("Failed to post fallback blocked-message notice for %s", message.author)


async def check_character_name(message: discord.Message, sftp: "SftpBridge") -> "tuple[bool, str]":
    """Returns (allowed, reason) for message.author's post. Two-stage:

    1. characters.json (WhitelistManager's /approvecharacter record) --
       fast, no SFTP round-trip, covers anyone staff has run the command
       for.
    2. Only if (1) has no entry or doesn't match: fall back to the
       account this Discord ID actually owns (accounts.json, set only by
       staff running /whitelist -- never derived from anything the poster
       could type themselves) and THAT account's live in-game character
       name (status.txt). Deliberately scoped to the poster's own account
       only -- never searches the whole online player list for a match,
       since that would let someone impersonate another player's character
       just by copying their name into their own nickname.

    This is what actually lets someone through whose PZ login username
    doesn't relate to their Discord identity at all (e.g. a Discord name
    with characters that got stripped down to a random player12345-style
    username, or a special character PZ's own sanitizer mangled) -- their
    approved character name might never have made it into characters.json,
    but their account ownership and live character name are both still
    verifiable the same way regardless.
    """
    display_name = message.author.display_name
    author_id = str(message.author.id)

    character_name = load_characters().get(author_id)
    if character_name and character_name_matches(character_name, display_name):
        return True, f"characters.json match ({character_name!r})"

    accounts = load_accounts()
    username = account_username(accounts.get(author_id))
    if not username:
        return False, "no characters.json entry, and no accounts.json entry to fall back to"

    status_lines = await asyncio.get_event_loop().run_in_executor(
        None, sftp.read_status, REMOTE_STATUS_FILE
    )
    live_character_name = get_live_character_name(status_lines, username)
    if not live_character_name:
        return False, f"no characters.json match, and no live character name found for their account ({username!r})"

    if character_name_matches(live_character_name, display_name):
        return True, f"live status.txt match (account {username!r}, character {live_character_name!r})"
    return False, (
        f"no characters.json match, and their account's ({username!r}) live character "
        f"{live_character_name!r} doesn't match their nickname either"
    )


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

        display_name = message.author.display_name

        if CHARACTERS_FILE_PATH and not any(
            role.id in NAME_CHECK_BYPASS_ROLE_IDS for role in message.author.roles
        ):
            allowed, reason = await check_character_name(message, self.sftp)
            if NAME_CHECK_ENFORCE:
                log.info(
                    "on_message: name check %s for %s (display_name=%r): %s",
                    "PASSED" if allowed else "BLOCKED", message.author, display_name, reason,
                )
                if not allowed:
                    await handle_name_mismatch(message, load_characters().get(str(message.author.id)))
                    return
            else:
                # Dry run: NAME_CHECK_ENFORCE is off, so this is purely
                # observational -- log what WOULD have happened and relay
                # the message normally either way. Review these logs for a
                # few days before flipping NAME_CHECK_ENFORCE=true.
                log.info(
                    "on_message: [DRY RUN] name check would have %s for %s (display_name=%r): %s",
                    "PASSED" if allowed else "BLOCKED", message.author, display_name, reason,
                )

        chunks = _chunk_message(text, MAX_MESSAGE_LENGTH)
        if len(chunks) > 1:
            log.info("on_message: split %d chars into %d chunks (max %d)",
                      len(text), len(chunks), MAX_MESSAGE_LENGTH)

        try:
            for chunk in chunks:
                # Same message.id on every chunk -- the Lua side round-trips
                # it back on each chunk's outbox echo unchanged, so the
                # original gets deleted once (fetch_message on an
                # already-deleted id just raises NotFound, silently caught
                # in poll_outbox) while every chunk still gets its own
                # in-character radio line.
                line = f"{_escape_field(display_name)}|{message.id}|{_escape_field(chunk)}"
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


def _chunk_message(text: str, max_len: int) -> list[str]:
    """Split text into pieces of at most max_len characters. Breaks on the
    last whitespace within the limit where one exists, so words aren't cut
    mid-word; falls back to a hard cut for a single "word" longer than
    max_len (e.g. a long URL/string with no spaces). Returns [text]
    unchanged (no copy) when it already fits."""
    if len(text) <= max_len:
        return [text]

    chunks = []
    remaining = text
    while len(remaining) > max_len:
        split_at = remaining.rfind(" ", 0, max_len + 1)
        if split_at <= 0:
            split_at = max_len
        chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


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
