# TAZC Discord Radio Bridge

Bridges 100.0 MHz in-game radio to a Discord channel, both directions, via
SFTP + two plain files. See `TAZC/42/media/lua/server/TAZC_DiscordBridge.lua`
for the game-side half of this.

## 1. Create the Discord bot application

1. Go to the Discord Developer Portal and create a **New Application**. Name
   it whatever you like (e.g. "TAZC Radio").
2. Open the **Bot** tab. Click **Reset Token** to generate a token, copy it
   -- this goes in `.env` as `DISCORD_BOT_TOKEN`. Treat it like a password;
   anyone with it controls your bot.
3. On the same **Bot** tab, under **Privileged Gateway Intents**, enable
   **Message Content Intent**. Without this the bot can see *that* a message
   was sent but not read its text.
4. Open **OAuth2 -> URL Generator**. Under **Scopes** check `bot`. Under
   **Bot Permissions** check at minimum: `View Channels`, `Send Messages`,
   `Read Message History`, `Add Reactions`, `Manage Messages`. Copy the
   generated URL, open it in a browser, and add the bot to your Discord
   server. `Manage Messages` is required because the bot deletes the
   original plain-text message once it posts the in-character "radio"
   version -- without it, deletes silently fail (logged as a warning) and
   both copies of the message stay in the channel.

   If the bot is already in your server and the bridged channel has its
   own per-role permission overwrites (as `#radio-100` does), adding the
   scope to the invite URL and re-authorizing may not be enough -- the
   channel-specific overwrite can still block it. Simplest fix: open the
   channel's settings in Discord, go to **Permissions**, find the bot's
   role/overwrite entry, and toggle **Manage Messages** to Allow directly.
5. In Discord, enable **Developer Mode** (User Settings -> Advanced), then
   right-click the channel you want bridged and **Copy Channel ID**. This
   goes in `.env` as `DISCORD_CHANNEL_ID`.

## 2. Provision a small always-on box

Any small Linux VPS works (Hetzner/DigitalOcean/Vultr/Linode's cheapest
tier, or Oracle Cloud's free tier). You need SSH access to it. Once you're
in:

```bash
sudo apt update && sudo apt install -y python3 python3-venv python3-pip git
```

## 3. Get SFTP access to your PZ server (Indifferent Broccoli)

Get the SFTP hostname, port, username, and either a password or (preferred)
an SSH key from your host's control panel. You also need the **absolute
path on the server** to the `Zomboid` data folder for this server instance
(the one containing `Lua/`, `Saves/`, `Server/`) -- your host's file manager
should show this; it's what goes in `.env` as `ZOMBOID_DATA_PATH`.

## 4. Install and configure the bot

On the VPS:

```bash
git clone <your repo, or just copy this folder over>
cd DiscordBridge
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
nano .env   # fill in every value
```

Run it once by hand to check it works:

```bash
python3 bot.py
```

You should see `Logged in as <bot name>` in the log output. Speak on
100MHz in-game and confirm it shows up in the Discord channel; post in the
Discord channel and confirm a garbled version comes through on 100MHz
in-game.

## 5. Keep it running (systemd)

Create `/etc/systemd/system/tazc-bridge.service`:

```ini
[Unit]
Description=TAZC Discord Radio Bridge
After=network.target

[Service]
Type=simple
User=YOUR_LINUX_USER
WorkingDirectory=/path/to/DiscordBridge
ExecStart=/path/to/DiscordBridge/.venv/bin/python3 bot.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tazc-bridge
sudo systemctl status tazc-bridge     # check it's running
journalctl -u tazc-bridge -f          # follow the logs
```

## Known limitations (first pass)

- Inbound (Discord -> game) broadcast only reaches radios carried in a
  player's hands, belt, or inventory -- vehicle and ground/base-station
  radios tuned to 100MHz are not reached yet, since a Discord message has
  no in-world position to search outward from.
- `MAX_MESSAGE_LENGTH` in `.env` must be kept in sync by hand with your
  server's actual `TAZC_Config.MaxMessageLength` sandbox setting -- the bot
  has no way to read that from the running server.
- The read-then-truncate pattern on both ends has a small race window: a
  message arriving in the moment between one side reading and clearing a
  file could be missed. Rare in practice, not solved by a lock file here.
