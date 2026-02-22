import discord
from discord.ext import commands, tasks
from discord import app_commands
import aiosqlite, json, os, asyncio, itertools
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN        = os.environ.get("DISCORD_TOKEN") or "YOUR_TOKEN_HERE"
DB_FILE      = os.path.join(os.environ.get("DB_DIR", "."), "ranked.db")
CONFIG_FILE  = "config.json"
STARTING_MMR = 1000
BASE_GAIN    = 25
STAFF_ROLES  = ["owner", "admin", "moderator", "helper"]

RANKS = [
    (900,  "iron",     "Iron"),
    (1200, "bronze",   "Bronze"),
    (1500, "silver",   "Silver"),
    (1800, "gold",     "Gold"),
    (2100, "platinum", "Platinum"),
    (4000, "diamond",  "Diamond"),
    (9999, "champion", "Champion"),
]

RANK_ROLE_NAMES = [r[2] for r in RANKS]  # ["Iron", "Bronze", ...]
# Mode-specific role names e.g. "Iron 1v1", "Gold 2v2"
def rank_role_name(mmr: int, mode: str) -> str:
    return f"{rank_label(mmr)} {mode}"

def all_rank_role_names(mode: str) -> list:
    return [f"{r[2]} {mode}" for r in RANKS]

# ── Database ──────────────────────────────────────────────────────────────────
db: aiosqlite.Connection = None

async def init_db():
    global db
    db = await aiosqlite.connect(DB_FILE)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")   # concurrent reads + safe writes
    await db.execute("PRAGMA foreign_keys=ON")
    await db.executescript("""
        CREATE TABLE IF NOT EXISTS players (
            uid             TEXT PRIMARY KEY,
            username        TEXT NOT NULL,
            mmr_1v1         INTEGER DEFAULT 1000,
            mmr_2v2         INTEGER DEFAULT 1000,
            wins_1v1        INTEGER DEFAULT 0,
            losses_1v1      INTEGER DEFAULT 0,
            wins_2v2        INTEGER DEFAULT 0,
            losses_2v2      INTEGER DEFAULT 0,
            streak_1v1      INTEGER DEFAULT 0,
            streak_2v2      INTEGER DEFAULT 0,
            best_streak_1v1 INTEGER DEFAULT 0,
            best_streak_2v2 INTEGER DEFAULT 0,
            loss_streak_1v1 INTEGER DEFAULT 0,
            loss_streak_2v2 INTEGER DEFAULT 0,
            active_lobby    TEXT DEFAULT NULL
        );

        CREATE TABLE IF NOT EXISTS history (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            uid        TEXT NOT NULL,
            mode       TEXT NOT NULL,
            result     TEXT NOT NULL,
            mmr_change INTEGER NOT NULL,
            timestamp  TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_history_uid ON history(uid);

        CREATE TABLE IF NOT EXISTS h2h (
            uid1    TEXT NOT NULL,
            uid2    TEXT NOT NULL,
            wins    INTEGER DEFAULT 0,
            losses  INTEGER DEFAULT 0,
            PRIMARY KEY (uid1, uid2)
        );

        -- Lobbies and pending stored as JSON blobs — they're small and transient
        CREATE TABLE IF NOT EXISTS lobbies (
            lobby_id TEXT PRIMARY KEY,
            data     TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pending (
            pending_id TEXT PRIMARY KEY,
            data       TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS banned (
            uid TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS season_archive (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            season    INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            data      TEXT NOT NULL
        );

        -- Simple key/value for season number, match count, and lobby counters
        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        INSERT OR IGNORE INTO meta VALUES ('season',            '1');
        INSERT OR IGNORE INTO meta VALUES ('total_matches',     '0');
        INSERT OR IGNORE INTO meta VALUES ('lobby_counter_1v1', '0');
        INSERT OR IGNORE INTO meta VALUES ('lobby_counter_2v2', '0');
    """)
    await db.commit()


# ── DB Helpers — Players ──────────────────────────────────────────────────────

async def db_get_player(uid: str) -> dict | None:
    async with db.execute("SELECT * FROM players WHERE uid=?", (uid,)) as cur:
        row = await cur.fetchone()
    return dict(row) if row else None

async def db_upsert_player(uid: str, username: str) -> dict:
    """Create player if they don't exist, always refresh their username."""
    await db.execute("""
        INSERT INTO players (uid, username) VALUES (?,?)
        ON CONFLICT(uid) DO UPDATE SET username=excluded.username
    """, (uid, username))
    await db.commit()
    return await db_get_player(uid)

async def db_update_player(uid: str, **fields):
    if not fields: return
    cols = ", ".join(f"{k}=?" for k in fields)
    vals = list(fields.values()) + [uid]
    await db.execute(f"UPDATE players SET {cols} WHERE uid=?", vals)
    await db.commit()

async def db_get_all_players() -> dict:
    async with db.execute("SELECT * FROM players") as cur:
        rows = await cur.fetchall()
    return {row["uid"]: dict(row) for row in rows}

async def db_get_players_by_uids(uids: list) -> dict:
    if not uids: return {}
    placeholders = ",".join("?" * len(uids))
    async with db.execute(f"SELECT * FROM players WHERE uid IN ({placeholders})", uids) as cur:
        rows = await cur.fetchall()
    return {row["uid"]: dict(row) for row in rows}


# ── DB Helpers — History ──────────────────────────────────────────────────────

async def db_add_history(uid: str, mode: str, result: str, mmr_change: int, timestamp: str):
    await db.execute(
        "INSERT INTO history (uid, mode, result, mmr_change, timestamp) VALUES (?,?,?,?,?)",
        (uid, mode, result, mmr_change, timestamp)
    )
    await db.commit()

async def db_get_history(uid: str) -> list:
    async with db.execute(
        "SELECT mode, result, mmr_change, timestamp FROM history WHERE uid=? ORDER BY id ASC",
        (uid,)
    ) as cur:
        rows = await cur.fetchall()
    return [dict(r) for r in rows]

async def db_clear_all_history():
    await db.execute("DELETE FROM history")
    await db.commit()


# ── DB Helpers — H2H ─────────────────────────────────────────────────────────

async def db_get_h2h(uid1: str, uid2: str) -> dict:
    async with db.execute(
        "SELECT wins, losses FROM h2h WHERE uid1=? AND uid2=?", (uid1, uid2)
    ) as cur:
        row = await cur.fetchone()
    return dict(row) if row else {"wins": 0, "losses": 0}

async def db_record_h2h_win(winner: str, loser: str):
    await db.execute("""
        INSERT INTO h2h (uid1, uid2, wins, losses) VALUES (?,?,1,0)
        ON CONFLICT(uid1,uid2) DO UPDATE SET wins=wins+1
    """, (winner, loser))
    await db.execute("""
        INSERT INTO h2h (uid1, uid2, wins, losses) VALUES (?,?,0,1)
        ON CONFLICT(uid1,uid2) DO UPDATE SET losses=losses+1
    """, (loser, winner))
    await db.commit()

async def db_clear_all_h2h():
    await db.execute("DELETE FROM h2h")
    await db.commit()

async def db_get_rival(uid: str) -> tuple[str, dict] | tuple[None, None]:
    """Return (opponent_uid, {wins, losses}) for the opponent uid has played most against."""
    async with db.execute(
        "SELECT uid2, wins, losses FROM h2h WHERE uid1=? ORDER BY (wins+losses) DESC LIMIT 1", (uid,)
    ) as cur:
        row = await cur.fetchone()
    if not row or (row["wins"] + row["losses"]) == 0:
        return None, None
    return row["uid2"], {"wins": row["wins"], "losses": row["losses"]}


# ── DB Helpers — Lobbies ──────────────────────────────────────────────────────

async def db_get_lobby(lobby_id: str) -> dict | None:
    async with db.execute("SELECT data FROM lobbies WHERE lobby_id=?", (lobby_id,)) as cur:
        row = await cur.fetchone()
    return json.loads(row["data"]) if row else None

async def db_save_lobby(lobby_id: str, lobby: dict):
    await db.execute(
        "INSERT OR REPLACE INTO lobbies (lobby_id, data) VALUES (?,?)",
        (lobby_id, json.dumps(lobby))
    )
    await db.commit()

async def db_delete_lobby(lobby_id: str):
    await db.execute("DELETE FROM lobbies WHERE lobby_id=?", (lobby_id,))
    await db.commit()

async def db_get_all_lobbies() -> dict:
    async with db.execute("SELECT lobby_id, data FROM lobbies") as cur:
        rows = await cur.fetchall()
    return {row["lobby_id"]: json.loads(row["data"]) for row in rows}


# ── DB Helpers — Pending ──────────────────────────────────────────────────────

async def db_get_pending(pid: str) -> dict | None:
    async with db.execute("SELECT data FROM pending WHERE pending_id=?", (pid,)) as cur:
        row = await cur.fetchone()
    return json.loads(row["data"]) if row else None

async def db_save_pending(pid: str, data: dict):
    await db.execute(
        "INSERT OR REPLACE INTO pending (pending_id, data) VALUES (?,?)",
        (pid, json.dumps(data))
    )
    await db.commit()

async def db_delete_pending(pid: str):
    await db.execute("DELETE FROM pending WHERE pending_id=?", (pid,))
    await db.commit()

async def db_lobby_has_pending(lobby_id: str) -> str | None:
    """Returns the pending_id if this lobby already has a report, otherwise None."""
    async with db.execute("SELECT pending_id, data FROM pending") as cur:
        rows = await cur.fetchall()
    for row in rows:
        d = json.loads(row["data"])
        if d.get("lobby_id") == lobby_id:
            return row["pending_id"]
    return None

async def db_clear_pending_for_lobby(lobby_id: str):
    async with db.execute("SELECT pending_id, data FROM pending") as cur:
        rows = await cur.fetchall()
    for row in rows:
        d = json.loads(row["data"])
        if d.get("lobby_id") == lobby_id:
            await db.execute("DELETE FROM pending WHERE pending_id=?", (row["pending_id"],))
    await db.commit()


# ── DB Helpers — Banned ───────────────────────────────────────────────────────

async def db_is_banned(uid: str) -> bool:
    async with db.execute("SELECT 1 FROM banned WHERE uid=?", (uid,)) as cur:
        return await cur.fetchone() is not None

async def db_ban(uid: str):
    await db.execute("INSERT OR IGNORE INTO banned (uid) VALUES (?)", (uid,))
    await db.commit()

async def db_unban(uid: str):
    await db.execute("DELETE FROM banned WHERE uid=?", (uid,))
    await db.commit()


# ── DB Helpers — Meta ─────────────────────────────────────────────────────────

async def db_get_meta(key: str, default: str = "0") -> str:
    async with db.execute("SELECT value FROM meta WHERE key=?", (key,)) as cur:
        row = await cur.fetchone()
    return row["value"] if row else default

async def db_set_meta(key: str, value):
    await db.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)", (key, str(value))
    )
    await db.commit()

async def db_get_season() -> int:
    return int(await db_get_meta("season", "1"))

async def db_increment_total_matches():
    await db.execute(
        "UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='total_matches'"
    )
    await db.commit()

async def db_increment_lobby_counter(mode: str) -> int:
    key     = f"lobby_counter_{mode}"
    current = int(await db_get_meta(key, "0"))
    next_val = (current % 1000) + 1  # wraps 1→1000→1
    await db_set_meta(key, next_val)
    return next_val


# ── Config (stays JSON — it's tiny and rarely written) ───────────────────────

def load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {
        "leaderboard_1v1_channel": None, "leaderboard_1v1_message": None,
        "leaderboard_2v2_channel": None, "leaderboard_2v2_message": None,
        "match_log_channel": None, "rankup_channel": None,
        "lobby_category": None, "spotlight_channel": None,
        "queue_1v1_channel": None,
        "queue_2v2_channel": None,
        "ranked_verify_channel": None,   # channel with the how-to embed + reaction
        "ranked_verify_message": None,   # message ID of that embed
        "ranked_verified_role": None,    # role ID granted on reaction
        "commands_channel": None,        # channel for the user commands embed
        "commands_message": None,        # message ID of the commands embed
    }

def save_config(cfg: dict):
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)


# ── Pure Utilities ────────────────────────────────────────────────────────────

def rank_label(mmr: int) -> str:
    """Returns rank name without emoji (used as fallback / for role names)."""
    for threshold, _, name in RANKS:
        if mmr < threshold:
            return name
    return RANKS[-1][2]

def rank_label_emoji(mmr: int, guild: discord.Guild = None) -> str:
    """Returns rank label with custom emoji if guild is provided, else plain name."""
    for threshold, emoji_name, name in RANKS:
        if mmr < threshold:
            if guild:
                emoji = discord.utils.get(guild.emojis, name=emoji_name)
                if emoji:
                    return f"{emoji} {name}"
            return name
    # Champion (last)
    _, emoji_name, name = RANKS[-1]
    if guild:
        emoji = discord.utils.get(guild.emojis, name=emoji_name)
        if emoji:
            return f"{emoji} {name}"
    return name

def rank_index(mmr: int) -> int:
    for i, (threshold, _, _) in enumerate(RANKS):
        if mmr < threshold:
            return i
    return len(RANKS) - 1

def calc_mmr_change(winner_mmr: int, loser_mmr: int):
    diff   = loser_mmr - winner_mmr
    factor = max(0.5, min(2.0, 1 + diff / 400))
    gain   = max(10, min(50, round(BASE_GAIN * factor)))
    loss   = max(10, min(50, round(BASE_GAIN / factor)))
    return gain, loss

def rank_label_g(mmr: int) -> str:
    """rank_label with emoji — auto-uses the bot's first guild. Use everywhere in embeds."""
    guild = bot.guilds[0] if bot.guilds else None
    return rank_label_emoji(mmr, guild)


    uids = list(players_mmr.keys())
    best, best_diff = None, float("inf")
    for combo in itertools.combinations(uids, 2):
        t1 = list(combo)
        t2 = [u for u in uids if u not in t1]
        diff = abs(sum(players_mmr[u] for u in t1) / 2 - sum(players_mmr[u] for u in t2) / 2)
        if diff < best_diff:
            best_diff = diff
            best = (t1, t2)
    return best


# ── Bot ───────────────────────────────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot  = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

lobby_creation_cooldowns: dict = {}


# ── Embed Builders ────────────────────────────────────────────────────────────
# These are pure sync functions — callers must pre-fetch `players` dict from DB.

def build_leaderboard_embed(players: dict, season: int, mode: str) -> discord.Embed:
    wk, lk  = f"wins_{mode}", f"losses_{mode}"
    mmr_key = f"mmr_{mode}"
    sk      = f"best_streak_{mode}"
    eligible   = [(uid, p) for uid, p in players.items() if p.get(wk, 0) + p.get(lk, 0) > 0]
    top_mmr    = sorted(eligible, key=lambda x: x[1][mmr_key], reverse=True)[:10]
    top_streak = sorted(eligible, key=lambda x: x[1].get(sk, 0), reverse=True)[:10]
    medals = ["🥇", "🥈", "🥉"]
    embed  = discord.Embed(
        title=f"{'⚔️' if mode == '1v1' else '🤝'} {mode.upper()} Leaderboard — Season {season}",
        color=discord.Color.red() if mode == "1v1" else discord.Color.purple(),
        timestamp=datetime.now(timezone.utc),
    )
    mmr_lines = []
    for i, (uid, p) in enumerate(top_mmr):
        medal = medals[i] if i < 3 else f"`{i + 1}.`"
        mmr_lines.append(
            f"{medal} **{p.get('username', '?')}** — {rank_label_g(p[mmr_key])} | {p[mmr_key]} MMR | {p.get(wk, 0)}W/{p.get(lk, 0)}L"
        )
    embed.add_field(name="Top MMR", value="\n".join(mmr_lines) or "No players yet", inline=False)
    streak_lines = []
    for i, (uid, p) in enumerate(top_streak):
        medal = medals[i] if i < 3 else f"`{i + 1}.`"
        streak_lines.append(
            f"{medal} **{p.get('username', '?')}** — Best: 🔥{p.get(sk, 0)} | Current: {p.get(f'streak_{mode}', 0)}"
        )
    embed.add_field(name="Win Streaks", value="\n".join(streak_lines) or "No players yet", inline=False)
    embed.set_footer(text="Updates every 60s")
    return embed


def build_lobby_embed(players: dict, lobby: dict) -> discord.Embed:
    mode  = lobby["mode"]
    max_p = 2 if mode == "1v1" else 4
    color = discord.Color.red() if mode == "1v1" else discord.Color.purple()
    embed = discord.Embed(
        title=f"{'⚔️' if mode == '1v1' else '🤝'} {mode.upper()} Ranked Lobby",
        color=color,
    )
    embed.add_field(name="Host",    value=lobby["host_name"],                  inline=True)
    embed.add_field(name="Mode",    value=mode.upper(),                        inline=True)
    embed.add_field(name="Players", value=f"{len(lobby['players'])}/{max_p}", inline=True)
    lines = []
    for uid in lobby["players"]:
        p          = players.get(uid, {})
        mmr        = p.get(f"mmr_{mode}", STARTING_MMR)
        name       = p.get("username", "Player")
        ready_mark = "✅" if uid in lobby.get("ready", []) else "⏳"
        lines.append(f"{ready_mark} **{name}** — {rank_label_g(mmr)} ({mmr} MMR)")
    embed.add_field(name="Players", value="\n".join(lines) or "None", inline=False)
    if lobby.get("team1") and lobby.get("team2"):
        t1 = ", ".join(players.get(u, {}).get("username", "?") for u in lobby["team1"])
        t2 = ", ".join(players.get(u, {}).get("username", "?") for u in lobby["team2"])
        embed.add_field(name="🔴 Team 1", value=t1, inline=True)
        embed.add_field(name="🔵 Team 2", value=t2, inline=True)
    status_text = {
        "open":      "Waiting for players...",
        "full":      "Lobby full — ready up!",
        "teams_set": "Teams set — ready up!",
        "launched":  "Match in progress",
    }
    embed.set_footer(text=status_text.get(lobby.get("status", "open"), ""))
    return embed


def build_spotlight_embed(players: dict, season: int) -> discord.Embed:
    items = list(players.items())
    if not items:
        return discord.Embed(title="No players yet", color=discord.Color.gold())
    top_1v1     = max(items, key=lambda x: x[1].get("mmr_1v1", 0))
    top_2v2     = max(items, key=lambda x: x[1].get("mmr_2v2", 0))
    top_streak  = max(items, key=lambda x: max(x[1].get("best_streak_1v1", 0), x[1].get("best_streak_2v2", 0)))
    most_active = max(items, key=lambda x: (
        x[1].get("wins_1v1", 0) + x[1].get("losses_1v1", 0) +
        x[1].get("wins_2v2", 0) + x[1].get("losses_2v2", 0)
    ))
    embed = discord.Embed(
        title="Weekly Player Spotlight",
        description="This week's standouts",
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc),
    )
    p = top_1v1[1]
    embed.add_field(name="Top 1v1",
        value=f"**{p.get('username', '?')}**\n{rank_label_g(p['mmr_1v1'])} | {p['mmr_1v1']} MMR", inline=True)
    p = top_2v2[1]
    embed.add_field(name="Top 2v2",
        value=f"**{p.get('username', '?')}**\n{rank_label_g(p['mmr_2v2'])} | {p['mmr_2v2']} MMR", inline=True)
    p = top_streak[1]
    embed.add_field(name="Best Streak",
        value=f"**{p.get('username', '?')}**\n{max(p.get('best_streak_1v1', 0), p.get('best_streak_2v2', 0))} wins", inline=True)
    p = most_active[1]
    total = p.get("wins_1v1",0)+p.get("losses_1v1",0)+p.get("wins_2v2",0)+p.get("losses_2v2",0)
    embed.add_field(name="Most Active",
        value=f"**{p.get('username', '?')}**\n{total} games", inline=True)
    embed.set_footer(text=f"Season {season}")
    return embed


# ── Leaderboard Auto-Update ───────────────────────────────────────────────────

@tasks.loop(seconds=60)
async def update_leaderboards():
    cfg     = load_config()
    players = await db_get_all_players()
    season  = await db_get_season()
    for mode in ("1v1", "2v2"):
        ch_id  = cfg.get(f"leaderboard_{mode}_channel")
        msg_id = cfg.get(f"leaderboard_{mode}_message")
        if not ch_id or not msg_id: continue
        channel = bot.get_channel(int(ch_id))
        if not channel: continue
        try:
            msg = await channel.fetch_message(int(msg_id))
            await msg.edit(embed=build_leaderboard_embed(players, season, mode))
        except Exception:
            pass


# ── Discord Helpers ───────────────────────────────────────────────────────────

async def get_or_create_lobby_category(guild: discord.Guild) -> discord.CategoryChannel:
    cfg = load_config()
    if cfg.get("lobby_category"):
        cat = guild.get_channel(int(cfg["lobby_category"]))
        if cat: return cat
    cat = await guild.create_category("🎮 Active Lobbies")
    cfg["lobby_category"] = str(cat.id)
    save_config(cfg)
    return cat

async def create_private_channel(guild, lobby_id, player_ids, mode, lobby_name: str = ""):
    category   = await get_or_create_lobby_category(guild)
    overwrites = {guild.default_role: discord.PermissionOverwrite(read_messages=False)}
    for role in guild.roles:
        if role.name.lower() in STAFF_ROLES:
            overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)
    for uid in player_ids:
        member = guild.get_member(int(uid))
        if member:
            overwrites[member] = discord.PermissionOverwrite(
                read_messages=True, send_messages=True, attach_files=True, embed_links=True
            )
    # Use lobby name if available (e.g. "1v1 #3" → "1v1-3"), else fall back to timestamp tail
    if lobby_name:
        safe_name = lobby_name.lower().replace(" ", "-").replace("#", "")
    else:
        safe_name = f"{mode}-{lobby_id[-6:]}"
    return await guild.create_text_channel(
        name=safe_name,
        category=category,
        overwrites=overwrites,
        topic=f"Private {mode.upper()} ranked lobby | ID: {lobby_id}",
    )

async def delete_channel_after(channel, delay: int):
    await asyncio.sleep(delay)
    try: await channel.delete()
    except Exception: pass

async def delete_notify_message(guild, lobby_id: str):
    try:
        lobby  = await db_get_lobby(lobby_id)
        if not lobby: return
        msg_id = lobby.get("notify_message_id")
        ch_id  = lobby.get("notify_message_channel")
        if not msg_id or not ch_id: return
        ch = guild.get_channel(int(ch_id))
        if not ch: return
        msg = await ch.fetch_message(int(msg_id))
        await msg.delete()
    except Exception:
        pass

async def delete_ready_alert(guild, lobby_id: str):
    """Delete the 'Click Ready within 3 minutes' alert message stored in the lobby."""
    try:
        lobby  = await db_get_lobby(lobby_id)
        if not lobby: return
        msg_id = lobby.get("ready_alert_id")
        ch_id  = lobby.get("ready_alert_channel_id")
        if not msg_id or not ch_id: return
        ch = guild.get_channel(int(ch_id))
        if not ch: return
        msg = await ch.fetch_message(int(msg_id))
        await msg.delete()
    except Exception:
        pass

async def assign_rank_role(guild: discord.Guild, member: discord.Member, mmr: int, mode: str):
    """Give member their rank role for the given mode, remove all other rank roles for that mode."""
    if not member: return
    target_name = rank_role_name(mmr, mode)
    for role_name in all_rank_role_names(mode):
        role = discord.utils.get(guild.roles, name=role_name)
        if not role:
            try:
                role = await guild.create_role(name=role_name, reason="Rank role auto-created")
            except Exception:
                continue
        try:
            if role_name == target_name and role not in member.roles:
                await member.add_roles(role, reason=f"{mode} rank updated")
            elif role_name != target_name and role in member.roles:
                await member.remove_roles(role, reason=f"{mode} rank updated")
        except Exception:
            pass

async def check_rankup(guild, uid: str, old_mmr: int, new_mmr: int, mode: str):
    member = guild.get_member(int(uid))
    if member:
        await assign_rank_role(guild, member, new_mmr, mode)
    old_idx = rank_index(old_mmr)
    new_idx = rank_index(new_mmr)
    if new_idx == old_idx: return
    cfg    = load_config()
    ch_id  = cfg.get("rankup_channel")
    if not ch_id: return
    channel = guild.get_channel(int(ch_id))
    if not channel: return
    name      = member.display_name if member else "Someone"
    old_label = rank_label_emoji(old_mmr, guild)
    new_label = rank_label_emoji(new_mmr, guild)
    mode_icon = "⚔️" if mode == "1v1" else "🤝"
    if new_idx > old_idx:
        embed = discord.Embed(
            title="🎉 Rank Up!",
            description=f"**{name}** ranked up in **{mode_icon} {mode.upper()}**!\n{old_label} → **{new_label}**",
            color=discord.Color.gold(),
        )
    else:
        embed = discord.Embed(
            title="📉 Rank Down",
            description=f"**{name}** dropped a rank in **{mode_icon} {mode.upper()}**.\n{old_label} → **{new_label}**",
            color=discord.Color.dark_grey(),
        )
    await channel.send(embed=embed)

async def post_match_log(guild, embed: discord.Embed):
    cfg   = load_config()
    ch_id = cfg.get("match_log_channel")
    if not ch_id: return
    channel = guild.get_channel(int(ch_id))
    if channel:
        try: await channel.send(embed=embed)
        except Exception: pass

def _is_staff(interaction: discord.Interaction) -> bool:
    roles = [r.name.lower() for r in interaction.user.roles]
    return interaction.user.guild_permissions.administrator or any(r in STAFF_ROLES for r in roles)


# ── Core Match Logic ──────────────────────────────────────────────────────────

async def apply_match(
    guild, winners: list, losers: list, mode: str, lobby_id: str = None
) -> discord.Embed:
    all_uids    = winners + losers
    all_players = await db_get_players_by_uids(all_uids)

    gain_base = sum(all_players[u][f"mmr_{mode}"] for u in winners) // len(winners)
    loss_base = sum(all_players[u][f"mmr_{mode}"] for u in losers)  // len(losers)
    gain, loss = calc_mmr_change(gain_base, loss_base)
    now    = datetime.now(timezone.utc).isoformat()
    season = await db_get_season()
    result_lines_w, result_lines_l = [], []

    for uid in winners:
        p          = all_players[uid]
        old_mmr    = p[f"mmr_{mode}"]
        new_streak = p[f"streak_{mode}"] + 1
        new_best   = max(new_streak, p[f"best_streak_{mode}"])
        new_mmr    = old_mmr + gain
        await db_update_player(uid, **{
            f"mmr_{mode}":          new_mmr,
            f"wins_{mode}":         p[f"wins_{mode}"] + 1,
            f"streak_{mode}":       new_streak,
            f"best_streak_{mode}":  new_best,
            f"loss_streak_{mode}":  0,
            "active_lobby":         None,
        })
        await db_add_history(uid, mode, "win", gain, now)
        result_lines_w.append(f"**{p['username']}** +{gain} MMR → **{new_mmr}**")
        if guild: await check_rankup(guild, uid, old_mmr, new_mmr, mode)

    for uid in losers:
        p       = all_players[uid]
        old_mmr = p[f"mmr_{mode}"]
        new_mmr = max(100, old_mmr - loss)
        await db_update_player(uid, **{
            f"mmr_{mode}":         new_mmr,
            f"losses_{mode}":      p[f"losses_{mode}"] + 1,
            f"streak_{mode}":      0,
            f"loss_streak_{mode}": p[f"loss_streak_{mode}"] + 1,
            "active_lobby":        None,
        })
        await db_add_history(uid, mode, "loss", -loss, now)
        result_lines_l.append(f"**{p['username']}** -{loss} MMR → **{new_mmr}**")

    if mode == "1v1" and len(winners) == 1 and len(losers) == 1:
        await db_record_h2h_win(winners[0], losers[0])

    if lobby_id:
        lobby = await db_get_lobby(lobby_id)
        if lobby:
            lobby["status"] = "complete"
            await db_save_lobby(lobby_id, lobby)

    await db_increment_total_matches()

    icon  = "⚔️" if mode == "1v1" else "🤝"
    embed = discord.Embed(
        title=f"{icon} {mode.upper()} Match Result",
        color=discord.Color.green(),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="Winners", value="\n".join(result_lines_w), inline=False)
    embed.add_field(name="Losers",  value="\n".join(result_lines_l), inline=False)
    embed.set_footer(text=f"Season {season}")
    return embed


# ── Views ─────────────────────────────────────────────────────────────────────

class LobbyView(discord.ui.View):
    """Public lobby message — Join / Leave for everyone."""
    def __init__(self, lobby_id: str):
        super().__init__(timeout=2700)  # 45 min
        self.lobby_id = lobby_id

    @discord.ui.button(label="✅ Join", style=discord.ButtonStyle.green)
    async def join(self, interaction: discord.Interaction, button: discord.ui.Button):
        lobby = await db_get_lobby(self.lobby_id)
        if not lobby or lobby["status"] != "open":
            await interaction.response.send_message("❌ Lobby not available.", ephemeral=True); return
        uid   = str(interaction.user.id)
        mode  = lobby["mode"]
        max_p = 2 if mode == "1v1" else 4
        if await db_is_banned(uid):
            await interaction.response.send_message("❌ You are banned from using the bot.", ephemeral=True); return
        if uid in lobby["players"]:
            await interaction.response.send_message("You are already in this lobby!", ephemeral=True); return
        if len(lobby["players"]) >= max_p:
            await interaction.response.send_message("❌ Lobby is full!", ephemeral=True); return
        p = await db_get_player(uid)
        if p and p.get("active_lobby") and p["active_lobby"] != self.lobby_id:
            existing = await db_get_lobby(p["active_lobby"])
            ch_id    = existing.get("private_channel") if existing else None
            if ch_id:
                ch = interaction.guild.get_channel(int(ch_id))
                if ch:
                    await interaction.response.send_message(f"❌ You're already in an active lobby: {ch.mention}", ephemeral=True); return
            await interaction.response.send_message(
                "❌ You're stuck in a lobby that no longer exists.\nUse `/leavelobby` to unstick yourself.", ephemeral=True
            ); return
        await db_upsert_player(uid, interaction.user.display_name)
        await db_update_player(uid, active_lobby=self.lobby_id)
        lobby["players"].append(uid)
        lobby["last_activity"] = datetime.now(timezone.utc).isoformat()
        if len(lobby["players"]) == max_p:
            lobby["status"] = "full"
        await db_save_lobby(self.lobby_id, lobby)
        players = await db_get_players_by_uids(lobby["players"])
        embed   = build_lobby_embed(players, lobby)
        if lobby["status"] == "full":
            ping_str = " ".join(f"<@{p}>" for p in lobby["players"])
            if mode == "2v2":
                await interaction.response.edit_message(embed=embed, view=TeamSelectView(self.lobby_id))
                alert = await interaction.followup.send(
                    f"🔔 {ping_str} — lobby is full! **Pick your teams** above.", wait=True
                )
                # Pick-teams message auto-deletes after 30s — it's just a nudge
                async def _del_pick(m):
                    await asyncio.sleep(30)
                    try: await m.delete()
                    except Exception: pass
                bot.loop.create_task(_del_pick(alert))
            else:
                await interaction.response.edit_message(embed=embed, view=ReadyView(self.lobby_id))
                bot.loop.create_task(ready_check_timeout(interaction.guild, self.lobby_id))
                alert = await interaction.followup.send(
                    f"🔔 {ping_str} — lobby is full! **Click Ready** above within 3 minutes.", wait=True
                )
                # Store so it can be deleted when everyone readies up or timeout fires
                lobby["ready_alert_id"]         = str(alert.id)
                lobby["ready_alert_channel_id"] = str(alert.channel.id)
                await db_save_lobby(self.lobby_id, lobby)
        else:
            await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="🚪 Leave", style=discord.ButtonStyle.red)
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        lobby = await db_get_lobby(self.lobby_id)
        if not lobby:
            await interaction.response.send_message("❌ Lobby not found.", ephemeral=True); return
        uid = str(interaction.user.id)
        if uid not in lobby["players"]:
            await interaction.response.send_message("You are not in this lobby.", ephemeral=True); return
        was_host = uid == lobby.get("host")
        lobby["players"].remove(uid)
        await db_update_player(uid, active_lobby=None)
        if len(lobby["players"]) == 0:
            await db_delete_lobby(self.lobby_id)
            await interaction.response.edit_message(content="🚪 Lobby closed.", embed=None, view=None)
            async def _del_closed(m):
                await asyncio.sleep(10)
                try: await m.delete()
                except Exception: pass
            bot.loop.create_task(_del_closed(interaction.message))
            return
        if was_host:
            nh     = lobby["players"][0]
            nh_row = await db_get_player(nh)
            lobby["host"]      = nh
            lobby["host_name"] = nh_row["username"] if nh_row else nh
        lobby["status"] = "open"
        await db_save_lobby(self.lobby_id, lobby)
        players = await db_get_players_by_uids(lobby["players"])
        await interaction.response.edit_message(embed=build_lobby_embed(players, lobby), view=self)


class TeamSelectView(discord.ui.View):
    def __init__(self, lobby_id: str):
        super().__init__(timeout=300)
        self.lobby_id = lobby_id

    @discord.ui.button(label="🔴 Join Team 1", style=discord.ButtonStyle.red)
    async def team1(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._pick_team(interaction, "team1")

    @discord.ui.button(label="🔵 Join Team 2", style=discord.ButtonStyle.blurple)
    async def team2(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._pick_team(interaction, "team2")

    @discord.ui.button(label="⚖️ Auto-Balance", style=discord.ButtonStyle.green)
    async def autobalance(self, interaction: discord.Interaction, button: discord.ui.Button):
        lobby = await db_get_lobby(self.lobby_id)
        if not lobby:
            await interaction.response.send_message("❌ Lobby not found.", ephemeral=True); return
        if str(interaction.user.id) not in lobby.get("players", []):
            await interaction.response.send_message("❌ You are not in this lobby.", ephemeral=True); return
        players = await db_get_players_by_uids(lobby["players"])
        mmr_map = {uid: players[uid]["mmr_2v2"] for uid in lobby["players"]}
        t1, t2  = best_2v2_balance(mmr_map)
        lobby["team1"] = t1
        lobby["team2"] = t2
        lobby["status"] = "teams_set"
        await db_save_lobby(self.lobby_id, lobby)
        embed    = build_lobby_embed(players, lobby)
        ping_str = " ".join(f"<@{p}>" for p in lobby["players"])
        await interaction.response.edit_message(embed=embed, view=ReadyView(self.lobby_id))
        bot.loop.create_task(ready_check_timeout(interaction.guild, self.lobby_id))
        alert = await interaction.followup.send(
            f"🔔 {ping_str} — teams are set! **Click Ready** above within 3 minutes.", wait=True
        )
        lobby["ready_alert_id"]         = str(alert.id)
        lobby["ready_alert_channel_id"] = str(alert.channel.id)
        await db_save_lobby(self.lobby_id, lobby)

    @discord.ui.button(label="🚪 Leave", style=discord.ButtonStyle.grey, row=1)
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        lobby = await db_get_lobby(self.lobby_id)
        if not lobby:
            await interaction.response.send_message("❌ Lobby not found.", ephemeral=True); return
        uid = str(interaction.user.id)
        if uid not in lobby.get("players", []):
            await interaction.response.send_message("❌ You are not in this lobby.", ephemeral=True); return
        # Remove from teams if already picked
        for team_key in ("team1", "team2"):
            if uid in lobby.get(team_key, []):
                lobby[team_key].remove(uid)
        was_host = uid == lobby.get("host")
        lobby["players"].remove(uid)
        await db_update_player(uid, active_lobby=None)
        if len(lobby["players"]) == 0:
            await db_delete_lobby(self.lobby_id)
            await interaction.response.edit_message(content="🚪 Lobby closed.", embed=None, view=None)
            async def _del_closed(m):
                await asyncio.sleep(10)
                try: await m.delete()
                except Exception: pass
            bot.loop.create_task(_del_closed(interaction.message))
            return
        if was_host:
            nh     = lobby["players"][0]
            nh_row = await db_get_player(nh)
            lobby["host"]      = nh
            lobby["host_name"] = nh_row["username"] if nh_row else nh
        lobby["status"] = "open"
        await db_save_lobby(self.lobby_id, lobby)
        players = await db_get_players_by_uids(lobby["players"])
        await interaction.response.edit_message(embed=build_lobby_embed(players, lobby), view=LobbyView(self.lobby_id))

    async def _pick_team(self, interaction: discord.Interaction, team: str):
        lobby = await db_get_lobby(self.lobby_id)
        if not lobby:
            await interaction.response.send_message("❌ Lobby not found.", ephemeral=True); return
        uid   = str(interaction.user.id)
        # Only lobby members can pick a team
        if uid not in lobby.get("players", []):
            await interaction.response.send_message("❌ You are not in this lobby.", ephemeral=True); return
        other = "team2" if team == "team1" else "team1"
        if team  not in lobby: lobby[team]  = []
        if other not in lobby: lobby[other] = []
        if uid in lobby[other]: lobby[other].remove(uid)
        if uid not in lobby[team]:
            if len(lobby[team]) >= 2:
                await interaction.response.send_message("❌ That team is full!", ephemeral=True); return
            lobby[team].append(uid)
        players = await db_get_players_by_uids(lobby["players"])
        embed   = build_lobby_embed(players, lobby)
        if len(lobby.get("team1", [])) == 2 and len(lobby.get("team2", [])) == 2:
            lobby["status"] = "teams_set"
            await db_save_lobby(self.lobby_id, lobby)
            ping_str = " ".join(f"<@{p}>" for p in lobby["players"])
            await interaction.response.edit_message(embed=embed, view=ReadyView(self.lobby_id))
            bot.loop.create_task(ready_check_timeout(interaction.guild, self.lobby_id))
            alert = await interaction.followup.send(
                f"🔔 {ping_str} — teams are set! **Click Ready** above within 3 minutes.", wait=True
            )
            lobby["ready_alert_id"]         = str(alert.id)
            lobby["ready_alert_channel_id"] = str(alert.channel.id)
            await db_save_lobby(self.lobby_id, lobby)
        else:
            await db_save_lobby(self.lobby_id, lobby)
            await interaction.response.edit_message(embed=embed, view=self)


class ReadyView(discord.ui.View):
    def __init__(self, lobby_id: str):
        super().__init__(timeout=180)
        self.lobby_id = lobby_id

    @discord.ui.button(label="✅ Ready", style=discord.ButtonStyle.green)
    async def ready(self, interaction: discord.Interaction, button: discord.ui.Button):
        lobby = await db_get_lobby(self.lobby_id)
        if not lobby:
            await interaction.response.send_message("❌ Lobby not found.", ephemeral=True); return
        uid = str(interaction.user.id)
        if uid not in lobby["players"]:
            await interaction.response.send_message("❌ You are not in this lobby.", ephemeral=True); return
        if uid not in lobby.get("ready", []):
            lobby.setdefault("ready", []).append(uid)
        await db_save_lobby(self.lobby_id, lobby)
        if sorted(lobby["ready"]) == sorted(lobby["players"]):
            await delete_ready_alert(interaction.guild, self.lobby_id)
            await launch_private_lobby(interaction, lobby, self.lobby_id)
        else:
            remaining = len(lobby["players"]) - len(lobby["ready"])
            await interaction.response.send_message(
                f"✅ You are ready! Waiting for {remaining} more player(s)...", ephemeral=True
            )

    @discord.ui.button(label="🚪 Leave", style=discord.ButtonStyle.red)
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        lobby = await db_get_lobby(self.lobby_id)
        if not lobby:
            await interaction.response.send_message("❌ Lobby not found.", ephemeral=True); return
        uid = str(interaction.user.id)
        if uid not in lobby["players"]:
            await interaction.response.send_message("❌ You are not in this lobby.", ephemeral=True); return
        lobby["players"].remove(uid)
        if uid in lobby.get("ready", []): lobby["ready"].remove(uid)
        await db_update_player(uid, active_lobby=None)
        if len(lobby["players"]) == 0:
            await db_delete_lobby(self.lobby_id)
            await interaction.response.edit_message(content="🚪 Lobby closed.", embed=None, view=None)
            async def _del_closed(m):
                await asyncio.sleep(10)
                try: await m.delete()
                except Exception: pass
            bot.loop.create_task(_del_closed(interaction.message))
            return
        if uid == lobby.get("host"):
            nh     = lobby["players"][0]
            nh_row = await db_get_player(nh)
            lobby["host"]      = nh
            lobby["host_name"] = nh_row["username"] if nh_row else nh
        lobby["status"] = "open"
        await db_save_lobby(self.lobby_id, lobby)
        players = await db_get_players_by_uids(lobby["players"])
        await interaction.response.edit_message(embed=build_lobby_embed(players, lobby), view=LobbyView(self.lobby_id))


async def ready_check_timeout(guild, lobby_id: str):
    await asyncio.sleep(180)
    lobby = await db_get_lobby(lobby_id)
    if not lobby or lobby.get("status") in ("launched", "complete"): return
    not_ready = [u for u in lobby["players"] if u not in lobby.get("ready", [])]
    if not_ready:
        await delete_ready_alert(guild, lobby_id)
        for uid in lobby["players"]:
            await db_update_player(uid, active_lobby=None)
        await db_delete_lobby(lobby_id)


async def launch_private_lobby(interaction: discord.Interaction, lobby: dict, lobby_id: str):
    guild   = interaction.guild
    channel = await create_private_channel(guild, lobby_id, lobby["players"], lobby["mode"], lobby.get("lobby_name", ""))
    lobby["private_channel"] = str(channel.id)
    lobby["status"] = "launched"
    await db_save_lobby(lobby_id, lobby)

    mode    = lobby["mode"]
    players = await db_get_players_by_uids(lobby["players"])
    ping_str = " ".join(f"<@{uid}>" for uid in lobby["players"])

    embed = discord.Embed(
        title=f"{'⚔️' if mode == '1v1' else '🤝'} {mode.upper()} Ranked Match",
        description=(
            "**How to start your match:**\n"
            "1️⃣ One player creates a **Custom Game** in Valorant and shares the lobby code here\n"
            "2️⃣ Can't agree on who makes it? Use `/coinflip` — it'll pick for you\n"
            "3️⃣ Play your match, then hit **🏆 I Won** or **💀 I Lost** below\n"
            "4️⃣ The other side confirms — MMR updates instantly\n\n"
            "📸 **Dispute?** Post a screenshot here as evidence, then hit **❌ Dispute** to flag for staff."
        ),
        color=discord.Color.green(),
    )
    if mode == "2v2":
        t1   = lobby.get("team1", lobby["players"][:2])
        t2   = lobby.get("team2", lobby["players"][2:])
        mmr1 = sum(players[u]["mmr_2v2"] for u in t1) // 2
        mmr2 = sum(players[u]["mmr_2v2"] for u in t2) // 2
        embed.add_field(name=f"🔴 Team 1 (avg {mmr1} MMR)",
                        value="\n".join(f"• {players[u]['username']}" for u in t1), inline=True)
        embed.add_field(name=f"🔵 Team 2 (avg {mmr2} MMR)",
                        value="\n".join(f"• {players[u]['username']}" for u in t2), inline=True)
    else:
        for uid in lobby["players"]:
            p = players.get(uid, {})
            embed.add_field(
                name=p.get("username", "Player"),
                value=f"{rank_label_g(p.get('mmr_1v1', STARTING_MMR))} | {p.get('mmr_1v1', STARTING_MMR)} MMR",
                inline=True,
            )
    embed.set_footer(text="Use the buttons below to report your match result.")

    intro = discord.Embed(title="Players", color=discord.Color.blurple())
    for uid in lobby["players"]:
        p      = players.get(uid, {})
        mmr    = p.get(f"mmr_{mode}", STARTING_MMR)
        w      = p.get(f"wins_{mode}", 0)
        l      = p.get(f"losses_{mode}", 0)
        streak = p.get(f"streak_{mode}", 0)
        wr     = f"{round(w / (w + l) * 100)}%" if (w + l) > 0 else "N/A"
        extra  = f" | 🔥{streak}" if streak >= 3 else ""
        intro.add_field(
            name=p.get("username", "Player"),
            value=f"{rank_label_g(mmr)} | {mmr} MMR | {w}W/{l}L | {wr}{extra}",
            inline=True,
        )
    await channel.send(embed=intro)
    await channel.send(ping_str, embed=embed, view=MatchView(lobby_id))
    try: await interaction.message.delete()
    except Exception: pass

    class GoToLobbyView(discord.ui.View):
        def __init__(self_inner):
            super().__init__(timeout=60)
            self_inner.add_item(discord.ui.Button(
                label="➡️ Go to Match",
                style=discord.ButtonStyle.link,
                url=f"https://discord.com/channels/{guild.id}/{channel.id}",
            ))

    notify_embed = discord.Embed(
        title="✅ Match Started!",
        description=f"Your private match channel is ready.\n{channel.mention}",
        color=discord.Color.green(),
    )
    await interaction.response.send_message(embed=notify_embed, view=GoToLobbyView())
    try:
        notify_msg = await interaction.original_response()
        lobby["notify_message_id"]      = str(notify_msg.id)
        lobby["notify_message_channel"] = str(interaction.channel.id)
        await db_save_lobby(lobby_id, lobby)
    except Exception:
        pass
    bot.loop.create_task(inactivity_timeout(channel, lobby_id))


# ── Match View ────────────────────────────────────────────────────────────────

class MatchView(discord.ui.View):
    def __init__(self, lobby_id: str):
        super().__init__(timeout=None)
        self.lobby_id = lobby_id

    @discord.ui.button(label="🏆 I Won", style=discord.ButtonStyle.green)
    async def report_win(self, interaction: discord.Interaction, button: discord.ui.Button):
        lobby = await db_get_lobby(self.lobby_id)
        if not lobby or lobby.get("status") == "complete":
            await interaction.response.send_message("❌ This lobby is already closed.", ephemeral=True); return
        uid = str(interaction.user.id)
        if uid not in lobby["players"]:
            await interaction.response.send_message("❌ You are not in this lobby.", ephemeral=True); return
        if await db_lobby_has_pending(self.lobby_id):
            await interaction.response.send_message(
                "⏳ A report is already pending. Wait for the other player to respond.", ephemeral=True
            ); return
        mode = lobby["mode"]
        if mode == "1v1":
            winners = [uid]
            losers  = [u for u in lobby["players"] if u != uid]
        else:
            t1 = lobby.get("team1", lobby["players"][:2])
            t2 = lobby.get("team2", lobby["players"][2:])
            winners = t1 if uid in t1 else t2
            losers  = t2 if uid in t1 else t1
        players = await db_get_players_by_uids(lobby["players"])
        w_avg   = sum(players[u][f"mmr_{mode}"] for u in winners) // len(winners)
        l_avg   = sum(players[u][f"mmr_{mode}"] for u in losers)  // len(losers)
        gain, loss = calc_mmr_change(w_avg, l_avg)
        pending_id = f"p_{uid}_{int(datetime.now(timezone.utc).timestamp())}"
        await db_save_pending(pending_id, {
            "mode": mode, "winners": winners, "losers": losers,
            "gain": gain, "loss": loss, "lobby_id": self.lobby_id, "reported_by": uid,
        })
        loser_mentions = " ".join(f"<@{u}>" for u in losers)
        embed = discord.Embed(
            title="🏆 Win Reported",
            description=f"**{interaction.user.display_name}** says they won.\n\n{loser_mentions} — confirm below.",
            color=discord.Color.yellow(),
        )
        embed.add_field(name="MMR at stake", value=f"+{gain} for winners | -{loss} for losers")
        await interaction.response.send_message(embed=embed, view=ConfirmView(pending_id))

    @discord.ui.button(label="💀 I Lost", style=discord.ButtonStyle.blurple)
    async def report_loss(self, interaction: discord.Interaction, button: discord.ui.Button):
        lobby = await db_get_lobby(self.lobby_id)
        if not lobby or lobby.get("status") == "complete":
            await interaction.response.send_message("❌ This lobby is already closed.", ephemeral=True); return
        uid = str(interaction.user.id)
        if uid not in lobby["players"]:
            await interaction.response.send_message("❌ You are not in this lobby.", ephemeral=True); return
        if await db_lobby_has_pending(self.lobby_id):
            await interaction.response.send_message("⏳ A report is already pending.", ephemeral=True); return
        mode = lobby["mode"]
        if mode == "1v1":
            losers  = [uid]
            winners = [u for u in lobby["players"] if u != uid]
        else:
            t1 = lobby.get("team1", lobby["players"][:2])
            t2 = lobby.get("team2", lobby["players"][2:])
            losers  = t1 if uid in t1 else t2
            winners = t2 if uid in t1 else t1
        players = await db_get_players_by_uids(lobby["players"])
        w_avg   = sum(players[u][f"mmr_{mode}"] for u in winners) // len(winners)
        l_avg   = sum(players[u][f"mmr_{mode}"] for u in losers)  // len(losers)
        gain, loss = calc_mmr_change(w_avg, l_avg)
        pending_id = f"p_{uid}_{int(datetime.now(timezone.utc).timestamp())}"
        await db_save_pending(pending_id, {
            "mode": mode, "winners": winners, "losers": losers,
            "gain": gain, "loss": loss, "lobby_id": self.lobby_id, "reported_by": uid,
        })
        winner_mentions = " ".join(f"<@{u}>" for u in winners)
        embed = discord.Embed(
            title="💀 Loss Reported",
            description=f"**{interaction.user.display_name}** says they lost.\n\n{winner_mentions} — confirm below.",
            color=discord.Color.yellow(),
        )
        embed.add_field(name="MMR at stake", value=f"+{gain} for winners | -{loss} for losers")
        await interaction.response.send_message(embed=embed, view=ConfirmView(pending_id))

    @discord.ui.button(label="🏳️ Forfeit", style=discord.ButtonStyle.red)
    async def forfeit(self, interaction: discord.Interaction, button: discord.ui.Button):
        lobby = await db_get_lobby(self.lobby_id)
        if not lobby or lobby.get("status") == "complete":
            await interaction.response.send_message("❌ This lobby is already closed.", ephemeral=True); return
        uid = str(interaction.user.id)
        if uid not in lobby["players"]:
            await interaction.response.send_message("❌ You are not in this lobby.", ephemeral=True); return
        mode = lobby["mode"]
        if mode == "1v1":
            losers  = [uid]
            winners = [u for u in lobby["players"] if u != uid]
        else:
            t1 = lobby.get("team1", lobby["players"][:2])
            t2 = lobby.get("team2", lobby["players"][2:])
            losers  = t1 if uid in t1 else t2
            winners = t2 if uid in t1 else t1
        result_embed  = await apply_match(interaction.guild, winners, losers, mode, self.lobby_id)
        await post_match_log(interaction.guild, result_embed)
        ff_embed = discord.Embed(
            title="🏳️ Forfeit",
            description=f"**{interaction.user.display_name}** forfeited.",
            color=discord.Color.red(),
        )
        all_players = winners + losers
        rematch_view = RematchView(mode, all_players, interaction.channel, self.lobby_id)
        await interaction.response.send_message(embeds=[ff_embed, result_embed], view=rematch_view)

    @discord.ui.button(label="🔧 Force Close — Staff Only", style=discord.ButtonStyle.grey, row=1)
    async def force_close(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not _is_staff(interaction):
            await interaction.response.send_message(
                "❌ Staff only — you must have owner, admin, moderator, or helper role.", ephemeral=True
            ); return
        lobby = await db_get_lobby(self.lobby_id)
        if lobby:
            for u in lobby.get("players", []):
                await db_update_player(u, active_lobby=None)
            lobby["status"] = "complete"
            await db_save_lobby(self.lobby_id, lobby)
        await db_clear_pending_for_lobby(self.lobby_id)
        await interaction.response.send_message(
            f"🔧 Force closed by **{interaction.user.display_name}**. No MMR changed. Deleting in 10s..."
        )
        bot.loop.create_task(delete_channel_after(interaction.channel, 10))


# ── Dispute Resolution ────────────────────────────────────────────────────────

class DisputeResolutionView(discord.ui.View):
    """Staff-only dropdown to resolve a disputed match and apply MMR."""
    def __init__(self, mode: str, all_players: list, lobby_id: str, player_names: dict):
        super().__init__(timeout=None)
        self.mode        = mode
        self.all_players = all_players
        self.lobby_id    = lobby_id
        self.resolved    = False
        options = [
            discord.SelectOption(label=f"🏆 {player_names.get(uid, uid)} wins", value=uid)
            for uid in all_players
        ]
        select          = discord.ui.Select(placeholder="⚔️ Select the winner...", options=options)
        select.callback = self.winner_selected
        self.add_item(select)

    async def winner_selected(self, interaction: discord.Interaction):
        if not _is_staff(interaction):
            await interaction.response.send_message("❌ Only staff can resolve disputes.", ephemeral=True); return
        if self.resolved:
            await interaction.response.send_message("This dispute has already been resolved.", ephemeral=True); return
        self.resolved = True
        self.stop()
        winner_uid = interaction.data["values"][0]
        if self.mode == "1v1":
            winners = [winner_uid]
            losers  = [u for u in self.all_players if u != winner_uid]
        else:
            lobby = await db_get_lobby(self.lobby_id)
            t1 = lobby.get("team1", self.all_players[:2]) if lobby else self.all_players[:2]
            t2 = lobby.get("team2", self.all_players[2:]) if lobby else self.all_players[2:]
            winners = t1 if winner_uid in t1 else t2
            losers  = [u for u in self.all_players if u not in winners]
        result_embed = await apply_match(interaction.guild, winners, losers, self.mode, self.lobby_id)
        await post_match_log(interaction.guild, result_embed)
        players     = await db_get_players_by_uids([winner_uid])
        winner_name = players.get(winner_uid, {}).get("username", winner_uid)
        resolve_embed = discord.Embed(
            title="✅ Dispute Resolved by Staff",
            description=f"**{interaction.user.display_name}** ruled **{winner_name}** as the winner.\nMMR has been updated.",
            color=discord.Color.green(),
        )
        await interaction.response.edit_message(content="", embed=resolve_embed, view=None)
        await interaction.followup.send(embed=result_embed)


# ── Confirm View ──────────────────────────────────────────────────────────────

class ConfirmView(discord.ui.View):
    """Shown after a win/loss is reported — other player confirms or disputes."""
    def __init__(self, pending_id: str):
        super().__init__(timeout=300)
        self.pending_id = pending_id
        self.resolved   = False

    @discord.ui.button(label="✅ Confirm", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.resolved:
            await interaction.response.send_message("Already resolved.", ephemeral=True); return
        pending = await db_get_pending(self.pending_id)
        if not pending:
            await interaction.response.send_message("❌ Report expired or already resolved.", ephemeral=True); return
        uid = str(interaction.user.id)
        all_players = pending["winners"] + pending["losers"]
        if uid not in all_players:
            await interaction.response.send_message("❌ You are not in this match.", ephemeral=True); return
        # The confirmer must be on the OPPOSING side to the reporter
        # (in 1v1 this is just "not the reporter"; in 2v2 it's "not on the reporter's team")
        reporter     = pending.get("reported_by")
        reporter_on_winners = reporter in pending["winners"]
        uid_on_winners      = uid in pending["winners"]
        if uid_on_winners == reporter_on_winners:
            # Same team as reporter — can't self-confirm
            await interaction.response.send_message(
                "❌ Only the opposing team can confirm this result.", ephemeral=True
            ); return
        self.resolved = True
        self.stop()
        result_embed = await apply_match(
            interaction.guild, pending["winners"], pending["losers"],
            pending["mode"], pending.get("lobby_id")
        )
        await db_delete_pending(self.pending_id)
        await post_match_log(interaction.guild, result_embed)
        rematch_view = RematchView(pending["mode"], all_players, interaction.channel, pending.get("lobby_id", ""))
        await interaction.response.send_message(embed=result_embed, view=rematch_view)

    @discord.ui.button(label="❌ Dispute", style=discord.ButtonStyle.red)
    async def dispute(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.resolved:
            await interaction.response.send_message("Already resolved.", ephemeral=True); return
        pending = await db_get_pending(self.pending_id)
        if not pending:
            await interaction.response.send_message("❌ Report not found.", ephemeral=True); return
        uid = str(interaction.user.id)
        if uid == pending.get("reported_by"):
            await interaction.response.send_message("❌ You can't dispute your own report.", ephemeral=True); return
        all_players = pending["winners"] + pending["losers"]
        if uid not in all_players:
            await interaction.response.send_message("❌ You are not in this match.", ephemeral=True); return
        reporter          = pending.get("reported_by")
        reporter_on_winners = reporter in pending["winners"]
        uid_on_winners      = uid in pending["winners"]
        if uid_on_winners == reporter_on_winners:
            await interaction.response.send_message(
                "❌ Only the opposing team can dispute this result.", ephemeral=True
            ); return
        self.resolved = True
        self.stop()
        mode         = pending["mode"]
        lobby_id     = pending.get("lobby_id", "")
        players_data = await db_get_players_by_uids(all_players)
        player_names = {uid: players_data.get(uid, {}).get("username", uid) for uid in all_players}
        await db_delete_pending(self.pending_id)
        cfg = load_config()
        staff_mentions = " ".join(
            role.mention for role in interaction.guild.roles if role.name.lower() in STAFF_ROLES
        ) or "Staff"
        dispute_embed = discord.Embed(
            title="⚠️ Match Disputed — Staff Required",
            description=f"A player disputed the result in {interaction.channel.mention}. No MMR changed.\nUse the dropdown below to select the winner.",
            color=discord.Color.orange(),
        )
        for p_uid in all_players:
            p    = players_data.get(p_uid, {})
            name = p.get("username", p_uid)
            mmr  = p.get(f"mmr_{mode}", 1000)
            dispute_embed.add_field(name=name, value=f"{rank_label_g(mmr)} | {mmr} MMR", inline=True)
        resolution_view = DisputeResolutionView(mode, all_players, lobby_id, player_names)
        await interaction.response.send_message(
            f"⚠️ {staff_mentions} — a match result has been disputed!",
            embed=dispute_embed,
            view=resolution_view,
        )
        log_ch = cfg.get("match_log_channel")
        if log_ch:
            ch = interaction.guild.get_channel(int(log_ch))
            if ch:
                await ch.send(f"⚠️ {staff_mentions} Dispute in {interaction.channel.mention}", embed=dispute_embed)


# ── Rematch Views ─────────────────────────────────────────────────────────────

class RematchConfirmView(discord.ui.View):
    """Sent to the other player after someone requests a rematch."""
    def __init__(self, requester_id, requester_name, mode, player_ids, lobby_id, original_message, rematch_count):
        super().__init__(timeout=60)
        self.requester_id     = requester_id
        self.requester_name   = requester_name
        self.mode             = mode
        self.player_ids       = player_ids
        self.lobby_id         = lobby_id
        self.original_message = original_message
        self.rematch_count    = rematch_count
        self.resolved         = False

    @discord.ui.button(label="✅ Confirm Rematch", style=discord.ButtonStyle.green)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = str(interaction.user.id)
        if uid == self.requester_id:
            await interaction.response.send_message("⏳ Waiting for the other player to confirm...", ephemeral=True); return
        if uid not in self.player_ids:
            await interaction.response.send_message("❌ You are not in this match.", ephemeral=True); return
        if self.resolved:
            await interaction.response.send_message("Already resolved.", ephemeral=True); return
        self.resolved = True
        self.stop()
        lobby = await db_get_lobby(self.lobby_id)
        if lobby:
            lobby["players"]       = list(self.player_ids)
            lobby["status"]        = "launched"
            lobby["ready"]         = []
            lobby["last_activity"] = datetime.now(timezone.utc).isoformat()
            lobby["rematch_count"] = self.rematch_count
            # Keep team assignments for 2v2 — don't wipe them
            await db_save_lobby(self.lobby_id, lobby)
        for pid in self.player_ids:
            await db_update_player(pid, active_lobby=self.lobby_id)
        await interaction.response.defer()
        channel = interaction.channel
        try:
            msgs = [m async for m in channel.history(limit=30) if m.author == bot.user]
            if msgs: await channel.delete_messages(msgs)
        except Exception:
            pass
        mode      = self.mode
        ping_str  = " ".join(f"<@{p}>" for p in self.player_ids)
        remaining = 3 - self.rematch_count
        players   = await db_get_players_by_uids(self.player_ids)
        match_embed = discord.Embed(
            title=f"{'⚔️' if mode == '1v1' else '🤝'} {mode.upper()} Ranked Match — Rematch {self.rematch_count}/3",
            description="Share your Valorant custom lobby code here. Report the result when done.",
            color=discord.Color.green(),
        )
        if mode == "2v2" and lobby and lobby.get("team1") and lobby.get("team2"):
            t1   = lobby["team1"]
            t2   = lobby["team2"]
            mmr1 = sum(players[u].get("mmr_2v2", STARTING_MMR) for u in t1) // 2
            mmr2 = sum(players[u].get("mmr_2v2", STARTING_MMR) for u in t2) // 2
            match_embed.add_field(
                name=f"🔴 Team 1 (avg {mmr1} MMR)",
                value="\n".join(f"• {players[u]['username']}" for u in t1), inline=True
            )
            match_embed.add_field(
                name=f"🔵 Team 2 (avg {mmr2} MMR)",
                value="\n".join(f"• {players[u]['username']}" for u in t2), inline=True
            )
        else:
            for pid in self.player_ids:
                p = players.get(pid, {})
                match_embed.add_field(
                    name=p.get("username", "Player"),
                    value=f"{rank_label_g(p.get(f'mmr_{mode}', STARTING_MMR))} | {p.get(f'mmr_{mode}', STARTING_MMR)} MMR",
                    inline=True,
                )
        footer = (f"{remaining} rematch{'es' if remaining != 1 else ''} remaining after this"
                  if remaining > 0 else "This was the final rematch — no more rematches available")
        match_embed.set_footer(text=footer)
        await channel.send(ping_str, embed=match_embed, view=MatchView(self.lobby_id))

    @discord.ui.button(label="❌ Decline", style=discord.ButtonStyle.red)
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = str(interaction.user.id)
        if uid == self.requester_id:
            await interaction.response.send_message("❌ You can't decline your own request.", ephemeral=True); return
        if uid not in self.player_ids:
            await interaction.response.send_message("❌ You are not in this match.", ephemeral=True); return
        if self.resolved:
            await interaction.response.send_message("Already resolved.", ephemeral=True); return
        self.resolved = True
        self.stop()
        lobby = await db_get_lobby(self.lobby_id)
        if lobby:
            for pid in self.player_ids:
                await db_update_player(pid, active_lobby=None)
            lobby["status"] = "complete"
            await db_save_lobby(self.lobby_id, lobby)
        await interaction.response.edit_message(
            content=f"❌ **{interaction.user.display_name}** declined the rematch. Channel deleting in 30s...",
            embed=None, view=None,
        )
        bot.loop.create_task(delete_channel_after(interaction.channel, 30))

    async def on_timeout(self):
        if self.resolved: return
        try:
            await self.original_message.edit(content="⏱️ Rematch request expired.", embed=None, view=None)
        except Exception:
            pass


class RematchView(discord.ui.View):
    """Post-match: Rematch or Leave."""
    def __init__(self, mode: str, player_ids: list, channel, lobby_id: str):
        super().__init__(timeout=300)
        self.mode       = mode
        self.player_ids = player_ids
        self.channel    = channel
        self.lobby_id   = lobby_id
        self.resolved   = False

    @discord.ui.button(label="🔄 Rematch", style=discord.ButtonStyle.blurple)
    async def rematch(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = str(interaction.user.id)
        if uid not in self.player_ids:
            await interaction.response.send_message("❌ Only players from this match can request a rematch.", ephemeral=True); return
        if self.resolved:
            await interaction.response.send_message("⏳ A rematch request is already pending.", ephemeral=True); return
        lobby         = await db_get_lobby(self.lobby_id)
        rematch_count = (lobby.get("rematch_count", 0) if lobby else 0) + 1
        if rematch_count > 3:
            await interaction.response.send_message(
                "❌ Maximum of 3 rematches reached. Use 🚪 Leave to close the lobby.", ephemeral=True
            ); return
        self.resolved = True
        self.stop()
        players_data  = await db_get_players_by_uids(self.player_ids)
        requester     = players_data.get(uid, {}).get("username", interaction.user.display_name)
        other_ids     = [p for p in self.player_ids if p != uid]
        other_mention = " ".join(f"<@{p}>" for p in other_ids)
        remaining_after = 3 - rematch_count
        label = f"🔄 Rematch {rematch_count}/3" + (" (Final rematch!)" if remaining_after == 0 else "")
        footer_note = (
            "⚠️ This is the **final** rematch allowed."
            if remaining_after == 0
            else f"({remaining_after} rematch{'es' if remaining_after != 1 else ''} will remain after this)"
        )
        embed = discord.Embed(
            title=label,
            description=f"**{requester}** wants a rematch!\n\n{other_mention} — do you accept?\n\n{footer_note}",
            color=discord.Color.blurple(),
        )
        for child in self.children:
            child.disabled = True
        try: await interaction.message.edit(view=self)
        except Exception: pass
        confirm_view = RematchConfirmView(
            uid, requester, self.mode, self.player_ids, self.lobby_id, interaction.message, rematch_count
        )
        await interaction.response.send_message(embed=embed, view=confirm_view)

    @discord.ui.button(label="🚪 Leave", style=discord.ButtonStyle.red)
    async def leave(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = str(interaction.user.id)
        if uid not in self.player_ids:
            await interaction.response.send_message("❌ You are not in this match.", ephemeral=True); return
        lobby = await db_get_lobby(self.lobby_id)
        if lobby:
            for pid in self.player_ids:
                await db_update_player(pid, active_lobby=None)
            lobby["status"] = "complete"
            await db_save_lobby(self.lobby_id, lobby)
        players_data = await db_get_players_by_uids([uid])
        leaver       = players_data.get(uid, {}).get("username", interaction.user.display_name)
        await interaction.response.send_message(f"🚪 **{leaver}** left. Channel deleting in 10s...")
        bot.loop.create_task(delete_notify_message(interaction.guild, self.lobby_id))
        bot.loop.create_task(delete_channel_after(interaction.channel, 10))

    async def on_timeout(self):
        try:
            bot.loop.create_task(delete_notify_message(self.channel.guild, self.lobby_id))
        except Exception:
            pass
        try: await self.channel.delete()
        except Exception: pass


# ── Inactivity ────────────────────────────────────────────────────────────────

class ExtendTimeView(discord.ui.View):
    def __init__(self, lobby_id: str):
        super().__init__(timeout=600)
        self.lobby_id = lobby_id
        self.extended = False
        self.message  = None

    @discord.ui.button(label="✅ Yes, need more time (+30 min)", style=discord.ButtonStyle.green)
    async def extend(self, interaction: discord.Interaction, button: discord.ui.Button):
        lobby = await db_get_lobby(self.lobby_id)
        if not lobby:
            await interaction.response.send_message("❌ Lobby not found.", ephemeral=True); return
        if str(interaction.user.id) not in lobby.get("players", []):
            await interaction.response.send_message("❌ You are not in this lobby.", ephemeral=True); return
        self.extended    = True
        lobby["extended"] = True
        await db_save_lobby(self.lobby_id, lobby)
        self.stop()
        await interaction.response.send_message("✅ 30 extra minutes added!")

    async def on_timeout(self):
        if self.extended: return
        lobby = await db_get_lobby(self.lobby_id)
        if lobby and lobby.get("status") != "complete":
            for uid in lobby.get("players", []):
                await db_update_player(uid, active_lobby=None)
            await db_delete_lobby(self.lobby_id)
        try:
            ch = self.message.channel if self.message else None
            if ch:
                await ch.send("⏱️ No response — lobby closed.")
                await asyncio.sleep(5)
                await ch.delete()
        except Exception:
            pass


async def inactivity_timeout(channel, lobby_id: str):
    # ── 60-min warning to match log ──
    await asyncio.sleep(3600)  # 60 min
    lobby = await db_get_lobby(lobby_id)
    if not lobby or lobby.get("status") == "complete": return
    cfg    = load_config()
    log_id = cfg.get("match_log_channel")
    if log_id:
        log_ch = channel.guild.get_channel(int(log_id))
        if log_ch:
            warn_embed = discord.Embed(
                title="Match Running Long",
                description=f"A match in {channel.mention} has been open for **60 minutes** with no result reported.\nPlayers: {' '.join(f'<@{u}>' for u in lobby.get('players', []))}",
                color=discord.Color.orange(),
                timestamp=datetime.now(timezone.utc),
            )
            try: await log_ch.send(embed=warn_embed)
            except Exception: pass
    # ── 45-min extend prompt (2700s from launch, so 15 more min = 900s) ──
    await asyncio.sleep(900)  # total 75 min since launch → ask to extend
    lobby = await db_get_lobby(lobby_id)
    if not lobby or lobby.get("status") == "complete": return
    mentions = " ".join(f"<@{uid}>" for uid in lobby.get("players", []))
    view = ExtendTimeView(lobby_id)
    try:
        msg = await channel.send(
            mentions + " — This lobby has been open for 45 minutes with no result. Need more time?",
            view=view,
        )
        view.message = msg
    except Exception:
        return
    await asyncio.sleep(600)  # 10 min for response
    lobby = await db_get_lobby(lobby_id)
    if not lobby or lobby.get("status") == "complete": return
    if lobby.get("extended"):
        await asyncio.sleep(1800)  # 30 more min
        lobby = await db_get_lobby(lobby_id)
        if not lobby or lobby.get("status") == "complete": return
        for uid in lobby.get("players", []):
            await db_update_player(uid, active_lobby=None)
        await db_delete_lobby(lobby_id)
        try:
            await channel.send("⏱️ Time's up — lobby closed.")
            await asyncio.sleep(5)
            await channel.delete()
        except Exception:
            pass


async def inactivity_timeout_open(channel, lobby_id: str):
    await asyncio.sleep(2700)  # 45 min
    lobby = await db_get_lobby(lobby_id)
    if not lobby or lobby.get("status") != "open": return
    for uid in lobby.get("players", []):
        await db_update_player(uid, active_lobby=None)
    await db_delete_lobby(lobby_id)
    try:
        async for msg in channel.history(limit=20):
            if msg.author == bot.user and msg.embeds:
                title = msg.embeds[0].title or ""
                if "Ranked Lobby" in title:
                    await msg.delete()
                    break
    except Exception:
        pass


# ── Challenge ─────────────────────────────────────────────────────────────────

async def _challenge_warning(view: "ChallengeView", mention: str):
    """Sends a 1-minute warning ping at the 4-minute mark."""
    await asyncio.sleep(240)  # wait 4 minutes
    if view.accepted or view.is_finished(): return
    try:
        if view.message:
            await view.message.reply(
                f"{mention} — you have **1 minute** to accept or decline this challenge before it expires.",
                mention_author=False,
            )
    except Exception:
        pass


class ChallengeView(discord.ui.View):
    def __init__(self, challenger_id: str, challenged_id: str, challenged_mention: str):
        super().__init__(timeout=300)  # 5 minutes
        self.challenger_id    = challenger_id
        self.challenged_id    = challenged_id
        self.challenged_mention = challenged_mention
        self.accepted         = False
        self.message          = None  # set after send

    @discord.ui.button(label="✅ Accept", style=discord.ButtonStyle.green)
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.challenged_id:
            await interaction.response.send_message("❌ This challenge is not for you.", ephemeral=True); return
        for uid in [self.challenger_id, self.challenged_id]:
            p = await db_get_player(uid)
            if p and p.get("active_lobby"):
                existing = await db_get_lobby(p["active_lobby"])
                ch_id    = existing.get("private_channel") if existing else None
                name     = p.get("username", "A player")
                if ch_id:
                    ch = interaction.guild.get_channel(int(ch_id))
                    if ch:
                        await interaction.response.send_message(
                            f"❌ **{name}** is already in an active lobby: {ch.mention}", ephemeral=True
                        ); return
                await interaction.response.send_message(
                    f"❌ **{name}** is stuck in a ghost lobby. They need to use `/leavelobby` first.", ephemeral=True
                ); return
        self.accepted = True
        self.stop()
        lobby_id     = f"ch_{self.challenger_id}_{int(datetime.now(timezone.utc).timestamp())}"
        lobby_num    = await db_increment_lobby_counter("challenge")
        challenger_p = await db_get_player(self.challenger_id)
        lobby = {
            "mode": "1v1", "host": self.challenger_id,
            "host_name": challenger_p["username"] if challenger_p else "Unknown",
            "players": [self.challenger_id, self.challenged_id],
            "status": "full", "ready": [],
            "last_activity": datetime.now(timezone.utc).isoformat(),
            "lobby_name": f"Challenge #{lobby_num}",
        }
        await db_save_lobby(lobby_id, lobby)
        for uid in [self.challenger_id, self.challenged_id]:
            await db_update_player(uid, active_lobby=lobby_id)
        players = await db_get_players_by_uids(lobby["players"])
        embed   = build_lobby_embed(players, lobby)
        await interaction.response.edit_message(content="✅ Challenge accepted! Ready up:", embed=embed, view=ReadyView(lobby_id))
        bot.loop.create_task(ready_check_timeout(interaction.guild, lobby_id))

    @discord.ui.button(label="❌ Decline", style=discord.ButtonStyle.red)
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.challenged_id:
            await interaction.response.send_message("❌ This challenge is not for you.", ephemeral=True); return
        self.stop()
        await interaction.response.edit_message(content="❌ Challenge declined.", embed=None, view=None)

    async def on_timeout(self):
        if self.accepted: return
        try:
            if self.message:
                await self.message.edit(
                    content=f"⏱️ Challenge expired — {self.challenged_mention} didn't respond in time.",
                    embed=None, view=None
                )
        except Exception:
            pass


# ── History View ──────────────────────────────────────────────────────────────

class HistoryView(discord.ui.View):
    """Paginated match history browser."""
    PAGE_SIZE = 8

    def __init__(self, player_uid: str, username: str, history: list,
                 mmr_1v1: int, mmr_2v2: int, mode_filter: str = "all"):
        super().__init__(timeout=120)
        self.player_uid  = player_uid
        self.username    = username
        self.mmr_1v1     = mmr_1v1
        self.mmr_2v2     = mmr_2v2
        self.mode_filter = mode_filter
        self.all_history = list(reversed(history))  # newest first
        self.page        = 0
        self._update_buttons()

    def filtered(self):
        if self.mode_filter == "all":
            return self.all_history
        return [h for h in self.all_history if h.get("mode") == self.mode_filter]

    def total_pages(self):
        return max(1, -(-len(self.filtered()) // self.PAGE_SIZE))

    def _update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page >= self.total_pages() - 1
        self.mode_btn.label    = f"Filter: {self.mode_filter.upper()}"

    def build_embed(self) -> discord.Embed:
        entries      = self.filtered()
        total        = len(entries)
        page_entries = entries[self.page * self.PAGE_SIZE: (self.page + 1) * self.PAGE_SIZE]
        embed = discord.Embed(title=f"Match History — {self.username}", color=discord.Color.blue())
        embed.add_field(
            name="Current MMR",
            value=f"⚔️ 1v1: **{self.mmr_1v1}** {rank_label_g(self.mmr_1v1)}  |  🤝 2v2: **{self.mmr_2v2}** {rank_label_g(self.mmr_2v2)}",
            inline=False,
        )
        if not page_entries:
            embed.description = "No matches found."
        else:
            lines = []
            for h in page_entries:
                result = h.get("result", "?")
                mode   = h.get("mode", "?")
                change = h.get("mmr_change", 0)
                ts_raw = h.get("timestamp", "")
                try:
                    dt     = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                    ts_str = f"<t:{int(dt.timestamp())}:d>"
                except Exception:
                    ts_str = ts_raw[:10] if ts_raw else "?"
                icon      = "✅" if result == "win" else "❌"
                mode_icon = "⚔️" if mode == "1v1" else "🤝"
                sign      = "+" if change >= 0 else ""
                lines.append(f"{icon} {mode_icon} **{mode.upper()}** — `{sign}{change} MMR` — {ts_str}")
            embed.description = "\n".join(lines)
        wins   = sum(1 for h in entries if h.get("result") == "win")
        losses = sum(1 for h in entries if h.get("result") == "loss")
        wr     = f"{round(wins / total * 100)}%" if total > 0 else "N/A"
        embed.set_footer(text=f"Page {self.page + 1}/{self.total_pages()} · {wins}W/{losses}L ({wr}) in {total} shown games")
        return embed

    @discord.ui.button(label="◀ Prev", style=discord.ButtonStyle.grey)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 0: self.page -= 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="Filter: ALL", style=discord.ButtonStyle.blurple)
    async def mode_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        cycle = {"all": "1v1", "1v1": "2v2", "2v2": "all"}
        self.mode_filter = cycle.get(self.mode_filter, "all")
        self.page = 0
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.grey)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page < self.total_pages() - 1: self.page += 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)


# ── on_ready ──────────────────────────────────────────────────────────────────

@bot.event
async def on_ready():
    await init_db()
    await tree.sync()
    update_leaderboards.start()
    weekly_spotlight_task.start()
    print(f"✅ {bot.user} is online! Database: {DB_FILE}")


# ── Slash Commands ────────────────────────────────────────────────────────────

@tree.command(name="create_lobby", description="Create a ranked 1v1 or 2v2 lobby")
@app_commands.describe(mode="1v1 or 2v2")
@app_commands.choices(mode=[
    app_commands.Choice(name="1v1", value="1v1"),
    app_commands.Choice(name="2v2", value="2v2"),
])
async def create_lobby(interaction: discord.Interaction, mode: str):
    uid = str(interaction.user.id)
    if await db_is_banned(uid):
        await interaction.response.send_message("❌ You are banned from using the bot.", ephemeral=True); return
    # Channel enforcement — 1v1 lobbies must be created in the 1v1 queue channel, same for 2v2
    cfg      = load_config()
    queue_ch = cfg.get(f"queue_{mode}_channel")
    if queue_ch and str(interaction.channel.id) != queue_ch:
        ch = interaction.guild.get_channel(int(queue_ch))
        ch_mention = ch.mention if ch else f"the ranked-{mode} channel"
        await interaction.response.send_message(
            f"❌ **{mode}** lobbies can only be created in {ch_mention}.", ephemeral=True
        ); return
    # 10-second spam cooldown
    now       = datetime.now(timezone.utc).timestamp()
    last      = lobby_creation_cooldowns.get(uid, 0)
    remaining = 10 - (now - last)
    if remaining > 0:
        await interaction.response.send_message(
            f"⏳ Please wait **{remaining:.1f}s** before creating another lobby.", ephemeral=True
        ); return
    lobby_creation_cooldowns[uid] = now
    p = await db_get_player(uid)
    if p and p.get("active_lobby"):
        existing = await db_get_lobby(p["active_lobby"])
        ch_id    = existing.get("private_channel") if existing else None
        if ch_id:
            ch = interaction.guild.get_channel(int(ch_id))
            if ch:
                await interaction.response.send_message(
                    f"❌ You're already in an active lobby: {ch.mention}\nFinish or forfeit that match first.",
                    ephemeral=True,
                ); return
        await interaction.response.send_message(
            "❌ You're stuck in a lobby that no longer exists.\nUse `/leavelobby` to unstick yourself, then try again.",
            ephemeral=True,
        ); return
    await db_upsert_player(uid, interaction.user.display_name)
    lobby_num = await db_increment_lobby_counter(mode)
    lobby_id  = f"{uid}_{int(datetime.now(timezone.utc).timestamp())}"
    lobby = {
        "mode": mode, "host": uid, "host_name": interaction.user.display_name,
        "players": [uid], "status": "open", "ready": [],
        "last_activity": datetime.now(timezone.utc).isoformat(),
        "lobby_name": f"{mode} #{lobby_num}",
    }
    await db_save_lobby(lobby_id, lobby)
    await db_update_player(uid, active_lobby=lobby_id)
    players = await db_get_players_by_uids([uid])
    embed   = build_lobby_embed(players, lobby)
    await interaction.response.send_message(embed=embed, view=LobbyView(lobby_id))
    bot.loop.create_task(inactivity_timeout_open(interaction.channel, lobby_id))


@tree.command(name="challenge", description="Challenge someone to a 1v1")
@app_commands.describe(opponent="Who do you want to challenge?")
async def challenge(interaction: discord.Interaction, opponent: discord.Member):
    uid = str(interaction.user.id)
    oid = str(opponent.id)
    if await db_is_banned(uid) or await db_is_banned(oid):
        await interaction.response.send_message("❌ A banned player is involved.", ephemeral=True); return
    if uid == oid:
        await interaction.response.send_message("❌ You can't challenge yourself.", ephemeral=True); return
    # Channel enforcement — challenges are 1v1, must be used in the 1v1 queue channel
    cfg      = load_config()
    queue_ch = cfg.get("queue_1v1_channel")
    if queue_ch and str(interaction.channel.id) != queue_ch:
        ch = interaction.guild.get_channel(int(queue_ch))
        ch_mention = ch.mention if ch else "the ranked-1v1 channel"
        await interaction.response.send_message(
            f"❌ Challenges can only be sent in {ch_mention}.", ephemeral=True
        ); return
    p1 = await db_upsert_player(uid, interaction.user.display_name)
    p2 = await db_upsert_player(oid, opponent.display_name)
    embed = discord.Embed(
        title="⚔️ 1v1 Challenge",
        description=f"**{interaction.user.display_name}** challenges {opponent.mention}!\nExpires in 5 minutes.",
        color=discord.Color.red(),
    )
    embed.add_field(name=interaction.user.display_name, value=f"{rank_label_g(p1['mmr_1v1'])} | {p1['mmr_1v1']} MMR", inline=True)
    embed.add_field(name=opponent.display_name,         value=f"{rank_label_g(p2['mmr_1v1'])} | {p2['mmr_1v1']} MMR", inline=True)
    view = ChallengeView(uid, oid, opponent.mention)
    await interaction.response.send_message(embed=embed, view=view)
    view.message = await interaction.original_response()
    bot.loop.create_task(_challenge_warning(view, opponent.mention))


@tree.command(name="coinflip", description="Flip a coin to decide who creates the Valorant lobby")
async def coinflip(interaction: discord.Interaction):
    import random
    lobby = None
    uid   = str(interaction.user.id)
    p     = await db_get_player(uid)
    if p and p.get("active_lobby"):
        lobby = await db_get_lobby(p["active_lobby"])

    if lobby:
        mode    = lobby["mode"]
        players = await db_get_players_by_uids(lobby["players"])
        if mode == "1v1":
            # Pick one of the two players
            winner_uid  = random.choice(lobby["players"])
            winner_name = players.get(winner_uid, {}).get("username", "Someone")
            embed = discord.Embed(
                title="🪙 Coin Flip!",
                description=f"**{winner_name}** creates the Valorant lobby!",
                color=discord.Color.gold(),
            )
        else:
            # 2v2 — pick which team hosts
            t1      = lobby.get("team1", lobby["players"][:2])
            t2      = lobby.get("team2", lobby["players"][2:])
            winning_team = random.choice([t1, t2])
            team_num = "🔴 Team 1" if winning_team == t1 else "🔵 Team 2"
            names    = " & ".join(players.get(u, {}).get("username", "?") for u in winning_team)
            embed = discord.Embed(
                title="🪙 Coin Flip!",
                description=f"**{team_num}** creates the Valorant lobby!\n({names})",
                color=discord.Color.gold(),
            )
        await interaction.response.send_message(embed=embed)
    else:
        # Not in a lobby — generic flip between caller and someone else, or just heads/tails
        result = random.choice(["Heads 🪙", "Tails 🪙"])
        embed  = discord.Embed(
            title="🪙 Coin Flip!",
            description=f"**{result}!**\n*(Use this inside your match channel for a player/team pick)*",
            color=discord.Color.gold(),
        )
        await interaction.response.send_message(embed=embed)


@app_commands.describe(member="Leave blank for your own profile")
async def profile(interaction: discord.Interaction, member: discord.Member = None):
    target = member or interaction.user
    uid    = str(target.id)
    p      = await db_upsert_player(uid, target.display_name)
    w1, l1, mmr1 = p["wins_1v1"], p["losses_1v1"], p["mmr_1v1"]
    w2, l2, mmr2 = p["wins_2v2"], p["losses_2v2"], p["mmr_2v2"]
    wr1 = f"{round(w1 / (w1 + l1) * 100)}%" if (w1 + l1) > 0 else "N/A"
    wr2 = f"{round(w2 / (w2 + l2) * 100)}%" if (w2 + l2) > 0 else "N/A"
    embed = discord.Embed(title=f"{target.display_name}", color=discord.Color.blue())
    embed.add_field(name="1v1",
        value=f"**{rank_label_g(mmr1)}** | {mmr1} MMR\n{w1}W/{l1}L | {wr1}\n🔥 Streak: {p['streak_1v1']} (Best: {p['best_streak_1v1']})",
        inline=True)
    embed.add_field(name="2v2",
        value=f"**{rank_label_g(mmr2)}** | {mmr2} MMR\n{w2}W/{l2}L | {wr2}\n🔥 Streak: {p['streak_2v2']} (Best: {p['best_streak_2v2']})",
        inline=True)
    history = await db_get_history(uid)
    recent  = [h for h in history if h.get("result") in ("win", "loss")][-5:]
    if recent:
        lines = ["✅" if h["result"] == "win" else "❌" for h in reversed(recent)]
        embed.add_field(name="Last 5", value=" ".join(lines), inline=False)
    await interaction.response.send_message(embed=embed)


@tree.command(name="h2h", description="Head to head 1v1 record between two players")
async def h2h(interaction: discord.Interaction, player1: discord.Member, player2: discord.Member):
    uid1, uid2 = str(player1.id), str(player2.id)
    p1 = await db_upsert_player(uid1, player1.display_name)
    p2 = await db_upsert_player(uid2, player2.display_name)
    record = await db_get_h2h(uid1, uid2)
    total  = record["wins"] + record["losses"]
    embed  = discord.Embed(
        title=f"⚔️ {player1.display_name} vs {player2.display_name}",
        color=discord.Color.orange(),
    )
    if total == 0:
        embed.description = "These two have never faced each other."
    else:
        bar = "🟥" * round((record["wins"] / total) * 10) + "🟦" * (10 - round((record["wins"] / total) * 10))
        embed.add_field(name=player1.display_name,
            value=f"**{record['wins']}W/{record['losses']}L**\n{rank_label_g(p1['mmr_1v1'])} | {p1['mmr_1v1']} MMR", inline=True)
        embed.add_field(name=player2.display_name,
            value=f"**{record['losses']}W/{record['wins']}L**\n{rank_label_g(p2['mmr_1v1'])} | {p2['mmr_1v1']} MMR", inline=True)
        embed.add_field(name="Win Share", value=bar, inline=False)
        embed.set_footer(text=f"Total matches: {total}")
    await interaction.response.send_message(embed=embed)


@tree.command(name="leaderboard", description="View the leaderboard")
@app_commands.choices(mode=[
    app_commands.Choice(name="1v1", value="1v1"),
    app_commands.Choice(name="2v2", value="2v2"),
])
async def leaderboard(interaction: discord.Interaction, mode: str):
    players = await db_get_all_players()
    season  = await db_get_season()
    await interaction.response.send_message(embed=build_leaderboard_embed(players, season, mode))


@tree.command(name="history", description="Browse a player's match history")
@app_commands.describe(member="Leave blank for your own history", mode="Filter by mode (default: all)")
@app_commands.choices(mode=[
    app_commands.Choice(name="All", value="all"),
    app_commands.Choice(name="1v1", value="1v1"),
    app_commands.Choice(name="2v2", value="2v2"),
])
async def history_cmd(interaction: discord.Interaction, member: discord.Member = None, mode: str = "all"):
    target  = member or interaction.user
    uid     = str(target.id)
    p       = await db_upsert_player(uid, target.display_name)
    history = await db_get_history(uid)
    raw     = [h for h in history if h.get("result") in ("win", "loss")]
    if not raw:
        await interaction.response.send_message(
            f"**{target.display_name}** hasn't played any matches yet.", ephemeral=True
        ); return
    view  = HistoryView(uid, target.display_name, raw, p["mmr_1v1"], p["mmr_2v2"], mode_filter=mode)
    await interaction.response.send_message(embed=view.build_embed(), view=view)


@tree.command(name="server_stats", description="View server statistics")
async def server_stats(interaction: discord.Interaction):
    players       = await db_get_all_players()
    total_matches = int(await db_get_meta("total_matches", "0"))
    season        = await db_get_season()
    embed = discord.Embed(
        title="Server Statistics", color=discord.Color.teal(), timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="Total Matches", value=str(total_matches), inline=True)
    embed.add_field(name="Total Players", value=str(len(players)),  inline=True)
    embed.add_field(name="Season",        value=str(season),        inline=True)
    if players:
        ma = max(players.items(),
            key=lambda x: x[1].get("wins_1v1",0)+x[1].get("wins_2v2",0)+x[1].get("losses_1v1",0)+x[1].get("losses_2v2",0))
        h1 = max(players.items(), key=lambda x: x[1].get("mmr_1v1", 0))
        h2 = max(players.items(), key=lambda x: x[1].get("mmr_2v2", 0))
        games = ma[1].get("wins_1v1",0)+ma[1].get("wins_2v2",0)+ma[1].get("losses_1v1",0)+ma[1].get("losses_2v2",0)
        embed.add_field(name="Most Active", value=f"**{ma[1]['username']}** — {games} games", inline=False)
        embed.add_field(name="Top 1v1 MMR", value=f"**{h1[1]['username']}** — {h1[1]['mmr_1v1']}", inline=True)
        embed.add_field(name="Top 2v2 MMR", value=f"**{h2[1]['username']}** — {h2[1]['mmr_2v2']}", inline=True)
    await interaction.response.send_message(embed=embed)


@tree.command(name="rank", description="See a player's exact position on the leaderboard")
@app_commands.describe(member="Leave blank for yourself", mode="1v1 or 2v2 (default: 1v1)")
@app_commands.choices(mode=[
    app_commands.Choice(name="1v1", value="1v1"),
    app_commands.Choice(name="2v2", value="2v2"),
])
async def rank_cmd(interaction: discord.Interaction, member: discord.Member = None, mode: str = "1v1"):
    target  = member or interaction.user
    uid     = str(target.id)
    p       = await db_upsert_player(uid, target.display_name)
    players = await db_get_all_players()
    mmr_key = f"mmr_{mode}"
    wk, lk  = f"wins_{mode}", f"losses_{mode}"
    # Only rank players who have played at least one game
    ranked  = sorted(
        [(u, d) for u, d in players.items() if d.get(wk, 0) + d.get(lk, 0) > 0],
        key=lambda x: x[1][mmr_key], reverse=True
    )
    total   = len(ranked)
    pos     = next((i + 1 for i, (u, _) in enumerate(ranked) if u == uid), None)
    mmr     = p[mmr_key]
    w, l    = p.get(wk, 0), p.get(lk, 0)
    icon    = "⚔️" if mode == "1v1" else "🤝"
    embed   = discord.Embed(
        title=f"{icon} {mode.upper()} Rank — {target.display_name}",
        color=discord.Color.gold(),
    )
    if pos is None:
        embed.description = f"**{target.display_name}** hasn't played any {mode} matches yet and is unranked."
    else:
        # Percentile (top X%)
        percentile = round((1 - (pos - 1) / total) * 100) if total > 1 else 100
        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        pos_str = medals.get(pos, f"#{pos}")
        embed.description = (
            f"**{pos_str}** out of **{total}** ranked players\n"
            f"{rank_label_g(mmr)} | **{mmr} MMR**\n"
            f"{w}W / {l}L | Top **{percentile}%**"
        )
        # Show players immediately above and below for context
        if total > 1:
            neighbors = []
            if pos > 1:
                above_uid, above_p = ranked[pos - 2]
                neighbors.append(f"▲ **{above_p['username']}** — {above_p[mmr_key]} MMR (#{pos - 1})")
            neighbors.append(f"➤ **{target.display_name}** — {mmr} MMR (**#{pos}**)")
            if pos < total:
                below_uid, below_p = ranked[pos]
                neighbors.append(f"▼ **{below_p['username']}** — {below_p[mmr_key]} MMR (#{pos + 1})")
            embed.add_field(name="Standing", value="\n".join(neighbors), inline=False)
    await interaction.response.send_message(embed=embed)


@tree.command(name="rival", description="See who you've played against the most and your record vs them")
@app_commands.describe(member="Leave blank for yourself")
async def rival_cmd(interaction: discord.Interaction, member: discord.Member = None):
    target = member or interaction.user
    uid    = str(target.id)
    await db_upsert_player(uid, target.display_name)
    rival_uid, record = await db_get_rival(uid)
    embed = discord.Embed(title=f"Rival — {target.display_name}", color=discord.Color.red())
    if rival_uid is None:
        embed.description = f"**{target.display_name}** hasn't played any 1v1 matches yet."
    else:
        rival_p = await db_get_player(rival_uid)
        rival_name = rival_p["username"] if rival_p else rival_uid
        w, l   = record["wins"], record["losses"]
        total  = w + l
        result = "winning" if w > l else ("losing" if l > w else "even")
        bar    = "🟥" * round((w / total) * 10) + "🟦" * (10 - round((w / total) * 10))
        embed.description = (
            f"**{target.display_name}**'s most frequent opponent is **{rival_name}** "
            f"with **{total} match{'es' if total != 1 else ''}** played.\n"
            f"Currently **{result}** the series."
        )
        embed.add_field(name=target.display_name,  value=f"**{w}W / {l}L**", inline=True)
        embed.add_field(name=rival_name,            value=f"**{l}W / {w}L**", inline=True)
        embed.add_field(name="Head to Head",        value=bar, inline=False)
        if rival_p:
            embed.set_footer(text=f"{rival_name} is currently {rival_p['mmr_1v1']} MMR ({rank_label_g(rival_p['mmr_1v1'])})")
    await interaction.response.send_message(embed=embed)


@tree.command(name="compare", description="Side-by-side stat comparison of two players")
async def compare_cmd(interaction: discord.Interaction, player1: discord.Member, player2: discord.Member):
    uid1, uid2 = str(player1.id), str(player2.id)
    p1 = await db_upsert_player(uid1, player1.display_name)
    p2 = await db_upsert_player(uid2, player2.display_name)
    h2h_record = await db_get_h2h(uid1, uid2)
    def fmt(p, mode):
        w, l, mmr = p[f"wins_{mode}"], p[f"losses_{mode}"], p[f"mmr_{mode}"]
        wr = f"{round(w/(w+l)*100)}%" if (w+l) > 0 else "N/A"
        return f"{rank_label_g(mmr)}\n**{mmr} MMR** | {w}W/{l}L | {wr}\n🔥 Best streak: {p[f'best_streak_{mode}']}"
    embed = discord.Embed(
        title=f"⚔️ {player1.display_name}  vs  {player2.display_name}",
        color=discord.Color.orange(),
    )
    embed.add_field(name=f"1v1 — {player1.display_name}", value=fmt(p1, "1v1"), inline=True)
    embed.add_field(name=f"1v1 — {player2.display_name}", value=fmt(p2, "1v1"), inline=True)
    embed.add_field(name="\u200b", value="\u200b", inline=False)  # spacer
    embed.add_field(name=f"2v2 — {player1.display_name}", value=fmt(p1, "2v2"), inline=True)
    embed.add_field(name=f"2v2 — {player2.display_name}", value=fmt(p2, "2v2"), inline=True)
    # H2H if they've played
    h_total = h2h_record["wins"] + h2h_record["losses"]
    if h_total > 0:
        bar = "🟥" * round((h2h_record["wins"] / h_total) * 10) + "🟦" * (10 - round((h2h_record["wins"] / h_total) * 10))
        embed.add_field(
            name="🗂️ Head-to-Head (1v1)",
            value=f"{player1.display_name} **{h2h_record['wins']}** — **{h2h_record['losses']}** {player2.display_name}\n{bar}",
            inline=False,
        )
    await interaction.response.send_message(embed=embed)


@tree.command(name="active_lobbies", description="[Staff] View all currently open and in-progress lobbies")
async def active_lobbies_cmd(interaction: discord.Interaction):
    if not _is_staff(interaction):
        await interaction.response.send_message("❌ Staff only.", ephemeral=True); return
    all_lobbies = await db_get_all_lobbies()
    active = {lid: l for lid, l in all_lobbies.items() if l.get("status") not in ("complete",)}
    embed  = discord.Embed(
        title="Active Lobbies",
        color=discord.Color.teal(),
        timestamp=datetime.now(timezone.utc),
    )
    if not active:
        embed.description = "No active lobbies right now."
    else:
        now = datetime.now(timezone.utc)
        for lid, l in sorted(active.items(), key=lambda x: x[1].get("last_activity", ""), reverse=True):
            mode     = l.get("mode", "?")
            status   = l.get("status", "?")
            name     = l.get("lobby_name", lid[-8:])
            players  = l.get("players", [])
            # Calculate how long it's been open
            try:
                last_act = datetime.fromisoformat(l["last_activity"].replace("Z", "+00:00"))
                if last_act.tzinfo is None:
                    last_act = last_act.replace(tzinfo=timezone.utc)
                elapsed  = int((now - last_act).total_seconds() // 60)
                age_str  = f"{elapsed}m ago"
            except Exception:
                age_str = "unknown"
            status_icon = {"open": "🟡", "full": "🟠", "teams_set": "🟠", "launched": "🟢"}.get(status, "⚪")
            player_count = len(players)
            max_p = 2 if mode == "1v1" else 4
            ch_id = l.get("private_channel")
            ch_str = f" → <#{ch_id}>" if ch_id else ""
            embed.add_field(
                name=f"{status_icon} {name} ({mode}){ch_str}",
                value=f"Status: **{status}** | Players: {player_count}/{max_p} | Last activity: {age_str}",
                inline=False,
            )
        embed.set_footer(text=f"{len(active)} active {'lobby' if len(active)==1 else 'lobbies'}")
    await interaction.response.send_message(embed=embed, ephemeral=True)


class AuditView(discord.ui.View):
    """Paginated staff audit log for a player's MMR history."""
    PAGE_SIZE = 10

    def __init__(self, username: str, history: list):
        super().__init__(timeout=120)
        self.username = username
        self.history  = list(reversed(history))  # newest first
        self.page     = 0
        self._update_buttons()

    def total_pages(self):
        return max(1, -(-len(self.history) // self.PAGE_SIZE))

    def _update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page >= self.total_pages() - 1

    def build_embed(self) -> discord.Embed:
        start   = self.page * self.PAGE_SIZE
        entries = self.history[start: start + self.PAGE_SIZE]
        embed   = discord.Embed(
            title=f"Audit Log — {self.username}",
            color=discord.Color.greyple(),
            timestamp=datetime.now(timezone.utc),
        )
        lines = []
        for h in entries:
            result = h.get("result", "?")
            mode   = h.get("mode", "?")
            change = h.get("mmr_change", 0)
            ts_raw = h.get("timestamp", "")
            try:
                dt     = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                ts_str = f"<t:{int(dt.timestamp())}:f>"  # full date+time
            except Exception:
                ts_str = ts_raw[:19] if ts_raw else "?"
            icon = "✅" if result == "win" else "❌"
            sign = "+" if change >= 0 else ""
            lines.append(f"{icon} `{sign}{change:+d} MMR` · **{mode.upper()}** · {ts_str}")
        embed.description = "\n".join(lines) if lines else "No records."
        embed.set_footer(text=f"Page {self.page+1}/{self.total_pages()} · {len(self.history)} total entries")
        return embed

    @discord.ui.button(label="◀ Prev", style=discord.ButtonStyle.grey)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 0: self.page -= 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.grey)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page < self.total_pages() - 1: self.page += 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)


@tree.command(name="audit", description="[Staff] Full chronological MMR audit log for a player")
@app_commands.describe(member="Player to audit")
async def audit_cmd(interaction: discord.Interaction, member: discord.Member):
    if not _is_staff(interaction):
        await interaction.response.send_message("❌ Staff only.", ephemeral=True); return
    uid     = str(member.id)
    p       = await db_get_player(uid)
    history = await db_get_history(uid)
    raw     = [h for h in history if h.get("result") in ("win", "loss")]
    if not raw:
        await interaction.response.send_message(
            f"**{member.display_name}** has no match history.", ephemeral=True
        ); return
    username = p["username"] if p else member.display_name
    view     = AuditView(username, raw)
    await interaction.response.send_message(embed=view.build_embed(), view=view, ephemeral=True)


# ── Admin Commands ────────────────────────────────────────────────────────────

@tree.command(name="setup_leaderboard", description="[Admin] Post a permanent auto-updating leaderboard")
@app_commands.choices(mode=[
    app_commands.Choice(name="1v1", value="1v1"),
    app_commands.Choice(name="2v2", value="2v2"),
])
async def setup_leaderboard(interaction: discord.Interaction, mode: str):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    players = await db_get_all_players()
    season  = await db_get_season()
    cfg     = load_config()
    await interaction.response.send_message("✅ Leaderboard posted!", ephemeral=True)
    msg = await interaction.channel.send(embed=build_leaderboard_embed(players, season, mode))
    cfg[f"leaderboard_{mode}_channel"] = str(interaction.channel.id)
    cfg[f"leaderboard_{mode}_message"] = str(msg.id)
    save_config(cfg)


@tree.command(name="setup_match_log", description="[Admin] Set this channel as the match log")
async def setup_match_log(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    cfg = load_config(); cfg["match_log_channel"] = str(interaction.channel.id); save_config(cfg)
    await interaction.response.send_message(f"✅ Match log set to {interaction.channel.mention}", ephemeral=True)


@tree.command(name="setup_rankup", description="[Admin] Set channel for rank-up announcements")
async def setup_rankup(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    cfg = load_config(); cfg["rankup_channel"] = str(interaction.channel.id); save_config(cfg)
    await interaction.response.send_message(f"✅ Rank-up announcements set to {interaction.channel.mention}", ephemeral=True)



@tree.command(name="setup_queue_channels", description="[Admin] Set which channels are used for 1v1 and 2v2 queue")
@app_commands.describe(
    channel_1v1="Channel where /create_lobby 1v1 and /challenge are allowed",
    channel_2v2="Channel where /create_lobby 2v2 is allowed",
)
async def setup_queue_channels(
    interaction: discord.Interaction,
    channel_1v1: discord.TextChannel = None,
    channel_2v2: discord.TextChannel = None,
):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    if not channel_1v1 and not channel_2v2:
        await interaction.response.send_message("❌ Provide at least one channel.", ephemeral=True); return
    cfg   = load_config()
    lines_out = []
    if channel_1v1:
        cfg["queue_1v1_channel"] = str(channel_1v1.id)
        lines_out.append(f"⚔️ 1v1 queue → {channel_1v1.mention}")
    if channel_2v2:
        cfg["queue_2v2_channel"] = str(channel_2v2.id)
        lines_out.append(f"🤝 2v2 queue → {channel_2v2.mention}")
    save_config(cfg)
    await interaction.response.send_message("✅ Queue channels set:\n" + "\n".join(lines_out), ephemeral=True)


@tree.command(name="setup_ranked_verify", description="[Admin] Post the how-to-queue embed with reaction gate in this channel")
async def setup_ranked_verify(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    cfg  = load_config()
    guild = interaction.guild

    # Get or create the Ranked Verified role
    role = discord.utils.get(guild.roles, name="Ranked Verified")
    if not role:
        role = await guild.create_role(
            name="Ranked Verified",
            color=discord.Color.from_rgb(255, 85, 85),
            reason="Created by /setup_ranked_verify",
        )
    cfg["ranked_verified_role"] = str(role.id)

    embed = discord.Embed(
        title="How to Play Ranked",
        color=discord.Color.red(),
    )
    embed.add_field(
        name="1️⃣  Get Verified",
        value="React with ✅ below to unlock the ranked queue channels.",
        inline=False,
    )
    embed.add_field(
        name="2️⃣  Join a Queue",
        value="Go to the queue channel and use `/create_lobby` to open a lobby, or `/challenge @player` to directly challenge someone.",
        inline=False,
    )
    embed.add_field(
        name="3️⃣  Ready Up",
        value="Once the lobby fills, click **✅ Ready**. A private match channel will be created for you.",
        inline=False,
    )
    embed.add_field(
        name="4️⃣  Play & Report",
        value="Share your Valorant custom lobby code in the private channel. After the match, click **🏆 I Won** or **💀 I Lost** — the other player confirms and MMR updates instantly.",
        inline=False,
    )
    embed.add_field(
        name="Can't decide who makes the lobby?",
        value="Use `/coinflip` in your match channel — it'll randomly pick a player (1v1) or team (2v2).",
        inline=False,
    )
    embed.add_field(
        name="Dispute?",
        value="Post a screenshot as evidence in your match channel, then click **❌ Dispute** to flag it for staff.",
        inline=False,
    )
    embed.set_footer(text="React ✅ below to get access • Remove reaction to lose access")

    msg = await interaction.channel.send(embed=embed)
    await msg.add_reaction("✅")

    cfg["ranked_verify_channel"] = str(interaction.channel.id)
    cfg["ranked_verify_message"] = str(msg.id)
    save_config(cfg)

    await interaction.followup.send(
        f"✅ Ranked verify embed posted! **Ranked Verified** role created/found: {role.mention}\n"
        f"Make sure to lock your queue channels so only that role can use them.",
        ephemeral=True,
    )


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.member and payload.member.bot: return
    if str(payload.emoji) != "✅": return
    cfg = load_config()
    if str(payload.message_id) != cfg.get("ranked_verify_message"): return
    guild  = bot.get_guild(payload.guild_id)
    if not guild: return
    member = payload.member or guild.get_member(payload.user_id)
    if not member or member.bot: return
    role_id = cfg.get("ranked_verified_role")
    if not role_id: return
    role = guild.get_role(int(role_id))
    if role and role not in member.roles:
        try: await member.add_roles(role, reason="Ranked verify reaction")
        except Exception: pass


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if str(payload.emoji) != "✅": return
    cfg = load_config()
    if str(payload.message_id) != cfg.get("ranked_verify_message"): return
    guild  = bot.get_guild(payload.guild_id)
    if not guild: return
    member = guild.get_member(payload.user_id)
    if not member or member.bot: return
    role_id = cfg.get("ranked_verified_role")
    if not role_id: return
    role = guild.get_role(int(role_id))
    if role and role in member.roles:
        try: await member.remove_roles(role, reason="Ranked verify reaction removed")
        except Exception: pass


async def setup_spotlight(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    cfg = load_config(); cfg["spotlight_channel"] = str(interaction.channel.id); save_config(cfg)
    await interaction.response.send_message(f"✅ Spotlight set to {interaction.channel.mention}", ephemeral=True)


@tree.command(name="post_spotlight", description="[Admin] Post the player spotlight now")
async def post_spotlight(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    cfg   = load_config()
    ch_id = cfg.get("spotlight_channel")
    if not ch_id:
        await interaction.response.send_message("❌ Run /setup_spotlight first.", ephemeral=True); return
    channel = interaction.guild.get_channel(int(ch_id))
    if not channel:
        await interaction.response.send_message("❌ Channel not found.", ephemeral=True); return
    players = await db_get_all_players()
    season  = await db_get_season()
    await channel.send(embed=build_spotlight_embed(players, season))
    await interaction.response.send_message("✅ Spotlight posted!", ephemeral=True)


@tree.command(name="set_mmr", description="[Admin] Manually set a player's MMR")
@app_commands.choices(mode=[
    app_commands.Choice(name="1v1", value="1v1"),
    app_commands.Choice(name="2v2", value="2v2"),
])
async def set_mmr(interaction: discord.Interaction, member: discord.Member, mode: str, mmr: int):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    if not 100 <= mmr <= 9999:
        await interaction.response.send_message("❌ MMR must be 100–9999.", ephemeral=True); return
    uid = str(member.id)
    p   = await db_upsert_player(uid, member.display_name)
    old = p[f"mmr_{mode}"]
    await db_update_player(uid, **{f"mmr_{mode}": mmr})
    await interaction.response.send_message(
        f"✅ **{member.display_name}** {mode}: **{old}** → **{mmr}** {rank_label_g(mmr)}"
    )


@tree.command(name="mmr_reset", description="[Admin] Reset a player's stats")
@app_commands.choices(mode=[
    app_commands.Choice(name="1v1",  value="1v1"),
    app_commands.Choice(name="2v2",  value="2v2"),
    app_commands.Choice(name="both", value="both"),
])
async def mmr_reset(interaction: discord.Interaction, member: discord.Member, mode: str):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    uid    = str(member.id)
    await db_upsert_player(uid, member.display_name)
    modes  = ["1v1", "2v2"] if mode == "both" else [mode]
    fields = {}
    for m in modes:
        fields.update({
            f"mmr_{m}":          STARTING_MMR,
            f"wins_{m}":         0,
            f"losses_{m}":       0,
            f"streak_{m}":       0,
            f"best_streak_{m}":  0,
            f"loss_streak_{m}":  0,
        })
    await db_update_player(uid, **fields)
    await interaction.response.send_message(f"✅ Reset **{member.display_name}** {mode} stats.", ephemeral=True)


@tree.command(name="bot_ban", description="[Admin] Ban or unban a player from using the bot")
@app_commands.describe(member="Player", unban="Set True to unban")
async def bot_ban(interaction: discord.Interaction, member: discord.Member, unban: bool = False):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    uid = str(member.id)
    if unban:
        await db_unban(uid)
        await interaction.response.send_message(f"✅ **{member.display_name}** unbanned.")
    else:
        await db_ban(uid)
        await interaction.response.send_message(f"✅ **{member.display_name}** banned from the bot.")


@tree.command(name="end_season", description="[Admin] End season, archive results, reset all MMR")
async def end_season(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    season  = await db_get_season()
    players = await db_get_all_players()
    archive = {}
    for mode in ("1v1", "2v2"):
        top = sorted(players.items(), key=lambda x: x[1][f"mmr_{mode}"], reverse=True)[:10]
        archive[mode] = [
            {"username": p.get("username"), "mmr": p[f"mmr_{mode}"],
             "wins": p[f"wins_{mode}"], "losses": p[f"losses_{mode}"]}
            for _, p in top
        ]
    await db.execute("INSERT INTO season_archive (season, timestamp, data) VALUES (?,?,?)",
        (season, datetime.now(timezone.utc).isoformat(), json.dumps(archive)))
    # Reset all players
    await db.execute("""
        UPDATE players SET
            mmr_1v1=1000, mmr_2v2=1000,
            wins_1v1=0, losses_1v1=0, wins_2v2=0, losses_2v2=0,
            streak_1v1=0, streak_2v2=0, best_streak_1v1=0, best_streak_2v2=0,
            loss_streak_1v1=0, loss_streak_2v2=0, active_lobby=NULL
    """)
    await db_clear_all_history()
    await db_clear_all_h2h()
    await db_set_meta("season", season + 1)
    await db.commit()
    medals = ["🥇", "🥈", "🥉"]
    embed  = discord.Embed(title=f"🏁 Season {season} Ended!", color=discord.Color.gold())
    for mode in ("1v1", "2v2"):
        top3  = archive[mode][:3]
        lines = [f"{medals[i]} {p['username']} — {p['mmr']} MMR" for i, p in enumerate(top3)]
        embed.add_field(
            name=f"{'⚔️' if mode == '1v1' else '🤝'} {mode.upper()} Top 3",
            value="\n".join(lines) or "No data",
            inline=True,
        )
    await interaction.response.send_message(embed=embed)


class WipeConfirmView(discord.ui.View):
    def __init__(self, admin_id: str):
        super().__init__(timeout=30)
        self.admin_id = admin_id

    @discord.ui.button(label="✅ Yes, wipe everything", style=discord.ButtonStyle.red)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.admin_id:
            await interaction.response.send_message("❌ Only the admin who ran the command can confirm.", ephemeral=True); return
        self.stop()
        await interaction.response.defer()
        # Nuke every table
        await db.execute("DELETE FROM players")
        await db.execute("DELETE FROM history")
        await db.execute("DELETE FROM h2h")
        await db.execute("DELETE FROM lobbies")
        await db.execute("DELETE FROM pending")
        await db.execute("DELETE FROM banned")
        await db.execute("DELETE FROM season_archive")
        await db.execute("UPDATE meta SET value='1' WHERE key='season'")
        await db.execute("UPDATE meta SET value='0' WHERE key='total_matches'")
        await db.execute("UPDATE meta SET value='0' WHERE key='lobby_counter_1v1'")
        await db.execute("UPDATE meta SET value='0' WHERE key='lobby_counter_2v2'")
        await db.commit()
        embed = discord.Embed(
            title="🗑️ Full Wipe Complete",
            description="All players, MMR, history, lobbies, bans, and season data have been deleted.\nCounters reset to 0. Ready for fresh testing.",
            color=discord.Color.red(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text=f"Wiped by {interaction.user.display_name}")
        await interaction.edit_original_response(content=None, embed=embed, view=None)

    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.grey)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != self.admin_id:
            await interaction.response.send_message("❌ Not your command.", ephemeral=True); return
        self.stop()
        await interaction.response.edit_message(content="Wipe cancelled.", embed=None, view=None)

    async def on_timeout(self):
        try:
            await self.message.edit(content="⏱️ Wipe cancelled — confirmation timed out.", embed=None, view=None)
        except Exception:
            pass


@tree.command(name="wipe_all", description="[Admin] ⚠️ Permanently delete all players, MMR, history, and lobbies")
async def wipe_all(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    players = await db_get_all_players()
    embed = discord.Embed(
        title="⚠️ Full Database Wipe",
        description=(
            f"This will **permanently delete**:\n"
            f"• **{len(players)}** player profiles\n"
            f"• All MMR, wins, losses, streaks, and history\n"
            f"• All active lobbies and pending reports\n"
            f"• All ban records and season archives\n"
            f"• All lobby counters reset to 0\n\n"
            f"**This cannot be undone.** Confirm below."
        ),
        color=discord.Color.red(),
    )
    view = WipeConfirmView(str(interaction.user.id))
    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    view.message = await interaction.original_response()


@tree.command(name="sync_rank_roles", description="[Admin] Assign rank roles to all existing players for both 1v1 and 2v2")
async def sync_rank_roles(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)
    players = await db_get_all_players()
    guild   = interaction.guild
    updated = 0
    skipped = 0
    for uid, p in players.items():
        member = guild.get_member(int(uid))
        if not member:
            skipped += 1
            continue
        await assign_rank_role(guild, member, p["mmr_1v1"], "1v1")
        await assign_rank_role(guild, member, p["mmr_2v2"], "2v2")
        updated += 1
    await interaction.followup.send(
        f"✅ Synced 1v1 and 2v2 rank roles for **{updated}** players. ({skipped} not found in server)",
        ephemeral=True,
    )



async def leavelobby(interaction: discord.Interaction):
    uid = str(interaction.user.id)
    p   = await db_get_player(uid)
    if not p or not p.get("active_lobby"):
        await interaction.response.send_message("You are not stuck in any lobby.", ephemeral=True); return
    lobby_id = p["active_lobby"]
    lobby    = await db_get_lobby(lobby_id)
    if lobby and lobby.get("private_channel"):
        ch = interaction.guild.get_channel(int(lobby["private_channel"]))
        if ch:
            await interaction.response.send_message(
                f"Your lobby still exists: {ch.mention}. Ask staff to force close it.", ephemeral=True
            ); return
    await db_update_player(uid, active_lobby=None)
    if lobby:
        lobby["status"] = "complete"
        if uid in lobby.get("players", []): lobby["players"].remove(uid)
        await db_save_lobby(lobby_id, lobby)
    await interaction.response.send_message("✅ Unstuck! You can now create or join a lobby.", ephemeral=True)


@tree.command(name="clearstuck", description="[Staff] Unstick any player stuck in a deleted lobby")
@app_commands.describe(member="Player to unstick")
async def clearstuck(interaction: discord.Interaction, member: discord.Member):
    if not _is_staff(interaction):
        await interaction.response.send_message("❌ Staff only.", ephemeral=True); return
    uid = str(member.id)
    p   = await db_get_player(uid)
    if not p:
        await interaction.response.send_message(f"{member.display_name} has no player data.", ephemeral=True); return
    lid = p.get("active_lobby")
    if lid:
        lobby = await db_get_lobby(lid)
        if lobby:
            lobby["status"] = "complete"
            if uid in lobby.get("players", []): lobby["players"].remove(uid)
            await db_save_lobby(lid, lobby)
    await db_update_player(uid, active_lobby=None)
    await interaction.response.send_message(f"✅ **{member.display_name}** unstuck.", ephemeral=True)


@tree.command(name="setup_commands", description="[Admin] Post the player commands embed in this channel (auto-updates)")
async def setup_commands(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ Admin only.", ephemeral=True); return
    await interaction.response.defer(ephemeral=True)

    embed = discord.Embed(
        title="Skirmish Bot — Player Commands",
        description="All commands available to players.",
        color=discord.Color.red(),
    )
    embed.add_field(
        name="Queuing",
        value=(
            "`/create_lobby [1v1|2v2]` — Open a ranked lobby others can join\n"
            "`/challenge @player` — Directly challenge someone to a 1v1\n"
            "`/coinflip` — Randomly pick who makes the Valorant lobby (works in your match channel)"
        ),
        inline=False,
    )
    embed.add_field(
        name="Stats",
        value=(
            "`/profile [@player]` — View MMR, record, and recent results\n"
            "`/rank [@player]` — See exact leaderboard position and nearby players\n"
            "`/leaderboard [1v1|2v2]` — Top 10 by MMR and win streak\n"
            "`/h2h @player1 @player2` — Head-to-head record between two players\n"
            "`/rival [@player]` — Most-played opponent and series record\n"
            "`/compare @player1 @player2` — Side-by-side stat comparison\n"
            "`/history [@player]` — Paginated match history with mode filter\n"
            "`/server_stats` — Server-wide totals and top players"
        ),
        inline=False,
    )
    embed.add_field(
        name="Utility",
        value=(
            "`/leavelobby` — Unstick yourself if you're stuck in a deleted lobby"
        ),
        inline=False,
    )
    embed.add_field(
        name="Ranks",
        value=(
            "Iron · Bronze · Silver · Gold · Platinum · Diamond · Champion\n"
            "MMR thresholds: <900 · <1200 · <1500 · <1800 · <2100 · <4000 · 4000+"
        ),
        inline=False,
    )
    embed.set_footer(text="Separate 1v1 and 2v2 rank roles are assigned automatically after each match.")

    cfg = load_config()
    ch_id  = cfg.get("commands_channel")
    msg_id = cfg.get("commands_message")

    # Try to edit existing message first
    if ch_id and msg_id:
        try:
            ch  = interaction.guild.get_channel(int(ch_id))
            msg = await ch.fetch_message(int(msg_id))
            await msg.edit(embed=embed)
            await interaction.followup.send("✅ Commands embed updated.", ephemeral=True)
            return
        except Exception:
            pass

    msg = await interaction.channel.send(embed=embed)
    cfg["commands_channel"] = str(interaction.channel.id)
    cfg["commands_message"]  = str(msg.id)
    save_config(cfg)
    await interaction.followup.send("✅ Commands embed posted!", ephemeral=True)


@tree.command(name="help", description="Show all commands")
async def help_cmd(interaction: discord.Interaction):
    embed = discord.Embed(title="Skirmish Ranked Bot — Commands", color=discord.Color.red())
    embed.add_field(name="Lobbies",  value="`/create_lobby` · `/challenge @user` · `/coinflip`", inline=False)
    embed.add_field(name="Match",    value="Use **I Won / I Lost / Forfeit** buttons in your private lobby. Other player confirms — MMR updates instantly.", inline=False)
    embed.add_field(name="Stats",    value="`/profile` · `/h2h` · `/leaderboard` · `/server_stats` · `/history`\n`/rank` · `/rival` · `/compare @p1 @p2`", inline=False)
    embed.add_field(name="Ranks",    value="Iron <900 · Bronze <1200 · Silver <1500 · Gold <1800 · Platinum <2100 · Diamond <4000 · Champion 4000+", inline=False)
    embed.add_field(name="Admin",    value="`/setup_leaderboard` · `/setup_match_log` · `/setup_rankup` · `/setup_queue_channels` · `/setup_ranked_verify` · `/setup_commands`\n`/set_mmr` · `/mmr_reset` · `/bot_ban` · `/end_season` · `/clearstuck` · `/leavelobby`\n`/active_lobbies` · `/audit @player` · `/wipe_all` · `/sync_rank_roles`", inline=False)
    await interaction.response.send_message(embed=embed)


# ── Weekly Spotlight Task ─────────────────────────────────────────────────────

@tasks.loop(hours=168)
async def weekly_spotlight_task():
    cfg   = load_config()
    ch_id = cfg.get("spotlight_channel")
    if not ch_id: return
    players = await db_get_all_players()
    season  = await db_get_season()
    channel = bot.get_channel(int(ch_id))
    if channel:
        try: await channel.send(embed=build_spotlight_embed(players, season))
        except Exception: pass


# ── Run ───────────────────────────────────────────────────────────────────────

if not TOKEN or TOKEN == "YOUR_TOKEN_HERE":
    raise RuntimeError("❌ DISCORD_TOKEN environment variable is not set.")

bot.run(TOKEN)
