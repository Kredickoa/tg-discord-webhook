"""
Telegram → Discord Forwarder Bot (MTProto Userbot)
────────────────────────────────────────
Reads public channels and forwards to Discord as orange embeds.
Includes deduplication to prevent double-posting from two channels.
"""

import io
import json
import logging
import os
import time
import asyncio
import collections
import datetime

import motor.motor_asyncio
import httpx
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.custom import Button
from telethon.sessions import StringSession
from telethon.tl.types import Message, MessageMediaPhoto, MessageMediaDocument, DocumentAttributeVideo

load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────
API_ID = os.environ.get("TELEGRAM_API_ID")
API_HASH = os.environ.get("TELEGRAM_API_HASH")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION_STRING")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
MONGO_URI = os.environ.get("MONGO_URI", "").strip()
GITHUB_REPO = os.environ.get("GITHUB_REPO", "Kredickoa/tg-discord-webhook").strip()
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main").strip()
GITHUB_POLL_INTERVAL_SECONDS = int(os.environ.get("GITHUB_POLL_INTERVAL_SECONDS", "300"))

# Channels to listen to (usernames without @)
CHANNELS = ["ab3army", "ab3brigade"]

# Constants
EMBED_COLOR = 0xFF6600
DISCORD_MAX_BYTES = 100 * 1024 * 1024   # 100 MB Discord boosted server limit

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# LRU Cache for deduplication (keeps last 50 message signatures)
sent_cache: collections.deque = collections.deque(maxlen=50)

if not all([API_ID, API_HASH, SESSION_STRING, DISCORD_WEBHOOK_URL]):
    logger.error("❌ Не всі змінні середовища заповнені (.env)")
    exit(1)


# ─── Embed builders ───────────────────────────────────────────────────────────
def _base_embed(title: str | None = None, description: str | None = None) -> dict:
    embed: dict = {"color": EMBED_COLOR}
    if title:
        embed["title"] = title[:256]
    if description:
        embed["description"] = description[:4096]
    return embed


def _text_embed(text: str, channel_title: str) -> dict:
    embed = _base_embed(description=text[:4096])
    embed["footer"] = {"text": f"📢 {channel_title}"}
    return embed


def _media_embed_image(filename: str) -> dict:
    embed = _base_embed()
    embed["image"] = {"url": f"attachment://{filename}"}
    return embed


def _media_embed_video_ok(filename: str, size_mb: float) -> dict:
    return _base_embed(
        description=f"🎬 Відео `{filename}` ({size_mb:.1f} МБ) — завантажується нижче"
    )


def _media_embed_too_large(label: str, size_mb: float, msg_link: str) -> dict:
    if msg_link:
        desc = f"🔗 **[{label} ({size_mb:.1f} МБ) — Відкрити в Telegram]({msg_link})**"
    else:
        desc = f"📎 **{label} ({size_mb:.1f} МБ)**"
    return _base_embed(description=desc)


def _author_embed_part(channel_title: str) -> dict:
    """Common author block for the first embed."""
    # Note: Using generic 3ОШБ avatar for the embed author block
    return {"name": channel_title, "icon_url": "https://i.imgur.com/xCvzudY.png"}


# ─── Discord sender ───────────────────────────────────────────────────────────
async def _send(
    embeds: list[dict],
    username: str,
    file_data: bytes | None = None,
    filename: str | None = None,
) -> None:
    """POST to Discord webhook. Dynamically sets webhook username."""
    # Webhook identity overrides
    payload = {
        "username": username,
        "embeds": embeds
    }

    async with httpx.AsyncClient(timeout=60) as client:
        if file_data and filename:
            mime = _guess_mime(filename)
            resp = await client.post(
                DISCORD_WEBHOOK_URL,
                files={
                    "payload_json": (None, json.dumps(payload), "application/json"),
                    "files[0]": (filename, file_data, mime),
                },
            )
        else:
            resp = await client.post(DISCORD_WEBHOOK_URL, json=payload)

    if resp.status_code not in (200, 204):
        logger.error("Discord error %s: %s", resp.status_code, resp.text[:400])


def _guess_mime(filename: str) -> str:
    ext = filename.rsplit(".", 1)[-1].lower()
    return {
        "jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
        "mp4": "video/mp4", "mov": "video/quicktime",
        "mp3": "audio/mpeg", "ogg": "audio/ogg", "webp": "image/webp",
    }.get(ext, "application/octet-stream")


def _github_commit_api_url() -> str:
    return f"https://api.github.com/repos/{GITHUB_REPO}/commits"


def _github_headers() -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "User-Agent": "tg-discord-webhook-bot",
    }


# ─── Deduplication ────────────────────────────────────────────────────────────
def _is_duplicate(msg: Message) -> bool:
    """Returns True if this message is a duplicate of a recently sent one."""
    sig = None
    
    # 1. Forwarded post check
    if msg.fwd_from and msg.fwd_from.from_id and msg.fwd_from.channel_post:
        # e.g., "fwd:PeerChannel(channel_id=123):456"
        sig = f"fwd:{msg.fwd_from.from_id}:{msg.fwd_from.channel_post}"
    
    # 2. Exact text + media check
    elif msg.text or msg.media:
        parts = []
        if msg.text:
            parts.append(f"text:{hash(msg.raw_text)}")
        if msg.media:
            parts.append(f"media_type:{type(msg.media).__name__}")
            if isinstance(msg.media, MessageMediaDocument):
                parts.append(f"size:{msg.media.document.size}")
            elif isinstance(msg.media, MessageMediaPhoto):
                parts.append(f"photo:{msg.media.photo.id}")
        sig = "|".join(parts)
        
    if not sig:
        return False

    if sig in sent_cache:
        logger.info(f"⏭️ Пропущено дублікат: {sig}")
        return True
        
    sent_cache.append(sig)
    return False


def _escape_pings(text: str | None) -> str:
    return (text or "").replace("@", "＠")


# ─── Stats & Admin Bot ──────────────────────────────────────────────────────────
bot_client = None
db = None

if MONGO_URI:
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    db = mongo_client["telegram_forwarder"]

async def _add_stat(ch_title: str, msg_type: str):
    if db is not None:
        await db.stats.insert_one({
            "channel": ch_title,
            "type": msg_type,
            "time": datetime.datetime.now()
        })

if BOT_TOKEN:
    bot_client = TelegramClient(StringSession(""), int(API_ID), API_HASH)
    github_last_sha = None
    LAST_GH_CHECK = 0

    async def get_github_setting(user_id: int) -> bool:
        if db is not None:
            doc = await db.settings.find_one({"_id": f"gh_notify_{user_id}"})
            return doc["value"] if doc else False
        return False

    async def set_github_setting(user_id: int, val: bool):
        if db is not None:
            await db.settings.update_one(
                {"_id": f"gh_notify_{user_id}"},
                {"$set": {"value": val}},
                upsert=True
            )

    async def get_stored_sha() -> str | None:
        if db is not None:
            doc = await db.settings.find_one({"_id": "last_github_sha"})
            return doc["value"] if doc else None
        return None

    async def set_stored_sha(sha: str):
        if db is not None:
            await db.settings.update_one(
                {"_id": "last_github_sha"},
                {"$set": {"value": sha}},
                upsert=True
            )

    async def _fetch_github_commits(limit: int = 10) -> list[dict]:
        params = {"sha": GITHUB_BRANCH, "per_page": limit}
        async with httpx.AsyncClient(timeout=15) as c:
            resp = await c.get(_github_commit_api_url(), params=params, headers=_github_headers())
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else []

    async def check_github_commits() -> list[dict]:
        global github_last_sha
        try:
            commits = await _fetch_github_commits()
        except Exception as e:
            logger.error("GitHub check error for %s@%s: %s", GITHUB_REPO, GITHUB_BRANCH, e)
            return []

        if not commits:
            logger.warning("GitHub check returned no commits for %s@%s", GITHUB_REPO, GITHUB_BRANCH)
            return []

        latest_sha = commits[0]["sha"]

        if github_last_sha is None:
            github_last_sha = await get_stored_sha()

        if github_last_sha is None:
            github_last_sha = latest_sha
            await set_stored_sha(latest_sha)
            logger.info("GitHub watcher baseline initialized at %s for %s@%s", latest_sha[:7], GITHUB_REPO, GITHUB_BRANCH)
            return []

        if latest_sha == github_last_sha:
            return []

        new_commits: list[dict] = []
        for commit in commits:
            if commit["sha"] == github_last_sha:
                break
            new_commits.append(commit)
        else:
            logger.warning(
                "Stored SHA %s not found in latest %s commits for %s@%s; notifying only fetched history",
                github_last_sha[:7],
                len(commits),
                GITHUB_REPO,
                GITHUB_BRANCH,
            )

        github_last_sha = latest_sha
        await set_stored_sha(latest_sha)
        return list(reversed(new_commits))

    async def check_github() -> str | None:
        global github_last_sha
        url = "https://api.github.com/repos/Kredickoa/tg-discord-webhook/commits/main"
        async with httpx.AsyncClient() as c:
            try:
                resp = await c.get(url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    sha = data["sha"]
                    msg = data["commit"]["message"]
                    
                    # 1. Sync in-memory with DB on first run
                    if github_last_sha is None:
                        github_last_sha = await get_stored_sha()
                    
                    # 2. Compare
                    if github_last_sha is None:
                        # First time ever or no DB
                        github_last_sha = sha
                        await set_stored_sha(sha)
                        return None
                    
                    if sha != github_last_sha:
                        github_last_sha = sha
                        await set_stored_sha(sha)
                        return msg
            except Exception as e:
                logger.error(f"GitHub check error: {e}")
        return None

    async def github_watcher():
        logger.info("GitHub watcher started for %s@%s.", GITHUB_REPO, GITHUB_BRANCH)
        await asyncio.sleep(5)  # Short wait
        while True:
            try:
                new_commits = await check_github_commits()
                if new_commits and db is not None:
                    logger.info("Detected %s new GitHub commit(s) for %s@%s.", len(new_commits), GITHUB_REPO, GITHUB_BRANCH)
                    subscribers = []
                    async for doc in db.settings.find({"value": True}):
                        if str(doc["_id"]).startswith("gh_notify_"):
                            subscribers.append(int(doc["_id"].replace("gh_notify_", "")))

                    if not subscribers:
                        logger.info("No GitHub alert subscribers enabled in MongoDB.")

                    for commit in new_commits:
                        commit_msg = commit["commit"]["message"]
                        commit_sha = commit["sha"][:7]
                        commit_url = commit.get("html_url", "")
                        notify_text = (
                            f"вљ пёЏ **РЈРІР°РіР°! Р’Р»Р°СЃРЅРёРє РѕР±РЅРѕРІРёРІ РєРѕРґ РЅР° GitHub!**\n"
                            f"Р РµРїРѕР·РёС‚РѕСЂС–Р№: `{GITHUB_REPO}`\n"
                            f"Р“С–Р»РєР°: `{GITHUB_BRANCH}`\n"
                            f"SHA: `{commit_sha}`\n"
                            f"РљРѕРјС–С‚: `{commit_msg}`"
                        )
                        if commit_url:
                            notify_text += f"\n{commit_url}"

                        notify_text = (
                            "GitHub update detected\n"
                            f"Repo: `{GITHUB_REPO}`\n"
                            f"Branch: `{GITHUB_BRANCH}`\n"
                            f"SHA: `{commit_sha}`\n"
                            f"Commit: `{commit_msg}`"
                            + (f"\n{commit_url}" if commit_url else "")
                        )

                        for uid in subscribers:
                            try:
                                await bot_client.send_message(uid, notify_text)
                                logger.info("Notified %s about commit %s.", uid, commit_sha)
                            except Exception as e:
                                logger.error("Failed to notify %s about commit %s: %s", uid, commit_sha, e)
                await asyncio.sleep(GITHUB_POLL_INTERVAL_SECONDS)
                continue
                new_cmt = await check_github()
                if new_cmt:
                    logger.info(f"New GitHub commit detected: {new_cmt}")
                    if db is not None:
                        async for doc in db.settings.find({"value": True}):
                            if str(doc["_id"]).startswith("gh_notify_"):
                                uid = int(doc["_id"].replace("gh_notify_", ""))
                                try:
                                    await bot_client.send_message(
                                        uid,
                                        f"⚠️ **Увага! Власник обновив код на GitHub!**\nКоміт: `{new_cmt}`\n\nМожете оновити бота на Railway."
                                    )
                                    logger.info(f"Notified {uid} about new commit.")
                                except Exception as e:
                                    logger.error(f"Failed to notify {uid}: {e}")
            except Exception as e:
                logger.error(f"Error in github_watcher: {e}")
            
            await asyncio.sleep(GITHUB_POLL_INTERVAL_SECONDS)

    @bot_client.on(events.NewMessage(pattern="/start"))
    async def admin_start(event):
        btn = [
            [Button.inline("📊 Статистика (Mongo)", b"stats")],
            [Button.inline("📁 Перевірити GitHub", b"github")],
            [Button.inline("⚙️ Налаштування GitHub Alerts", b"gh_settings")]
        ]
        await event.respond("Привіт! Панель керування активна.", buttons=btn)

    @bot_client.on(events.CallbackQuery())
    async def admin_callback(event):
        global LAST_GH_CHECK
        user_id = event.sender_id
        if event.data == b"stats":
            if db is None:
                await event.answer("MongoDB не підключена!", alert=True)
                return
            recent = await db.stats.find().sort("time", -1).limit(15).to_list(length=15)
            if not recent:
                await event.answer("Статистика порожня.", alert=True)
                return
            lines = [f"🕐 {r['time'].strftime('%H:%M')} | {r['channel']} ➔ {r['type']}" for r in recent]
            txt = "**Останні 15 пересилань:**\n" + "\n".join(lines)
            await event.edit(txt, buttons=[[Button.inline("Назад", b"back")]])
            
        elif event.data == b"github":
            now = time.time()
            if now - LAST_GH_CHECK < 60:
                await event.answer("Зачекайте. Кулдаун 1 хв.", alert=True)
                return
            LAST_GH_CHECK = now
            await event.answer("Перевіряю GitHub...")
            url = "https://api.github.com/repos/Kredickoa/tg-discord-webhook/commits/main"
            async with httpx.AsyncClient() as c:
                resp = await c.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    msg = data["commit"]["message"]
                    date = data["commit"]["author"]["date"]
                    txt = f"**Останній коміт на GitHub:**\n`{msg}`\n📅 {date}"
                    await event.edit(txt, buttons=[[Button.inline("Назад", b"back")]])
                else:
                    await event.answer("Помилка API GitHub", alert=True)

        elif event.data == b"gh_settings":
            wants_notify = await get_github_setting(user_id)
            state = "🟢 УВІМКНЕНІ" if wants_notify else "🔴 ВИМКНЕНІ"
            txt = f"**Сповіщення про нові коміти GitHub**\nЗараз: {state}"
            btn = [
                [Button.inline("Увімкнути", b"gh_on"), Button.inline("Вимкнути", b"gh_off")],
                [Button.inline("Назад", b"back")]
            ]
            await event.edit(txt, buttons=btn)

        elif event.data == b"gh_on":
            was_notify = await get_github_setting(user_id)
            if was_notify:
                await event.answer("Сповіщення вже увімкнені!", alert=True)
                return
            await set_github_setting(user_id, True)
            await event.answer("Сповіщення увімкнено!", alert=True)
            txt = "**Сповіщення про нові коміти GitHub**\nЗараз: 🟢 УВІМКНЕНІ"
            btn = [
                [Button.inline("Увімкнути", b"gh_on"), Button.inline("Вимкнути", b"gh_off")],
                [Button.inline("Назад", b"back")]
            ]
            await event.edit(txt, buttons=btn)

        elif event.data == b"gh_off":
            was_notify = await get_github_setting(user_id)
            if not was_notify:
                await event.answer("Сповіщення вже вимкнені!", alert=True)
                return
            await set_github_setting(user_id, False)
            await event.answer("Сповіщення вимкнено!", alert=True)
            txt = "**Сповіщення про нові коміти GitHub**\nЗараз: 🔴 ВИМКНЕНІ"
            btn = [
                [Button.inline("Увімкнути", b"gh_on"), Button.inline("Вимкнути", b"gh_off")],
                [Button.inline("Назад", b"back")]
            ]
            await event.edit(txt, buttons=btn)

        elif event.data == b"back":
            btn = [
                [Button.inline("📊 Статистика (Mongo)", b"stats")],
                [Button.inline("📁 Перевірити GitHub", b"github")],
                [Button.inline("⚙️ Налаштування GitHub Alerts", b"gh_settings")]
            ]
            await event.edit("Панель керування:", buttons=btn)

# ─── Main handler ─────────────────────────────────────────────────────────────
client = TelegramClient(StringSession(SESSION_STRING), int(API_ID), API_HASH)

@client.on(events.NewMessage(chats=CHANNELS))
async def on_channel_post(event: events.NewMessage.Event):
    msg: Message = event.message
    
    # 1. Deduplication Check
    if _is_duplicate(msg):
        return

    ch_title = event.chat.title if event.chat else "Телеграм Канал"
    msg_link = f"https://t.me/{event.chat.username}/{msg.id}" if getattr(event.chat, 'username', None) else ""
    raw_text = _escape_pings(msg.text)

    def text_embed() -> list[dict]:
        if not raw_text:
            return []
        return [_text_embed(raw_text, ch_title)]

    # ── Text-only ─────────────────────────────────────────────────────────────
    if msg.text and not msg.media:
        embed = _text_embed(raw_text, ch_title)
        embed["author"] = _author_embed_part(ch_title)
        await _add_stat(ch_title, "Текст")
        await _send([embed], username=ch_title)
        return

    # ── Media Processing ──────────────────────────────────────────────────────
    if msg.media:
        size = 0
        is_video = False
        filename = "file"
        
        if isinstance(msg.media, MessageMediaDocument):
            size = msg.media.document.size
            # Get filename
            for attr in msg.media.document.attributes:
                if hasattr(attr, 'file_name'):
                    filename = attr.file_name
                if isinstance(attr, DocumentAttributeVideo):
                    is_video = True
                    if filename == "file":
                        filename = "video.mp4"
                        
        elif isinstance(msg.media, MessageMediaPhoto):
            # approximate size for photos
            size = max(s.size for s in msg.media.photo.sizes if hasattr(s, 'size'))
            filename = "photo.jpg"

        size_mb = size / 1024 / 1024

        # Download if within limits
        file_data = None
        if size <= DISCORD_MAX_BYTES:
            logger.info("Завантаження медіа: %s МБ", round(size_mb, 1))
            buf = io.BytesIO()
            await client.download_media(msg, buf)
            buf.seek(0)
            file_data = buf.read()

        # Build embeds
        if not file_data:
            await _add_stat(ch_title, f"Медіа (>100мб)")
            media_e = _media_embed_too_large(f"Файл «{filename}»" if not is_video else "Відео", size_mb, msg_link)
            media_e["author"] = _author_embed_part(ch_title)
            await _send([media_e] + text_embed(), username=ch_title)
            return

        # It's an image
        if filename.endswith(('jpg', 'jpeg', 'png', 'webp')) and not is_video:
            await _add_stat(ch_title, f"Фото ({round(size_mb, 1)}мб)")
            media_e = _media_embed_image(filename)
            media_e["author"] = _author_embed_part(ch_title)
            await _send([media_e] + text_embed(), username=ch_title, file_data=file_data, filename=filename)
        # It's a video
        elif is_video:
            await _add_stat(ch_title, f"Відео ({round(size_mb, 1)}мб)")
            media_e = _media_embed_video_ok(filename, size_mb)
            media_e["author"] = _author_embed_part(ch_title)
            await _send([media_e] + text_embed(), username=ch_title, file_data=file_data, filename=filename)
        # Other documents/audio
        else:
            await _add_stat(ch_title, f"Файл ({round(size_mb, 1)}мб)")
            media_e = _base_embed(description=f"📎 Файл: `{filename}` ({size_mb:.1f} МБ)")
            media_e["author"] = _author_embed_part(ch_title)
            await _send([media_e] + text_embed(), username=ch_title, file_data=file_data, filename=filename)


# ─── Entry point ──────────────────────────────────────────────────────────────
async def main() -> None:
    logger.info("Initializing Telethon clients...")
    await client.start()
    
    if bot_client:
        await bot_client.start(bot_token=BOT_TOKEN)
        bot_client.loop.create_task(github_watcher())
        
    logger.info("✅ Успішно! Прослуховування каналів: %s", CHANNELS)
    
    if bot_client:
        await asyncio.gather(
            client.run_until_disconnected(),
            bot_client.run_until_disconnected()
        )
    else:
        await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
