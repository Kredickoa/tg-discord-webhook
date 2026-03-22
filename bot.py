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
import collections

import httpx
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import Message, MessageMediaPhoto, MessageMediaDocument, DocumentAttributeVideo

load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────
API_ID = os.environ.get("TELEGRAM_API_ID")
API_HASH = os.environ.get("TELEGRAM_API_HASH")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION_STRING")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

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
    desc = f"⚠️ {label} ({size_mb:.1f} МБ) — перевищує ліміт Discord ({DISCORD_MAX_BYTES//1024//1024} МБ)."
    if msg_link:
        desc += f"\n🔗 [**Відкрити в Telegram**]({msg_link})"
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
            media_e = _media_embed_too_large(f"Файл «{filename}»" if not is_video else "Відео", size_mb, msg_link)
            media_e["author"] = _author_embed_part(ch_title)
            await _send([media_e] + text_embed(), username=ch_title)
            return

        # It's an image
        if filename.endswith(('jpg', 'jpeg', 'png', 'webp')) and not is_video:
            media_e = _media_embed_image(filename)
            media_e["author"] = _author_embed_part(ch_title)
            await _send([media_e] + text_embed(), username=ch_title, file_data=file_data, filename=filename)
        # It's a video
        elif is_video:
            media_e = _media_embed_video_ok(filename, size_mb)
            media_e["author"] = _author_embed_part(ch_title)
            await _send([media_e] + text_embed(), username=ch_title, file_data=file_data, filename=filename)
        # Other documents/audio
        else:
            media_e = _base_embed(description=f"📎 Файл: `{filename}` ({size_mb:.1f} МБ)")
            media_e["author"] = _author_embed_part(ch_title)
            await _send([media_e] + text_embed(), username=ch_title, file_data=file_data, filename=filename)


# ─── Entry point ──────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("🚀 MTProto Bot starting... connecting to Telegram")
    client.start()
    logger.info(f"✅ Успішно! Прослуховування каналів: {CHANNELS}")
    client.run_until_disconnected()


if __name__ == "__main__":
    main()
