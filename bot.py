"""
Telegram -> Discord forwarder bot (Telethon userbot).
"""

from __future__ import annotations

import asyncio
import collections
import datetime
import io
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

import httpx
import motor.motor_asyncio
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.custom import Button
from telethon.tl.types import (
    DocumentAttributeVideo,
    Message,
    MessageMediaDocument,
    MessageMediaPhoto,
)

load_dotenv()

API_ID = os.environ.get("TELEGRAM_API_ID")
API_HASH = os.environ.get("TELEGRAM_API_HASH")
SESSION_STRING = os.environ.get("TELEGRAM_SESSION_STRING")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
MONGO_URI = os.environ.get("MONGO_URI", "").strip()
GITHUB_REPO = os.environ.get("GITHUB_REPO", "Kredickoa/tg-discord-webhook").strip()
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main").strip()
GITHUB_POLL_INTERVAL_SECONDS = int(os.environ.get("GITHUB_POLL_INTERVAL_SECONDS", "300"))

CHANNELS = ["ab3army", "ab3brigade"]
CHANNEL_USERNAMES = {channel.lower() for channel in CHANNELS}
CHANNEL_IDS: set[int] = set()

EMBED_COLOR = 0xFF6600
DISCORD_MAX_BYTES = 100 * 1024 * 1024
DISCORD_MAX_ATTACHMENTS = 10
DISCORD_RETRY_LIMIT = 3

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telethon.client.updates").setLevel(logging.WARNING)
logging.getLogger("telethon.client.downloads").setLevel(logging.WARNING)
logging.getLogger("telethon.network").setLevel(logging.WARNING)

if not all([API_ID, API_HASH, SESSION_STRING, DISCORD_WEBHOOK_URL]):
    logger.error("Missing required environment variables.")
    raise SystemExit(1)


@dataclass(slots=True)
class DiscordAttachment:
    filename: str
    content: bytes
    mime: str


sent_cache: collections.deque[str] = collections.deque(maxlen=200)
sent_cache_set: set[str] = set()

mongo_client = None
db = None
if MONGO_URI:
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    db = mongo_client["telegram_forwarder"]


def _base_embed(title: str | None = None, description: str | None = None) -> dict[str, Any]:
    embed: dict[str, Any] = {"color": EMBED_COLOR}
    if title:
        embed["title"] = title[:256]
    if description:
        embed["description"] = description[:4096]
    return embed


def _text_embed(text: str, channel_title: str) -> dict[str, Any]:
    embed = _base_embed(description=text[:4096])
    embed["footer"] = {"text": channel_title}
    return embed


def _media_embed_too_large(label: str, size_mb: float, msg_link: str) -> dict[str, Any]:
    if msg_link:
        desc = f"[{label} ({size_mb:.1f} MB) - Open in Telegram]({msg_link})"
    else:
        desc = f"{label} ({size_mb:.1f} MB)"
    return _base_embed(description=desc)


def _guess_mime(filename: str) -> str:
    ext = filename.rsplit(".", 1)[-1].lower()
    return {
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "webp": "image/webp",
        "mp4": "video/mp4",
        "mov": "video/quicktime",
        "mp3": "audio/mpeg",
        "ogg": "audio/ogg",
    }.get(ext, "application/octet-stream")


def _escape_pings(text: str | None) -> str:
    return (text or "").replace("@", "＠")


def _chunk_attachments(attachments: list[DiscordAttachment]) -> list[list[DiscordAttachment]]:
    return [
        attachments[index:index + DISCORD_MAX_ATTACHMENTS]
        for index in range(0, len(attachments), DISCORD_MAX_ATTACHMENTS)
    ]


def _github_commit_api_url() -> str:
    return f"https://api.github.com/repos/{GITHUB_REPO}/commits"


def _github_headers() -> dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "User-Agent": "tg-discord-webhook-bot",
    }


def _get_forward_origin(msg: Message) -> tuple[int | None, int | None]:
    fwd_from = getattr(msg, "fwd_from", None)
    if not fwd_from:
        return None, None
    origin = getattr(fwd_from, "from_id", None)
    origin_channel_id = getattr(origin, "channel_id", None)
    origin_message_id = getattr(fwd_from, "channel_post", None)
    return origin_channel_id, origin_message_id


def _build_normalized_signature(chat, messages: list[Message]) -> str | None:
    primary = messages[0]
    current_chat_id = getattr(chat, "id", None)
    origin_channel_id, origin_message_id = _get_forward_origin(primary)
    grouped_id = getattr(primary, "grouped_id", None)

    if grouped_id is not None:
        item_refs: list[str] = []
        for msg in messages:
            item_origin_channel_id, item_origin_message_id = _get_forward_origin(msg)
            if item_origin_channel_id is not None and item_origin_message_id is not None:
                item_refs.append(f"{item_origin_channel_id}:{item_origin_message_id}")
                continue
            if current_chat_id is not None and getattr(msg, "id", None) is not None:
                item_refs.append(f"{current_chat_id}:{msg.id}")

        if len(item_refs) == len(messages):
            return "groupitems:" + "|".join(item_refs)

        signature_channel_id = origin_channel_id or current_chat_id
        if signature_channel_id is not None:
            return f"group:{signature_channel_id}:{grouped_id}"

    if origin_channel_id is not None and origin_message_id is not None:
        return f"msg:{origin_channel_id}:{origin_message_id}"

    if current_chat_id is not None and getattr(primary, "id", None) is not None:
        return f"msg:{current_chat_id}:{primary.id}"

    fallback_parts: list[str] = []
    for msg in messages:
        if msg.raw_text:
            fallback_parts.append(f"text:{hash(msg.raw_text)}")
        if isinstance(msg.media, MessageMediaPhoto):
            fallback_parts.append(f"photo:{msg.media.photo.id}")
        elif isinstance(msg.media, MessageMediaDocument):
            fallback_parts.append(f"doc:{msg.media.document.id}:{msg.media.document.size}")

    if not fallback_parts:
        return None

    return "fallback:" + "|".join(fallback_parts)


def _remember_signature(signature: str) -> None:
    if len(sent_cache) == sent_cache.maxlen:
        oldest = sent_cache.popleft()
        sent_cache_set.discard(oldest)
    sent_cache.append(signature)
    sent_cache_set.add(signature)


def _is_duplicate_signature(signature: str, message_id: int | None, grouped_id: int | None) -> bool:
    if signature in sent_cache_set:
        logger.info(
            "duplicate_skipped signature=%s message_id=%s grouped_id=%s",
            signature,
            message_id,
            grouped_id,
        )
        return True
    _remember_signature(signature)
    return False


def _is_target_chat(chat) -> bool:
    username = getattr(chat, "username", None)
    if username and username.lower() in CHANNEL_USERNAMES:
        return True

    chat_id = getattr(chat, "id", None)
    if isinstance(chat_id, int) and chat_id in CHANNEL_IDS:
        return True

    return False


async def _add_stat(channel_title: str, msg_type: str) -> None:
    if db is None:
        return
    await db.stats.insert_one(
        {
            "channel": channel_title,
            "type": msg_type,
            "time": datetime.datetime.now(),
        }
    )


async def _record_delivery_stat(channel_title: str, msg_type: str, delivered: bool) -> None:
    if delivered:
        await _add_stat(channel_title, msg_type)


async def _post_discord(
    username: str,
    embeds: list[dict[str, Any]],
    attachments: list[DiscordAttachment],
) -> bool:
    payload: dict[str, Any] = {"username": username}
    if embeds:
        payload["embeds"] = embeds

    async with httpx.AsyncClient(timeout=60) as client:
        for attempt in range(1, DISCORD_RETRY_LIMIT + 1):
            if attachments:
                files: dict[str, tuple[Any, ...]] = {
                    "payload_json": (None, json.dumps(payload), "application/json")
                }
                for index, attachment in enumerate(attachments):
                    files[f"files[{index}]"] = (
                        attachment.filename,
                        attachment.content,
                        attachment.mime,
                    )
                response = await client.post(DISCORD_WEBHOOK_URL, files=files)
            else:
                response = await client.post(DISCORD_WEBHOOK_URL, json=payload)

            if response.status_code in (200, 204):
                logger.info(
                    "discord_send_ok username=%s embeds=%s attachments=%s status=%s",
                    username,
                    len(embeds),
                    len(attachments),
                    response.status_code,
                )
                return True

            if response.status_code == 429:
                retry_after = 1.0
                try:
                    retry_after = float(response.json().get("retry_after", 1.0))
                except Exception:
                    retry_after = 1.0
                logger.warning(
                    "discord_rate_limited username=%s attempt=%s retry_after=%s",
                    username,
                    attempt,
                    retry_after,
                )
                await asyncio.sleep(retry_after)
                continue

            logger.error(
                "discord_send_failed username=%s status=%s body=%s",
                username,
                response.status_code,
                response.text[:400],
            )
            return False

    logger.error(
        "discord_send_failed username=%s status=429 body=retry_limit_exceeded",
        username,
    )
    return False


async def _send(
    username: str,
    embeds: list[dict[str, Any]] | None = None,
    attachments: list[DiscordAttachment] | None = None,
) -> bool:
    embeds = embeds or []
    attachments = attachments or []
    attachment_chunks = _chunk_attachments(attachments) if attachments else [[]]

    overall_success = True
    for chunk_index, attachment_chunk in enumerate(attachment_chunks):
        chunk_embeds = embeds if chunk_index == 0 else []
        delivered = await _post_discord(username, chunk_embeds, attachment_chunk)
        overall_success = overall_success and delivered
        if not delivered:
            return False

    return overall_success


async def _download_attachment(msg: Message) -> tuple[DiscordAttachment | None, str | None, float]:
    size = 0
    filename = f"message_{msg.id}"

    if isinstance(msg.media, MessageMediaDocument):
        size = msg.media.document.size
        filename = f"document_{msg.id}"
        for attr in msg.media.document.attributes:
            if hasattr(attr, "file_name"):
                filename = attr.file_name
            if isinstance(attr, DocumentAttributeVideo) and "." not in filename:
                filename = f"video_{msg.id}.mp4"
    elif isinstance(msg.media, MessageMediaPhoto):
        size = max(s.size for s in msg.media.photo.sizes if hasattr(s, "size"))
        filename = f"photo_{msg.id}.jpg"
    else:
        return None, None, 0.0

    size_mb = size / 1024 / 1024
    if size > DISCORD_MAX_BYTES:
        return None, filename, size_mb

    logger.info(
        "media_download_start message_id=%s size_mb=%.1f filename=%s",
        getattr(msg, "id", None),
        size_mb,
        filename,
    )
    buffer = io.BytesIO()
    await client.download_media(msg, buffer)
    buffer.seek(0)
    return DiscordAttachment(filename=filename, content=buffer.read(), mime=_guess_mime(filename)), filename, size_mb


def _build_text_embeds(channel_title: str, text: str | None) -> list[dict[str, Any]]:
    escaped = _escape_pings(text)
    if not escaped:
        return []
    return [_text_embed(escaped, channel_title)]


def _classify_messages(messages: list[Message]) -> str:
    if len(messages) > 1:
        return "Альбом"
    msg = messages[0]
    if msg.text and not msg.media:
        return "Текст"
    if isinstance(msg.media, MessageMediaPhoto):
        return "Фото"
    if isinstance(msg.media, MessageMediaDocument):
        for attr in msg.media.document.attributes:
            if isinstance(attr, DocumentAttributeVideo):
                return "Відео"
        return "Файл"
    return "Повідомлення"


async def _process_target_messages(chat, messages: list[Message], source: str) -> None:
    if not messages:
        return

    primary = messages[0]
    logger.info(
        "target_message_received source=%s chat_id=%s username=%s message_id=%s media=%s text=%s grouped_id=%s items=%s",
        source,
        getattr(chat, "id", None),
        getattr(chat, "username", None),
        getattr(primary, "id", None),
        any(bool(msg.media) for msg in messages),
        any(bool(msg.text) for msg in messages),
        getattr(primary, "grouped_id", None),
        len(messages),
    )

    signature = _build_normalized_signature(chat, messages)
    if signature:
        logger.info(
            "normalized_signature signature=%s chat_id=%s message_id=%s grouped_id=%s",
            signature,
            getattr(chat, "id", None),
            getattr(primary, "id", None),
            getattr(primary, "grouped_id", None),
        )
        if _is_duplicate_signature(signature, getattr(primary, "id", None), getattr(primary, "grouped_id", None)):
            return

    channel_title = getattr(chat, "title", None) or "Telegram Channel"
    text_source = next((msg.text for msg in messages if msg.text), None)
    embeds = _build_text_embeds(channel_title, text_source)
    attachments: list[DiscordAttachment] = []

    for msg in messages:
        if not msg.media:
            continue
        attachment, filename, size_mb = await _download_attachment(msg)
        if attachment is not None:
            attachments.append(attachment)
            continue

        if filename:
            msg_link = f"https://t.me/{chat.username}/{msg.id}" if getattr(chat, "username", None) else ""
            too_large_embed = _media_embed_too_large(filename, size_mb, msg_link)
            delivered = await _send(channel_title, embeds=[too_large_embed] + embeds)
            await _record_delivery_stat(channel_title, "Медіа (>100мб)", delivered)
            return

    if not attachments and not embeds:
        logger.info(
            "target_message_skipped chat_id=%s message_id=%s reason=no_content",
            getattr(chat, "id", None),
            getattr(primary, "id", None),
        )
        return

    delivered = await _send(channel_title, embeds=embeds, attachments=attachments)
    await _record_delivery_stat(channel_title, _classify_messages(messages), delivered)


async def _fetch_github_commits(limit: int = 10) -> list[dict[str, Any]]:
    params = {"sha": GITHUB_BRANCH, "per_page": limit}
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.get(_github_commit_api_url(), params=params, headers=_github_headers())
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, list) else []


if BOT_TOKEN:
    bot_client = TelegramClient(StringSession(""), int(API_ID), API_HASH)
    github_last_sha = None
    last_manual_github_check = 0.0

    async def get_github_setting(user_id: int) -> bool:
        if db is None:
            return False
        doc = await db.settings.find_one({"_id": f"gh_notify_{user_id}"})
        return bool(doc["value"]) if doc else False

    async def set_github_setting(user_id: int, value: bool) -> None:
        if db is None:
            return
        await db.settings.update_one(
            {"_id": f"gh_notify_{user_id}"},
            {"$set": {"value": value}},
            upsert=True,
        )

    async def get_stored_sha() -> str | None:
        if db is None:
            return None
        doc = await db.settings.find_one({"_id": "last_github_sha"})
        return doc["value"] if doc else None

    async def set_stored_sha(sha: str) -> None:
        if db is None:
            return
        await db.settings.update_one(
            {"_id": "last_github_sha"},
            {"$set": {"value": sha}},
            upsert=True,
        )

    async def check_github_commits() -> list[dict[str, Any]]:
        global github_last_sha
        try:
            commits = await _fetch_github_commits()
        except Exception as exc:
            logger.error("github_check_failed repo=%s branch=%s error=%s", GITHUB_REPO, GITHUB_BRANCH, exc)
            return []

        if not commits:
            return []

        latest_sha = commits[0]["sha"]
        if github_last_sha is None:
            github_last_sha = await get_stored_sha()

        if github_last_sha is None:
            github_last_sha = latest_sha
            await set_stored_sha(latest_sha)
            logger.info(
                "github_baseline_initialized sha=%s repo=%s branch=%s",
                latest_sha[:7],
                GITHUB_REPO,
                GITHUB_BRANCH,
            )
            return []

        if latest_sha == github_last_sha:
            return []

        new_commits: list[dict[str, Any]] = []
        for commit in commits:
            if commit["sha"] == github_last_sha:
                break
            new_commits.append(commit)

        github_last_sha = latest_sha
        await set_stored_sha(latest_sha)
        return list(reversed(new_commits))

    async def github_watcher() -> None:
        logger.info("github_watcher_started repo=%s branch=%s", GITHUB_REPO, GITHUB_BRANCH)
        await asyncio.sleep(5)
        while True:
            try:
                new_commits = await check_github_commits()
                if new_commits and db is not None:
                    subscribers: list[int] = []
                    async for doc in db.settings.find({"value": True}):
                        if str(doc["_id"]).startswith("gh_notify_"):
                            subscribers.append(int(str(doc["_id"]).replace("gh_notify_", "")))

                    logger.info(
                        "github_commits_detected count=%s subscribers=%s repo=%s branch=%s",
                        len(new_commits),
                        len(subscribers),
                        GITHUB_REPO,
                        GITHUB_BRANCH,
                    )

                    for commit in new_commits:
                        commit_msg = commit["commit"]["message"]
                        commit_sha = commit["sha"][:7]
                        commit_url = commit.get("html_url", "")
                        notify_text = (
                            "GitHub update detected\n"
                            f"Repo: `{GITHUB_REPO}`\n"
                            f"Branch: `{GITHUB_BRANCH}`\n"
                            f"SHA: `{commit_sha}`\n"
                            f"Commit: `{commit_msg}`"
                        )
                        if commit_url:
                            notify_text += f"\n{commit_url}"

                        for user_id in subscribers:
                            try:
                                await bot_client.send_message(user_id, notify_text)
                                logger.info("github_notify_ok user_id=%s sha=%s", user_id, commit_sha)
                            except Exception as exc:
                                logger.error("github_notify_failed user_id=%s sha=%s error=%s", user_id, commit_sha, exc)
            except Exception as exc:
                logger.error("github_watcher_failed error=%s", exc)

            await asyncio.sleep(GITHUB_POLL_INTERVAL_SECONDS)

    @bot_client.on(events.NewMessage(pattern="/start"))
    async def admin_start(event) -> None:
        buttons = [
            [Button.inline("Stats", b"stats")],
            [Button.inline("Check GitHub", b"github")],
            [Button.inline("GitHub Alerts", b"gh_settings")],
        ]
        await event.respond("Admin panel is active.", buttons=buttons)

    @bot_client.on(events.CallbackQuery())
    async def admin_callback(event) -> None:
        global last_manual_github_check
        user_id = event.sender_id

        if event.data == b"stats":
            if db is None:
                await event.answer("MongoDB is not configured.", alert=True)
                return
            recent = await db.stats.find().sort("time", -1).limit(15).to_list(length=15)
            if not recent:
                await event.answer("No delivery stats yet.", alert=True)
                return
            lines = [f"{record['time'].strftime('%H:%M')} | {record['channel']} -> {record['type']}" for record in recent]
            await event.edit("Last 15 deliveries:\n" + "\n".join(lines), buttons=[[Button.inline("Back", b"back")]])
            return

        if event.data == b"github":
            now = time.time()
            if now - last_manual_github_check < 60:
                await event.answer("Cooldown 60s.", alert=True)
                return
            last_manual_github_check = now
            try:
                commits = await _fetch_github_commits(limit=1)
            except Exception as exc:
                logger.error("github_manual_check_failed error=%s", exc)
                await event.answer("GitHub API error.", alert=True)
                return
            if not commits:
                await event.answer("No commits returned.", alert=True)
                return
            data = commits[0]
            text = (
                "Latest GitHub commit:\n"
                f"Repo: `{GITHUB_REPO}`\n"
                f"Branch: `{GITHUB_BRANCH}`\n"
                f"SHA: `{data['sha'][:7]}`\n"
                f"`{data['commit']['message']}`\n"
                f"{data['commit']['author']['date']}"
            )
            await event.edit(text, buttons=[[Button.inline("Back", b"back")]])
            return

        if event.data == b"gh_settings":
            enabled = await get_github_setting(user_id)
            state = "ON" if enabled else "OFF"
            buttons = [
                [Button.inline("Enable", b"gh_on"), Button.inline("Disable", b"gh_off")],
                [Button.inline("Back", b"back")],
            ]
            await event.edit(f"GitHub alerts are {state}.", buttons=buttons)
            return

        if event.data == b"gh_on":
            await set_github_setting(user_id, True)
            await event.answer("GitHub alerts enabled.", alert=True)
            await admin_callback(type("EventProxy", (), {"data": b"gh_settings", "sender_id": user_id, "edit": event.edit, "answer": event.answer}))
            return

        if event.data == b"gh_off":
            await set_github_setting(user_id, False)
            await event.answer("GitHub alerts disabled.", alert=True)
            await admin_callback(type("EventProxy", (), {"data": b"gh_settings", "sender_id": user_id, "edit": event.edit, "answer": event.answer}))
            return

        if event.data == b"back":
            buttons = [
                [Button.inline("Stats", b"stats")],
                [Button.inline("Check GitHub", b"github")],
                [Button.inline("GitHub Alerts", b"gh_settings")],
            ]
            await event.edit("Admin panel:", buttons=buttons)


client = TelegramClient(StringSession(SESSION_STRING), int(API_ID), API_HASH)


@client.on(events.Album())
async def on_channel_album(event: events.Album.Event) -> None:
    if not _is_target_chat(event.chat):
        return
    if not event.messages:
        return
    logger.info(
        "target_album_received chat_id=%s username=%s items=%s grouped_id=%s",
        getattr(event.chat, "id", None),
        getattr(event.chat, "username", None),
        len(event.messages),
        getattr(event.messages[0], "grouped_id", None),
    )
    await _process_target_messages(event.chat, list(event.messages), "album")


@client.on(events.NewMessage())
async def on_channel_post(event: events.NewMessage.Event) -> None:
    if not _is_target_chat(event.chat):
        return
    if getattr(event.message, "grouped_id", None) is not None:
        logger.info(
            "grouped_message_buffered chat_id=%s username=%s message_id=%s grouped_id=%s",
            getattr(event.chat, "id", None),
            getattr(event.chat, "username", None),
            getattr(event.message, "id", None),
            getattr(event.message, "grouped_id", None),
        )
        return
    await _process_target_messages(event.chat, [event.message], "new_message")


async def main() -> None:
    logger.info("Initializing Telethon clients...")
    await client.start()

    CHANNEL_IDS.clear()
    for channel_name in CHANNELS:
        try:
            entity = await client.get_entity(channel_name)
            entity_id = getattr(entity, "id", None)
            if isinstance(entity_id, int):
                CHANNEL_IDS.add(entity_id)
            logger.info(
                "Resolved channel target: username=%s id=%s title=%s",
                channel_name,
                entity_id,
                getattr(entity, "title", None),
            )
        except Exception as exc:
            logger.error("channel_resolve_failed username=%s error=%s", channel_name, exc)

    if bot_client:
        await bot_client.start(bot_token=BOT_TOKEN)
        bot_client.loop.create_task(github_watcher())

    logger.info("Forwarder started for channels=%s", CHANNELS)

    if bot_client:
        await asyncio.gather(
            client.run_until_disconnected(),
            bot_client.run_until_disconnected(),
        )
    else:
        await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
