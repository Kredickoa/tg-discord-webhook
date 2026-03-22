import os
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

load_dotenv()

API_ID = os.environ.get("TELEGRAM_API_ID")
API_HASH = os.environ.get("TELEGRAM_API_HASH")

if not API_ID or not API_HASH:
    print("❌ Помилка: Не знайдено TELEGRAM_API_ID або TELEGRAM_API_HASH у файлі .env")
    print("Отримайте їх на https://my.telegram.org/apps і додайте у .env")
    exit(1)

print("🔐 Авторизація в Telegram (введіть номер телефону з кодом країни, потім код з СМС/Телеграму)...")
with TelegramClient(StringSession(), int(API_ID), API_HASH) as client:
    print("\n✅ Успішно! Ось ваш TELEGRAM_SESSION_STRING:")
    print("-----------------------------------------------------------------------------------------")
    print(client.session.save())
    print("-----------------------------------------------------------------------------------------")
    print("👉 Збережіть цей довгий рядок у файл .env як TELEGRAM_SESSION_STRING (і в Railway теж)")
