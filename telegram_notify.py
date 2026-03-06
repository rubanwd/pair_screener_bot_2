
import requests

def send_telegram(bot_token, chat_id, text):
    if not bot_token or not chat_id:
        return

    url=f"https://api.telegram.org/bot{bot_token}/sendMessage"

    requests.post(url,json={
        "chat_id":chat_id,
        "text":text
    })
