import asyncio
import json
import os

from undetected_playwright.sync_api import sync_playwright, Playwright

profile_path = os.path.join(os.getcwd(), "user_data/fwefwefefef")
from playwright_stealth import stealth_sync

COOKIE_FILE = "cookies.json"


def save_cookies(context):
    with open(COOKIE_FILE, "w") as f:
        json.dump(context.cookies(), f)


def load_cookies(context):
    try:
        with open(COOKIE_FILE, "r") as f:
            cookies = json.load(f)
            context.add_cookies(cookies)
    except FileNotFoundError:
        print("Файл cookies.json не найден, пропускаем загрузку.")


def scrape_data(playwright: Playwright):
    args = []
    args.append("--disable-blink-features=AutomationControlled")
    args.append("--disable-features=WebRTC,WebGL,Canvas")
    args.append("--disable-webrtc")
    args.append("--disable-dev-shm-usage")
    args.append("--no-sandbox")
    browser = playwright.chromium.launch_persistent_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        user_data_dir=profile_path,
        headless=False,
        args=args,
        timezone_id="Europe/Volgograd",
        # proxy={'server': 'http://212.60.7.221:63968', 'username': 'JKThSkEu', 'password': 'whh3hUFn'}
        # proxy={"server": "http://45.153.52.106:63452", "username": "JKThSkEu", "password": "whh3hUFn"},
        proxy={'server': 'http://37.139.58.84:64536', 'username': 'JKThSkEu', 'password': 'whh3hUFn'}
    )

    # load_cookies(context)
    page = browser.new_page()
    # stealth_sync(page)

    response = page.goto("https://www.browserscan.net/")
    print(response.status)
    input("Press ENTER to exit:")
    # save_cookies(context)
    browser.close()


def main():
    with sync_playwright() as playwright:
        scrape_data(playwright)


if __name__ == "__main__":
    main()

# import requests
#
# PROXY = "http://username:password@proxy_ip:port"  # Заменить на свои данные
#
# proxies = {
#     "http": PROXY,
#     "https": PROXY
# }
#
# # Запрос к API, который определяет местоположение
# response = requests.get("http://ip-api.com/json", proxies=proxies)
#
# if response.status_code == 200:
#     data = response.json()
#     print(f"IP: {data['query']}")
#     print(f"Город: {data['city']}")
#     print(f"Страна: {data['country']}")
#     print(f"Тайм-зона: {data['timezone']}")
# else:
#     print("Ошибка получения данных")
