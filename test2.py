import asyncio
import os

from playwright.sync_api import sync_playwright, Playwright

profile_path = os.path.join(os.getcwd(), "user_data")


def scrape_data(playwright: Playwright):
    args = []
    args.append("--disable-blink-features=AutomationControlled")
    args.append("--disable-webrtc")
    browser = playwright.chromium.launch_persistent_context(
        user_data_dir=profile_path,
        headless=True,
        args=args,
        timezone_id="Europe/Moscow",
        proxy={
            'server': 'http://194.226.115.57:62210',
            'username': 'JKThSkEu',
            'password': 'whh3hUFn'
        })
    page = browser.new_page()
    response = page.goto("https://krasnoyarsk.cian.ru/sale/flat/313164518/",timeout=12000)
    print(response.status)
    input("Press ENTER to exit:")
    browser.close()


def main():
    with sync_playwright() as playwright:
        scrape_data(playwright)


if __name__ == "__main__":
    main()
