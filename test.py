# from playwright.async_api import async_playwright
import asyncio

from fake_headers import Headers
from undetected_playwright.async_api import async_playwright

# Применяем патч
BATCH_SIZE = 5
MAX_QUEUE_SIZE = 40
MAX_WORKERS = 18


async def scrape_page(context, page_url):
    try:
        page = await context.new_page()
        await page.goto(page_url, timeout=120000)
        await page.wait_for_timeout(timeout=120000)
        date_element = page.locator('[data-testid="metadata-updated-date"] span')
        text = await date_element.inner_text(timeout=5000)
    except Exception as e:
        print(e)
        return False, page_url, None, None, None


async def get_site_data(urls, proxy_url) -> (str, str):
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",  # Маскировка бота
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-infobars",
                "--disable-gpu",
            ],
            proxy=proxy_url
        )
        header = Headers(
            browser="chrome",  # Generate only Chrome UA
            os="win",  # Generate ony Windows platform
            headers=True  # generate misc headers

        )

        headers = header.generate()
        context = await browser.new_context(
            viewport={"width": 1280, "height": 1580}
        )
        tasks = [scrape_page(context, url) for url in
                 urls]  # Создаём 50 задач
        results = await asyncio.gather(*tasks)  # Запускаем
        await context.close()
        await browser.close()

        for result in results:
            yield result


async def main():
    async for i in get_site_data(
            ["https://www.browserscan.net/"],
            {"server": "http://195.208.95.129:64640", "username": "JKThSkEu", "password": "whh3hUFn"}
    ):
        print(i)


asyncio.run(main())
