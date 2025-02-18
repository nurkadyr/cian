import asyncio
import base64
import datetime
import hashlib
import json
import multiprocessing
import os
import time
import uuid
from io import BytesIO

import aiohttp
from PIL import Image
from curl_cffi import requests
from fake_headers import Headers
from undetected_playwright.sync_api import sync_playwright
from pymongo import MongoClient

from mongo import insert_photo, insert_html_data, insert_screenshot, update_unique_status
from ms import insert_product, insert_product_files, get_connection, is_url_exists

BATCH_SIZE = 3
MAX_QUEUE_SIZE = 70
MAX_WORKERS = 36


def parse_url(urls, proxy_url, db_html, db_photos, db_screenshots, conn):
    success, url, html_id, image_id, data = get_site_data(urls, proxy_url, db_html, db_photos, db_screenshots)
    if success:
        product_id = insert_product(
            conn,
            source=1, category=2, segment_on_source=3, vehicle_sub_type=4, region=5,
            deal_type=6, type_=7,
            brand=100, url=url, is_from_archive=0,
            task_guid="550e8400-e29b-41d4-a716-446655440000",
            creation_date=datetime.datetime.utcnow(), status=1, mongo_id=html_id,
            id_on_source=9999999, is_files_complete=1,
            last_modification_date=datetime.datetime.utcnow(),
            parser_version=1.0, weapon_kind=2, machine_name="Server-01"
        )
        product_file_id = insert_product_files(
            conn,
            url=None,
            mongo_id=image_id,
            product_id=product_id,
            file_type=1,
            creation_time=datetime.datetime.utcnow(),
            status=0
        )
        update_unique_status(db_photos, db_screenshots, "screenshots", image_id, product_id, product_file_id)
        for image_url, mongo_id in data["images_mongo"]:
            product_file_id = insert_product_files(
                conn,
                url=image_url,
                mongo_id=mongo_id,
                product_id=product_id,
                file_type=2,
                creation_time=datetime.datetime.utcnow(),
                status=0
            )
            update_unique_status(db_photos, db_screenshots, "photos", mongo_id, product_id, product_file_id)

    return success, url


def scrape_page(context, page_url, proxy, db_html, db_photos, db_screenshots):
    try:
        page = context.new_page()
        # page.on("request", lambda request: print(f"\n🔹 Запрос: {request.url}\n{request.headers}"))
        # await page.route("**/*", save_resource)  # Перехватываем все запросы
        page.goto(page_url, timeout=120000)

        date_element = page.locator('[data-testid="metadata-updated-date"] span')
        text = date_element.inner_text(timeout=5000)
        yesterday_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%d %b")
        today_date = datetime.datetime.now().strftime("%d %b")
        month_translation = {
            "Jan": "янв", "Feb": "фев", "Mar": "мар", "Apr": "апр",
            "May": "мая", "Jun": "июн", "Jul": "июл", "Aug": "авг",
            "Sep": "сен", "Oct": "окт", "Nov": "ноя", "Dec": "дек"
        }

        for eng, rus in month_translation.items():
            yesterday_date = yesterday_date.replace(eng, rus)
            today_date = today_date.replace(eng, rus)
        if "вчера" in text:
            new_text = text.replace("вчера", yesterday_date)
            date_element.evaluate('(node, newText) => node.innerText = newText', new_text)

        if "сегодня" in text:
            new_text = text.replace("сегодня", today_date)
            date_element.evaluate('(node, newText) => node.innerText = newText', new_text)

        selectors = [
            '[data-name="CardSection"]',
            '[data-name="CookiesNotification"]',
            '[data-name="AuthorBrandingAside"]',
            '[data-state="open"]',
            '[id="adfox-stretch-banner"]',
            '#mapsSection'
        ]

        for selector in selectors:
            page.locator(selector).evaluate_all("elements => elements.forEach(el => el.remove())")

        screenshot_path = f"screenshot/{uuid.uuid4()}.jpeg"
        page.locator("body").screenshot(path=screenshot_path, type="jpeg", quality=40)

        with open(screenshot_path, "rb") as img_file:
            base64_image = base64.b64encode(img_file.read()).decode("utf-8")
        os.remove(screenshot_path)
        try:
            json_data = page.locator('script[type="application/ld+json"]').inner_text(timeout=10000)
            images = json.loads(json_data).get("image", [])
        except Exception as e:
            print(e)
            images = []

        data = {
            "region": (page.locator('[itemprop="name"]').nth(0).inner_text()).split(" ")[-1]
        }

        html = page.content()
        page.close()
        images_mongo = []
        for i in download_image_list(images, db_photos, proxy):
            images_mongo.append(i)
        data["images_mongo"] = images_mongo

        return (
            True,
            page_url,
            str(insert_html_data(db_html, html).inserted_id),
            str(insert_screenshot(db_screenshots, base64_image).inserted_id),
            data
        )
    except Exception as e:
        print(e)
        return False, page_url, None, None, None


def get_site_data(url, proxy_url, db_html, db_photos, db_screenshots) -> (str, str):
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
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
        context = browser.new_context(
            user_agent=headers.pop("User-Agent"),
            extra_http_headers={**headers, 'Referer': 'https://google.com'},
            viewport={"width": 1280, "height": 1580}
        )
        result = scrape_page(context, url, proxy_url, db_html, db_photos, db_screenshots)

        context.close()
        browser.close()

        return result


def download_image_list(images, db_photos, proxy):
    proxy_url = f"http://{proxy['username']}:{proxy['password']}@{proxy['server'].replace('http://', '')}"

    tasks = [download_image(url, db_photos, proxy_url) for url in images]
    for i in tasks:
        if i is not None:
            yield i


def download_image(url, db_photos, proxy) -> (str, str):
    try:
        headers = {
            "Referer": "https://www.cian.ru/"
        }

        response = requests.get(url, headers=headers, impersonate="chrome", proxies={"http": proxy, "https": proxy},
                                timeout=60, verify=False)

        if response.status_code == 200:
            img = Image.open(BytesIO(response.content))
            buffer = BytesIO()
            img.save(buffer, format="JPEG", quality=50)
            image_base64 = base64.b64encode(buffer.getvalue()).decode("utf-8")

            return url, str(insert_photo(db_photos, image_base64).inserted_id)
        else:
            print(f"Failed to download {url}, status: {response.status_code}")

    except Exception as e:
        print(f"Error downloading {url}: {e}")


def extract_urls_from_folder():
    conn = get_connection()
    folder_path = "urls"
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Папка '{folder_path}' не найдена")
    count = 0
    for filename in os.listdir(folder_path):
        if filename.endswith(".txt"):  # Обрабатываем только .txt файлы
            file_path = os.path.join(folder_path, filename)
            with open(file_path, "r", encoding="utf-8") as file:
                for line in file:
                    url = line.strip()
                    if url:
                        count += 1
                        if count % 100 == 0:
                            print(count)

                        if count < 13200:
                            continue
                        if not is_url_exists(conn, url):
                            yield url
    conn.close()


def worker(queue, proxy_url):
    client = MongoClient("mongodb://192.168.1.59:27017/")
    db_html = client["htmlData2"]
    db_photos = client["adsPhotos2"]
    db_screenshots = client["adsScreenshots2"]
    conn = get_connection()
    while True:
        urls_chunk = queue.get()
        print("start", urls_chunk, queue.qsize())
        if urls_chunk is None:
            print("worker end")
            break  # Завершаем процесс
        success, url = parse_url(urls_chunk, proxy_url, db_html, db_photos, db_screenshots, conn)

        if not success:
            queue.put(url)
    conn.close()


async def producer(queue):
    """Наполняет очередь URL, чтобы не перегружать память."""
    for i in extract_urls_from_folder():
        queue.put(i)  # Добавляем в очередь
        while queue.qsize() >= MAX_QUEUE_SIZE:
            await asyncio.sleep(1)


async def main():
    proxy_list = [
        {"server": "http://45.153.52.106:63452", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://45.132.38.70:64582", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://45.146.171.213:63692", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://91.220.206.188:62146", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://194.226.115.57:62210", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://195.208.86.106:63376", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://195.208.91.14:62704", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://85.143.48.253:62458", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://194.226.126.60:62400", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://212.60.7.221:63968", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://45.92.172.172:62024", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://213.226.103.168:62434", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://194.156.105.39:63514", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://193.232.222.86:63724", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://176.103.86.68:64738", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://212.192.58.187:62820", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://45.147.15.196:62842", "username": "JKThSkEu", "password": "whh3hUFn"},
        {"server": "http://195.208.95.129:64640", "username": "JKThSkEu", "password": "whh3hUFn"}
    ]

    queue = multiprocessing.Queue()  # ✅ Используем multiprocessing.Queue()

    producer_task = asyncio.create_task(producer(queue))
    processes = []
    for i in range(MAX_WORKERS):
        proxy = proxy_list[i % len(proxy_list)]
        p = multiprocessing.Process(target=worker, args=(queue, proxy))
        p.start()
        processes.append(p)

    await producer_task

    for _ in processes:
        queue.put(None)

    for p in processes:
        p.join()


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    print(time.time() - start_time)
