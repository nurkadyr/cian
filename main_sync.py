import asyncio
import base64
import datetime
import json
import multiprocessing
import os
import random
import time
import uuid
from io import BytesIO

from PIL import Image
from curl_cffi import requests
from playwright.sync_api import sync_playwright
from pymongo import MongoClient

from mongo import insert_photo, insert_html_data, insert_screenshot, update_unique_status
from ms import insert_product, insert_product_files, get_connection, is_url_exists

# ua = UserAgent(os="Windows")
# ua.min_version = 119
# print(ua.firefox)
# exit()
MAX_QUEUE_SIZE = 20
MAX_WORKERS = 10


def parse_url(page, page_url, proxy_url, db_html, db_photos, db_screenshots, conn):
    success, url, html_id, image_id, data = scrape_page(page, page_url, proxy_url, db_html, db_photos, db_screenshots,
                                                        proxy_url)
    if success:
        product_id = insert_product(
            conn,
            source=1, category=2, segment_on_source=3, vehicle_sub_type=4, region=5,
            deal_type=6, type_=7,
            brand=100, url=url, is_from_archive=0,
            creation_date=datetime.datetime.utcnow(), status=1, mongo_id=html_id,
            task_guid="550e8400-e29b-41d4-a716-446655440000",
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


def scrape_page(page, page_url, proxy, db_html, db_photos, db_screenshots, proxy_url):
    try:
        response = page.goto(page_url, timeout=120000, wait_until="load")

        if response.status == 404:
            return False, None, None, None, None
        if response.status != 200:
            print(response.status, page_url, proxy_url, datetime.datetime.now())
            return False, page_url, None, None, None
        date_element = page.locator('[data-testid="metadata-updated-date"] span')
        text = date_element.text_content(timeout=11000)
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
        page.locator("body").screenshot(path=screenshot_path, type="jpeg", quality=25)

        with open(screenshot_path, "rb") as img_file:
            base64_image = base64.b64encode(img_file.read()).decode("utf-8")
        os.remove(screenshot_path)
        try:
            json_data = page.locator('script[type="application/ld+json"]').inner_text(timeout=10000)
            images = json.loads(json_data).get("image", [])
        except Exception as e:
            print(e, datetime.datetime.now())
            images = []

        data = {
            "region": (page.locator('[itemprop="name"]').nth(0).inner_text()).split(" ")[-1]
        }

        html = page.content()
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
        print(e, page_url, proxy_url, datetime.datetime.now())
        return False, page_url, None, None, None


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

        response = requests.get(url, headers=headers, impersonate="chrome", timeout=60, verify=False)

        if response.status_code == 200:
            img = Image.open(BytesIO(response.content))
            buffer = BytesIO()
            img.save(buffer, format="JPEG", quality=25)
            image_base64 = base64.b64encode(buffer.getvalue()).decode("utf-8")

            return url, str(insert_photo(db_photos, image_base64).inserted_id)
        else:
            print(f"Failed to download {url}, status: {response.status_code}", datetime.datetime.now())

    except Exception as e:
        print(f"Error downloading {url}: {e}", datetime.datetime.now())


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
                        if count % 1000 == 0:
                            print(count, datetime.datetime.now())

                        if count < 34470:
                            continue
                        if not is_url_exists(conn, url):
                            yield url
    conn.close()


def get_browser(p, proxy_url):
    args = []
    # args.append("--disable-blink-features=AutomationControlled")
    args.append("--disable-webrtc")
    args.append("--disable-dev-shm-usage")
    args.append('--no-first-run')
    args.append('--force-webrtc-ip-handling-policy')
    firefox_prefs = {
        "media.peerconnection.enabled": False,  # Отключает WebRTC
        "media.navigator.enabled": False,  # Отключает WebRTC API
        "media.peerconnection.ice.default_address_only": True,  # Не отправляет локальный IP
        "media.peerconnection.ice.no_host": True,  # Запрещает WebRTC запрашивать локальный IP
        "privacy.resistFingerprinting": False,  # Отключаем защиту от подмены TimeZone
        "timezone.override": "Europe/Moscow",  # Принудительно ставим тайм-зону
    }
    return p.firefox.launch(
        headless=True,
        args=args,
        ignore_default_args=["--enable-automation"],
        proxy=proxy_url,
        firefox_user_prefs=firefox_prefs
    )


def get_page(browser):
    context = browser.new_context(
        timezone_id="Europe/Moscow",
        permissions=['geolocation'],
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
        viewport={"width": random.choice([1200, 1300, 1400]), "height": random.choice([1600, 1500, 1400])},
        extra_http_headers={
            "accept-language": "en-US,en;q=0.9",
            "referer": "https://www.google.com/",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Dest": "document",
            "Sec-Ch-Ua-Mobile": "?0",
            "upgrade-insecure-requests": "1"
        }
    )
    return context.new_page()


def worker(queue, proxy_url):
    # client = MongoClient("mongodb://localhost:27017/")
    client = MongoClient("mongodb://192.168.1.59:27017/")
    db_html = client["htmlData2"]
    db_photos = client["adsPhotos2"]
    db_screenshots = client["adsScreenshots2"]
    conn = get_connection()

    with sync_playwright() as p:
        error_count = 0
        parse_count = 1
        browser = get_browser(p, proxy_url)
        page = get_page(browser)
        while True:
            if parse_count % 50 == 0:
                browser.close()
                browser = get_browser(p, proxy_url)
                page = get_page(browser)
            urls_chunk = queue.get()
            print("start", urls_chunk, queue.qsize(), datetime.datetime.now())
            if urls_chunk is None:
                print("worker end", datetime.datetime.now())
                break  # Завершаем процесс

            success, url = parse_url(page, urls_chunk, proxy_url, db_html, db_photos, db_screenshots, conn)
            print("success", success, datetime.datetime.now())
            if not success and url is not None:
                queue.put(url)
                error_count += 1
                if error_count == 3:
                    browser.close()
                    browser = get_browser(p, proxy_url)
                    page = get_page(browser)
                if error_count == 6:
                    break
            if success:
                error_count = 0
        browser.close()
    conn.close()


async def producer(queue):
    """Наполняет очередь URL, чтобы не перегружать память."""
    for i in extract_urls_from_folder():
        queue.put(i)  # Добавляем в очередь
        while queue.qsize() >= MAX_QUEUE_SIZE:
            await asyncio.sleep(1)


async def main():
    proxy_list = [
        {'server': 'http://91.230.38.134:62090', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        {'server': 'http://91.221.39.231:62104', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        {'server': 'http://91.220.229.74:64080', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        {'server': 'http://45.146.24.2:63348', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        {'server': 'http://77.83.80.22:63366', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        {'server': 'http://45.91.239.80:63076', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        {'server': 'http://45.132.38.19:61936', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        {'server': 'http://45.149.135.251:62188', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        {'server': 'http://45.150.61.124:62232', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        {'server': 'http://45.139.126.33:63682', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        # {'server': 'http://45.146.230.22:63922', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        # {'server': 'http://45.141.197.111:64062', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        # {'server': 'http://91.206.68.144:63518', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        # {'server': 'http://176.103.93.29:64892', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        # {'server': 'http://194.226.166.247:62364', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        # {'server': 'http://194.226.20.194:64998', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        # {'server': 'http://62.76.155.119:62868', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        # {'server': 'http://85.142.66.146:63570', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        # {'server': 'http://85.142.254.166:62854', 'username': 'JKThSkEu', 'password': 'whh3hUFn'},
        # {'server': 'http://154.209.208.230:62840', 'username': 'JKThSkEu', 'password': 'whh3hUFn'}
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
    # client = MongoClient("mongodb://localhost:27017/")
    # db_html = client["htmlData2"]
    # db_photos = client["adsPhotos2"]
    # db_screenshots = client["adsScreenshots2"]
    # conn = get_connection()
    # success, url = parse_url(
    #     "https://ekb.cian.ru/sale/flat/305725065/",
    #     {
    #         'server': 'http://194.226.115.57:62210',
    #         'username': 'JKThSkEu',
    #         'password': 'whh3hUFn'
    #     },
    #     db_html,
    #     db_photos,
    #     db_screenshots,
    #     conn
    # )
    # conn.close()
    print(time.time() - start_time, datetime.datetime.now())
