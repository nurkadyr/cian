import requests

PROXY = "http://JKThSkEu:whh3hUFn@176.103.95.57:63822"  # Заменить на свои данные

proxies = {
    "http": PROXY,
    "https": PROXY
}

# Запрос к API, который определяет местоположение
response = requests.get("http://ip-api.com/json", proxies=proxies)
print(response.json())