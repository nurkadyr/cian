import requests

PROXY = "http://JKThSkEu:whh3hUFn@91.230.38.134:62090"  # Заменить на свои данные

proxies = {
    "http": PROXY,
    "https": PROXY
}

# Запрос к API, который определяет местоположение
response = requests.get("http://ip-api.com/json", proxies=proxies)
print(response.json())