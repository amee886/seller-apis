import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id: str, client_id: str, seller_token: str) -> dict:
    """
    Получить список товаров магазина Ozon.

    Args:
        last_id (str): ID последнего товара для пагинации (пустая строка для начала).
        client_id (str): ID клиента Ozon Seller.
        seller_token (str): Токен API.

    Returns:
        dict: Результат ответа Ozon API (ключ result с товарами).

    Raises:
        requests.exceptions.RequestException: Если запрос к API не удался.

    Examples:
        Корректно:
        >>> res = get_product_list("", "123", "token123")
        >>> isinstance(res, dict)
        True

        Некорректно (плохой токен):
        >>> get_product_list("", "123", "wrong_token")
        requests.exceptions.HTTPError
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {"Client-Id": client_id, "Api-Key": seller_token}
    payload = {"filter": {"visibility": "ALL"}, "last_id": last_id, "limit": 1000}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json().get("result")


def get_offer_ids(client_id: str, seller_token: str) -> list[str]:
    """
    Получить список артикулов (offer_id) товаров магазина Ozon.

    Args:
        client_id (str): ID клиента Ozon Seller.
        seller_token (str): Токен API.

    Returns:
        list[str]: Список offer_id товаров.

    Raises:
        requests.exceptions.RequestException: Если запрос к API не удался.

    Examples:
        Корректно:
        >>> ids = get_offer_ids("123", "token123")
        >>> isinstance(ids, list)
        True

        Некорректно (плохой токен):
        >>> get_offer_ids("123", "wrong_token")
        requests.exceptions.HTTPError
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    return [product.get("offer_id") for product in product_list]


def update_price(prices: list[dict], client_id: str, seller_token: str) -> dict:
    """
    Обновить цены товаров в Ozon.

    Args:
        prices (list[dict]): Список словарей с ценами. Каждый словарь содержит:
            - offer_id (str): Артикул товара.
            - price (str): Новая цена.
            - old_price (str): Старая цена.
            - currency_code (str): Код валюты (например, "RUB").
            - auto_action_enabled (str): Настройка ("UNKNOWN").
        client_id (str): ID клиента Ozon Seller.
        seller_token (str): Токен API.

    Returns:
        dict: Ответ Ozon API.

    Raises:
        requests.exceptions.RequestException: Если запрос к API не удался.

    Examples:
        Корректно:
        >>> update_price([{"offer_id": "123", "price": "1000", "old_price": "0",
        ... "currency_code": "RUB", "auto_action_enabled": "UNKNOWN"}],
        ... "123", "token123")

        Некорректно (пустой список):
        >>> update_price([], "123", "token123")
        {'result': []}  # API не обновит ничего
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {"Client-Id": client_id, "Api-Key": seller_token}
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list[dict], client_id: str, seller_token: str) -> dict:
    """
    Обновить остатки товаров в Ozon.

    Args:
        stocks (list[dict]): Список словарей:
            - offer_id (str): Артикул товара.
            - stock (int): Количество товара на складе.
        client_id (str): ID клиента Ozon Seller.
        seller_token (str): Токен API.

    Returns:
        dict: Ответ Ozon API.

    Raises:
        requests.exceptions.RequestException: Если запрос к API не удался.

    Examples:
        Корректно:
        >>> update_stocks([{"offer_id": "123", "stock": 10}], "123", "token123")

        Некорректно (неверный ключ словаря):
        >>> update_stocks([{"id": "123", "qty": 10}], "123", "token123")
        requests.exceptions.HTTPError
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {"Client-Id": client_id, "Api-Key": seller_token}
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock() -> list[dict]:
    """
    Скачать и обработать файл `ostatki.xls` с сайта Casio.

    Returns:
        list[dict]: Список словарей, каждая строка Excel как запись:
            - Код (str): Артикул товара.
            - Название (str): Название товара.
            - Количество (str): Остаток (например, ">10" или "1").
            - Цена (str): Цена в формате "5'990.00 руб.".

    Raises:
        requests.exceptions.RequestException: Если файл не скачан.
        FileNotFoundError: Если Excel не найден после распаковки.

    Examples:
        Корректно:
        >>> data = download_stock()
        >>> isinstance(data, list)
        True

        Некорректно (нет доступа к сайту):
        >>> download_stock()
        requests.exceptions.ConnectionError
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file, na_values=None, keep_default_na=False, header=17
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")
    return watch_remnants


def create_stocks(watch_remnants: list[dict], offer_ids: list[str]) -> list[dict]:
    """
    Сформировать список остатков для API Ozon.

    Args:
        watch_remnants (list[dict]): Данные из Excel Casio.
        offer_ids (list[str]): Артикулы товаров в Ozon.

    Returns:
        list[dict]: Список словарей {offer_id, stock}.

    Examples:
        Корректно:
        >>> create_stocks([{"Код": "123", "Количество": ">10"}], ["123"])
        [{'offer_id': '123', 'stock': 100}]

        Некорректно (артикула нет в offer_ids):
        >>> create_stocks([{"Код": "999", "Количество": "5"}], ["123"])
        [{'offer_id': '123', 'stock': 0}]
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants: list[dict], offer_ids: list[str]) -> list[dict]:
    """
    Сформировать список цен для API Ozon.

    Args:
        watch_remnants (list[dict]): Данные из Excel Casio.
        offer_ids (list[str]): Артикулы товаров в Ozon.

    Returns:
        list[dict]: Список словарей {offer_id, price, old_price, currency_code, auto_action_enabled}.

    Examples:
        Корректно:
        >>> create_prices([{"Код": "123", "Цена": "5'990.00 руб."}], ["123"])
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB',
          'offer_id': '123', 'old_price': '0', 'price': '5990'}]

        Некорректно (артикула нет в offer_ids):
        >>> create_prices([{"Код": "999", "Цена": "1000.00 руб."}], ["123"])
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Преобразовать цену из строки в числовой формат.

    Args:
        price (str): Цена в формате "5'990.00 руб."

    Returns:
        str: Числовая строка, например "5990".

    Examples:
        Корректно:
        >>> price_conversion("5'990.00 руб.")
        '5990'

        Некорректно (пустая строка):
        >>> price_conversion("")
        ''
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разделить список на части по n элементов.

    Args:
        lst (list): Список для разбиения.
        n (int): Размер подсписка (>0).

    Yields:
        list: Подсписок длиной до n элементов.

    Raises:
        ValueError: Если n <= 0.

    Examples:
        Корректно:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]

        Некорректно (n=0):
        >>> list(divide([1, 2, 3], 0))
        ValueError: n должно быть больше 0
    """
    if n <= 0:
        raise ValueError("n должно быть больше 0")
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants: list[dict], client_id: str, seller_token: str) -> list[dict]:
    """
    Асинхронно загрузить цены в Ozon.

    Args:
        watch_remnants (list[dict]): Данные Excel Casio.
        client_id (str): ID клиента.
        seller_token (str): Токен API.

    Returns:
        list[dict]: Список цен, отправленных в API.

    Examples:
        Корректно:
        >>> await upload_prices([{"Код": "123", "Цена": "1000 руб."}], "123", "token123")

        Некорректно (нет товаров в Excel):
        >>> await upload_prices([], "123", "token123")
        []
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants: list[dict], client_id: str, seller_token: str) -> tuple[list[dict], list[dict]]:
    """
    Асинхронно загрузить остатки в Ozon.

    Args:
        watch_remnants (list[dict]): Данные Excel Casio.
        client_id (str): ID клиента.
        seller_token (str): Токен API.

    Returns:
        tuple: (список товаров с ненулевыми остатками, все остатки).

    Examples:
        Корректно:
        >>> await upload_stocks([{"Код": "123", "Количество": "5"}], "123", "token123")

        Некорректно (нет товаров):
        >>> await upload_stocks([], "123", "token123")
        ([], [])
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """
    Основная функция: скачать остатки Casio и обновить их в Ozon.

    Raises:
        requests.exceptions.ReadTimeout: Если превышено время ожидания ответа.
        requests.exceptions.ConnectionError: Если нет соединения с API.
        Exception: Любая другая ошибка.

    Examples:
        Корректно:
        >>> main()  # запускает процесс обновления остатков и цен

        Некорректно (не задан токен окружения):
        >>> main()
        KeyError: 'SELLER_TOKEN'
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
