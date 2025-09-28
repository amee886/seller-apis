import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список товаров из Яндекс.Маркета по ID кампании.

    Args:
        page(str): Токен для постраничной навигации.
            Пустая строка "" означает первую страницу.
        campaign_id(str):  Индефикатор кампании в Яндекс.Маркете.
        access_token(str): Токен для авторизации.

    Returns:
        dict: Словарь с результатами, в котором есть:
            - offerMappingEntries (list): список товаров.
            - paging (dict): данные дл яперехода на следующую страницу.
    Raises:
        requests.exceptions.HTTpError: Если API вернул ошибку (например, неверный токен).
        requests.exceptions.ConnectionError: Если нет соеденения с интернетом.
        requests.exceptions.Timeout: Если запрос превысил лимит времени.

    Examples:
        Корректное использование:
            >>> result = get_product_list("", "123456", "ya29.a0AfH6SM...")
            >>> isinstance(result, dict)
            True

        Некорректное использование:
        >>> result = get_product_list("", "123456", "WRONG_TOKEN")
        requests.exceptions.HTTPError: 401 Client Error
    
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Обновляет остатки товаров в Яндекс.Маркете.

    Args:
        stocks(list[dict]): Список словарей с остатками.
        campaign_id(str):  Индефикатор кампании в Яндекс.Маркете.
        access_token(str): Токен для авторизации.

    Returns:
        dict: Ответ API с результатами обновления.

    Raises:
        requests.exceptions.HTTPError: Ошибка API при обновлении остатков.

    Examples:
        Корректное использование:
            >>> update_stocks([{"sku": "123", "warehouseId": "1", "items": [{"count": 10, "type": "FIT", "updatedAt": "2025-09-15T00:00:00Z"}]}], "123456", "token")
            {"status": "OK"}
        Некорректное использование:
            >>> update_stocks([], "123456", "token")
            {"status": "ERROR", "message": "Empty stock list"}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Обновляет цены на товары в Яндекс.Маркете.

    Args:
        prices(list[dict]): Список с словарей с новыми ценами.
        campaign_id(str): Индефикатор кампании в Яндекс.Маркете.
        access_token(str): Токен для авторизации.

    Returns:
        dict: Ответ API с результатами обновления.

    Raises:
        requests.exceptions.HTTPError: Ошибка API при обновлении цен.

    Examples:
        Корректное использование:
             >>> update_price([{"id": "123", "price": {"value": 1000, "currencyId": "RUR"}}], "123456", "token")
            {"status": "OK"}
        Некорректное использование:
            >>> update_price([{"id": "123", "price": {"value": -500, "currencyId": "RUR"}}], "123456", "token")
            {"status": "ERROR", "message": "Invalid price"}

        
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркета

    Args:
        campaign_id(str): Индефикатор кампании в Яндекс.Маркете.
        market_token(str): OAuth-токен.

    Retursns:
        list[str]: Список артикулов.
    Raises:
        requests.exceptions.HTTPError: Если API вернул ошибку.
    Examples:
        Корректное использование:
            >>> offer_ids = get_offer_ids("123456", "token")
            >>> isinstance(offer_ids, list)
            True
        Некорректное использование:
            >>> get_offer_ids("WRONG_ID", "token")
            requests.exceptions.HTTPError: 404 Client Error
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Создает список остатков для загрузки на Янлекс.Маркет.

    Args: 
        watch_remnants(list[str]): Данные о товарах(остатки).
        offer_ids(list[str]): Список артикулов.
        warehouse_id(str): Индефикатор склада.
    Returns:
        list[dict]: Список остатков в формате API.
    Examples:
        Корректное использование:
            >>> remnants = [{"Код": "123", "Количество": "5"}]
            >>> create_stocks(remnants, ["123"], "1")
            [{"sku": "123", "warehouseId": "1", "items": [{"count": 5, "type": "FIT", "updatedAt": "..."}]}]
        Некорректное использование:
             >>> create_stocks([{"Количество": "5"}], ["123"], "1")
            []
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Сформировать список цен для загрузки на Яндекс.Маркет.

    Args:
        watch_remnants(list[str]): Данные о товарах(остатки).
        offer_ids(list[str]): Список артикулов.
        
    Returns:
        list[dict]: Список цен в формате API.
        
    Examples:
        Корректное использование:
            >>> remnants = [{"Код": "123", "Цена": "1'000.00 руб."}]
            >>> create_prices(remnants, ["123"])
            [{"id": "123", "price": {"value": 1000, "currencyId": "RUR"}}]

        Некорректное использование:
             >>> remnants = [{"Код": "123", "Цена": ""}]
            >>> create_prices(remnants, ["123"])
            ValueError: invalid literal for int() with base 10: ''
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Загружает цены на Яндекс.Маркет.

    Args:
        watch_remnants(list[str]): Данные о товарах(остатки).
        campaign_id(str): Индефикатор кампании в Яндекс.Маркете.
        market_token(str): OAuth-токен.

    Returns:
        list[dict]: Список цен.
    Examples:
        Корректное использование:
            >>> await upload_prices([{"Код": "123", "Цена": "1000"}], "123456", "token")
            [{"id": "123", "price": {"value": 1000, "currencyId": "RUR"}}]
        Некорректное использование:
            >>> await upload_prices([], "123456", "token")
            []
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Загружает остатки на Яндекс.Маркет.

    Args:
        watch_remnants(list[str]): Данные о товарах(остатки).
        campaign_id(str): Индефикатор кампании в Яндекс.Маркете.
        market_token(str): OAuth-токен.
        warehouse_id(str): Индефикатор склада.

    Returns:
        tuple[list[dict], list[dict]]: 
            not_empty (товары с остатками > 0), 
            stocks (все товары).
    Examples:
        Корректное использование:
            >>> await upload_stocks([{"Код": "123", "Цена": "1000"}], "123456", "token","1")
            ([{"sku": "123", ...}], [{"sku": "123", ...}])
        Некорректное использование:
            >>>await upload_stocks([{"Код": "123", "Цена": "1000"}], "123456", "token","1")
            ([],[])
    """    
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """
    Основная функция: Загружает остатки товаров с сайта Casio и обновляет данные в Яндекс.Маркете.

    Raises:
        requests.exceptions.ReadTimeout: Если превышено время ожидания ответа.
        requests.exceptions.ConnectionError: Если нет соединения с API.
        Exception: Любая другая ошибка.

    Examples:
        Корректно:
        >>> main()  # запускает процесс обновления остатков и цен

        Некорректно (не задан токен окружения):
        >>> main()
        KeyError: 'MARKET_TOKEN'
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
