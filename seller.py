import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон.

    Делает API-запрос на сайт озон для получения списка товаров. Для корректного запроса необходим идентификатор клиента и идентификатор продавца.
    Фильтр настроен на отображения всех товаров с лимитом 1000.

    Аргументы:
        last_id (str): Последний идентификатор.
        client_id (str): Идентификатор клиента.
        seller_token (str): Идентификатор продавца.

    Возвращаемое значение:
         list: Список с информацией о товарах в каталоге

    Пример корректного исполнения функции:
        Корректный ответ от сайта озон - 200 и получение ответа в json формате

    Пример некорректного исполнения функции:
        Корректный ответ от сайта озон - 400

    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон.

    Формерует список с информацией о товарах в каталоге и добавляет к нему артикул

    Аргументы:
        client_id (str): Идентификатор клиента.
        seller_token (str): Идентификатор продавца.

    Возвращаемое значение:
        Список предложений

    Пример корректного исполнения функции:
       Не пустой список предложений

    Пример некорректного исполнения функции:
        Пустой список предложений

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
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров.

    Делает API-запрос на сайт озон. Для корректного запроса необходим идентификатор клиента, идентификатор продавца,
    список с ценами

    Аргументы:
        prices (list): Список с ценами.
        client_id (str): Идентификатор клиента.
        seller_token (str): Идентификатор продавца.

    Возвращаемое значение:
        Ответ от сайта в json формате

    Пример корректного исполнения функции:
        Корректный ответ от сайта озон - 200 и получение ответа в json формате

    Пример некорректного исполнения функции:
        Корректный ответ от сайта озон - 400

    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки.

    Делает API-запрос на сайт озон. Для корректного запроса необходим идентификатор клиента,
    идентификатор продавца и список с остатками.

    Аргументы:
        stocks (list): Список с остатками.
        client_id (str): Идентификатор клиента.
        seller_token (str): Идентификатор продавца.

    Возвращаемое значение:
        Ответ от сайта в json формате

    Пример корректного исполнения функции:
        Корректный ответ от сайта озон - 200 и получение ответа в json формате

    Пример некорректного исполнения функции:
        Корректный ответ от сайта озон - 400

    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio.

    Скачивает и распаковывает архив с сайта casio. Остатки хранятся в таблице формата .xls.
    Из файла берет столбец с остатками товара и записывает в словарь.
    После всего удаляет файл, скаченный ранее с сайта

    Возвращаемое значение:
        Словарь с остатками товара

    Пример корректного исполнения функции:
        Заполненный словарь

    Пример некорректного исполнения функции:
        Пустой словарь

    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать запасы.

        Создает пустой список для записи запасов.
        В цикле по остаткам, которые вернула функция download_stock() с сайта Casio, проверяются,
        если количество товара больше 10 то запас 100,
        если количество товара 1 то запаса нет. Иначе запас равен количеству товара.
        В зависимости от условий список запасов пополняется артикулом и количеством товара.
        Также в цикле по предложениям в список запасов добавляется предложение без запаса.

        Аргументы:
            watch_remnants (dict): Словарь с остатками.
            offer_ids (list): Список товаров магазина.

        Возвращаемое значение:
            Список с запасами товара

        Пример корректного исполнения функции:
            Заполненный список

        Пример некорректного исполнения функции:
            Пустой список

        """
    # Уберем то, что не загружено в seller
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
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать цену.

        Создает пустой список для записи цены.
        В цикле по остаткам, которые вернула функция download_stock().
        Цена будет установлена в том случае, если в списке предложений найдется код товара из списка остатков.

        Аргументы:
            watch_remnants (dict): Словарь с остатками.
            offer_ids (list): Список товаров магазина.

        Возвращаемое значение:
            Список с ценами товара

        Пример корректного исполнения функции:
            Заполненный список

        Пример некорректного исполнения функции:
            Пустой список

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
    """Преобразовать цену.

    Отрезает от строки с ценой дробную часть и оставляет в целой части только цифры.

    Аргументы:
        price (str): Цена.

    Возвращаемое значение:
        Строка целой части числа без лишних знаков и валюты.

    Пример корректного исполнения функции:
        5'990.00 руб. -> 5990

    Пример некорректного исполнения функции:
        2'530.00 руб. -> .00 руб

    """

    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов.

    Отрезает от строки с ценой дробную часть и оставляет в целой части только цифры.

    Аргументы:
        lst (list): Список, который необходимо разделить.
        n (int): с каким шагом необходимо разделить.

    Возвращаемое значение:
        Список разделенный на части с установленным шагом.

    Пример корректного исполнения функции:
        Список разделен на части с установленным шагом

    Пример некорректного исполнения функции:
        Список разделен на части с не установленным шагом или пуст

    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Обновить цену.

        Получает от функции get_offer_ids() список товара.
        Получает от функции create_prices() список с ценами товара.
        Разделяет в цикле цену с шагом 1000 и получает от функции update_price() ответ в json формате

        Аргументы:
            watch_remnants (dict): Словарь с остатками.
            client_id (str): Идентификатор клиента.
            seller_token (str): Идентификатор продавца.

        Возвращаемое значение:
            Обновленный список с ценами

        Пример корректного исполнения функции:
            В списке обновились цены на товар

        Пример некорректного исполнения функции:
            В списке старые цены на товар

        """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Обновить остатки.

        Получает от функции get_offer_ids() список товара.
        Получает от функции create_stocks() список с остатками товара.
        Разделяет в цикле цену с шагом 100 и получает от функции update_price() ответ в json формате

        Аргументы:
            watch_remnants (dict): Словарь с остатками.
            client_id (str): Идентификатор клиента.
            seller_token (str): Идентификатор продавца.

        Возвращаемое значение:
            Обновленный список с остатками товара и список товара на который есть запас

        Пример корректного исполнения функции:
            В списке обновились остатки товара

        Пример некорректного исполнения функции:
            В списке ничего не изменилось

        """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
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
