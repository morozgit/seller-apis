import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров Яндекс-Маркета.

        Делает API-запрос на сайт Яндекс-Маркета. Информация возвращается для каждого товара Яндекс-Маркета.
        Например, размер упаковки и вес товара.
        Фильтр настроен на отображения всех товаров с лимитом 200.

        Аргументы:
            page (str): страница с товаром.
            campaign_id (str): Идентификатор поставщика.
            access_token (str): Токен полученный на Яндекс-Маркете.

        Возвращаемое значение:
            str: Строка с информацией о товарах в каталоге

        Пример корректного исполнения функции:
            Корректный ответ от сайта Яндекс-Маркета - 200 и получение ответа в json формате

        Пример некорректного исполнения функции:
            Корректный ответ от сайта Яндекс-Маркета - 400

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
    """Обновить остатки.

        Передает данные об остатках товаров на витрине.

        Аргументы:
            stocks (list): Список с остатками.
            campaign_id (str): Идентификатор поставщика.
            access_token (str): Токен полученный на Яндекс-Маркете.

        Возвращаемое значение:
            Пустой ответ

        Пример корректного исполнения функции:
            Корректный ответ от сайта Яндекс-Маркета - 200 Пустой ответ

        Пример некорректного исполнения функции:
            Не корректный ответ от сайта Яндекс-Маркета - 400

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
    """Установить цены товаров.

        Устанавливает цены на товары в магазине.

        Аргументы:
            prices (list): Список с ценами.
            campaign_id (str): Идентификатор поставщика.
            access_token (str): Токен полученный на Яндекс-Маркете.

        Возвращаемое значение:
            Яндекс-Маркет принял информацию о новых ценах

        Пример корректного исполнения функции:
            Корректный ответ- 200 Яндекс-Маркет принял информацию о новых ценах

        Пример некорректного исполнения функции:
            Не корректный ответ от сайта Яндекс-Маркета - 400

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
    """Получить артикулы товаров Яндекс маркета.

        Формерует список с информацией о товарах в каталоге и добавляет к нему SKU

        Аргументы:
            campaign_id (str): Идентификатор поставщика.
            market_token (str): Токен полученный на Яндекс-Маркете.

        Возвращаемое значение:
            Список предложений

        Пример корректного исполнения функции:
           Не пустой список предложений

        Пример некорректного исполнения функции:
            Пустой список предложений

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
    """Создать запасы.

        Создает пустой список для записи запасов.
        В цикле по остаткам, которые вернула функция download_stock() с сайта Casio, проверяются,
        если количество товара больше 10 то запас 100,
        если количество товара 1, то запаса нет. Иначе запас равен количеству товара.
        В зависимости от условий в список запасов добавляется артикул, идентификатор, где хранится товар на складе
        маркетплейса или складе поставщика. Также добавляется количество товара и время обновления.
        Также в цикле по предложениям в список запасов добавляется предложение с идентификатором,
        где хранится товар на складе маркетплейса или складе поставщика и время обновления.

        Аргументы:
            watch_remnants (dict): Словарь с остатками.
            offer_ids (list): Список товаров магазина.
            warehouse_id (str): Идентификатор, где хранится товар на складе маркетплейса или складе поставщика.

        Возвращаемое значение:
            Список с запасами товара

        Пример корректного исполнения функции:
            Заполненный список

        Пример некорректного исполнения функции:
            Пустой список

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
    """Обновить цену.

        Получает от функции get_offer_ids() список товара.
        Получает от функции create_prices() список с ценами товара.
        Разделяет в цикле цену с шагом 500 и получает от функции update_price() ответ в json формате

        Аргументы:
            watch_remnants (dict): Словарь с остатками.
            campaign_id (str): Идентификатор поставщика.
            market_token (str): Идентификатор продавца.

        Возвращаемое значение:
            Обновленный список с ценами

        Пример корректного исполнения функции:
            В списке обновились цены на товар

        Пример некорректного исполнения функции:
            В списке старые цены на товар

        """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Обновить остатки.

        Получает от функции get_offer_ids() список товара.
        Получает от функции create_stocks() список с остатками товара.
        Разделяет в цикле цену с шагом 2000 и получает от функции update_price() ответ в json формате

        Аргументы:
            watch_remnants (dict): Словарь с остатками.
            campaign_id (str): Идентификатор поставщика.
            market_token (str): Идентификатор продавца.
            warehouse_id (str): Идентификатор, где хранится товар на складе маркетплейса или складе поставщика.

        Возвращаемое значение:
            Обновленный список с остатками товара и список товара на который есть запас

        Пример корректного исполнения функции:
            В списке обновились остатки товара

        Пример некорректного исполнения функции:
            В списке ничего не изменилось

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
