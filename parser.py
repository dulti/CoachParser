from datetime import datetime
import io
import random
import pandas as pd
import requests
# Сергиев Посад-1
from pandas import CategoricalDtype

from postgresql import Database

db = Database()
db.connect()

unit_name = 'Сергиев Посад-1'

db.execute("""SELECT unit_id FROM units WHERE unit_name = %s;""", (unit_name,))

unit_id = db.fetch(one=True)[0]  # 244

login = 'itdevelop'  # вынести во внешний файл
password = 'n75VJ737' # вынести во внешний файл
start_date = datetime(year=2022, month=4, day=7)
end_date = datetime(year=2022, month=4, day=14)
TIMEOUT_CONNECT = 60

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 6.3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 YaBrowser/17.6.1.749 Yowser/2.5 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 YaBrowser/18.3.1.1232 Yowser/2.5 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 10_0 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 YaBrowser/17.4.3.195.10 Mobile/14A346 Safari/E7FBAF',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36']

AUTH_PAYLOAD = {'State': '',
                'fromSiteId': '',
                'CountryCode': 'Ru',
                'login': login,
                'password': [password, 'ltr']}

HEADERS_AUTH = {'origin': 'https://auth.dodopizza.ru', 'referer': 'https://auth.dodopizza.ru/Authenticate/LogOn'}

# добавляем в словарь HEADERS_AUTH случайно выбранное значение user_agent
HEADERS_AUTH['User-Agent'] = random.choice(USER_AGENTS)

# создаем новую сессию
session = requests.Session()

# отправляем запрос post
response = session.post('https://auth.dodopizza.ru/Authenticate/LogOn', data=AUTH_PAYLOAD, headers=HEADERS_AUTH,
                        allow_redirects=True, timeout=TIMEOUT_CONNECT)

if response.ok:
    client_response = session.post('https://officemanager.dodopizza.ru/Reports/ClientsStatistic/Export',
                                    data={'unitsIds': unit_id, 'beginDate': start_date.strftime('%d.%m.%Y'),
                                          'endDate': end_date.strftime('%d.%m.%Y'),
                                          'hidePhoneNumbers': 'false'})

    result = io.BytesIO(client_response.content)

    df = pd.read_excel(result, skiprows=10, dtype='object')

    # Добавляем категорийный столбец first_order_types, который будет хранить значения Направления первого заказа
    order_type = CategoricalDtype(categories=['Доставка', 'Самовывоз', 'Ресторан'], ordered=True)
    df['first_order_type'] = df['Направление первого заказа'].astype(order_type).cat.codes

    # Дата первого заказа лежит в переданном диапазоне, который совпадает с диапазоном выгрузки
    df = df.drop(df[df['Дата первого заказа'] < start_date].index)
    df = df.drop(df[df['Дата последнего заказа'] >= end_date].index)

    # Отдел соответствует отделу первого И последнего заказа ! НО: МНОГО ПИЦЦЕРИЙ В ГОРОДЕ TODO
    df = df.drop(df[df['Отдел первого заказа'] != unit_name].index)
    df = df.drop(df[df['Отдел последнего заказа'] != unit_name].index)

    # Номер начинается на +79
    df = df.drop(df[~df['№ телефона'].str.startswith('+79')].index)

    for row in df.iterrows():
        db.execute("INSERT INTO clients (country_code, unit_id, phone, first_order_datetime, first_order_city, "
                   "last_order_datetime, last_order_city, first_order_type, sms_text, sms_text_city, ftp_path_city) "
                   "VALUES (%(country_code)s, %(unit_id)s, %(phone)s, %(first_date)s, %(first_city)s, %(last_date)s,"
                   "%(last_city)s, %(first_type)s, %(text)s, %(text_city)s, %(path_city)s)"
                   "ON CONFLICT DO NOTHING",
                   {'country_code': 'ru', 'unit_id': unit_id, 'phone': row[1]['№ телефона'],
                    'first_date': row[1]['Дата первого заказа'], 'first_city': row[1]['Отдел первого заказа'],
                    'last_date': row[1]['Дата последнего заказа'], 'last_city': row[1]['Отдел последнего заказа'],
                    'first_type': row[1]['first_order_type'], 'text': '', 'text_city': '', 'path_city': ''})

db.close()
