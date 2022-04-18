import requests
import pandas as pd

import config

# вынести строки (адреса) в .env
# либо в отдельный файл urls.xml \ urls.yaml

# Создаем новую сессию
from postgresql import Database

public_api_address = 'https://publicapi.dodois.io/ru/api/v1/unitinfo'
session = requests.Session()
result = session.get(public_api_address)
# Читаем значение json-объекта
json_result = result.json()

db = Database()
db.connect()

for unit in json_result:
    # unit - это словарь
    if unit['Approve'] and not unit['IsTemporarilyClosed']:
        query = """INSERT INTO units (country_code, unit_id, uuid, unit_name) VALUES (%(code)s, %(id)s, %(uuid)s, %(name)s)
        ON CONFLICT (country_code, unit_id) DO UPDATE
        SET uuid = %(uuid)s, unit_name = %(name)s"""
        params = {'code': 'ru', 'id': unit['Id'], 'uuid': unit['UUId'], 'name': unit['Name']}
        db.execute(query, params)

db.execute('SELECT * FROM units;')

units = pd.DataFrame(db.fetch())
units.to_csv(config.EXPORT_PATH + 'units.csv', encoding='utf8')

db.close()
