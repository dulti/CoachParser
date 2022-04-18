import requests, sys, io, re, random
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta


INTERVAL_DELTA = 92  # блоками по сколько дней выгружать
TIMEOUT_CONNECT = 10  # таймаут для страницы авторизации Додо ИС
TIMEOUT_REQUEST = 60  # таймаут для запроса excel-файла из Додо ИС


def bs_preprocess(html: str) -> str:
    """
    remove distracting whitespaces and newline characters
    from https://groups.google.com/g/beautifulsoup/c/F3sdgObXbO4?pli=1
    """
    pat = re.compile('(^[\s]+)|([\s]+$)', re.MULTILINE)
    html = re.sub(pat, '', html)  # remove leading and trailing whitespaces
    html = re.sub('\n', ' ', html)  # convert newlines to spaces
    # this preserves newline delimiters
    html = re.sub('[\s]+<', '<', html)  # remove whitespaces before opening tags
    html = re.sub('>[\s]+', '>', html)  # remove whitespaces after closing tags
    return html


def main(argv):

    if len(argv) != 3:
        print('Использование: python.exe имя_скрипта имя_файла_с_параметрами имя_выходного_файла')
        return

    try:
        inp_df = pd.read_excel(argv[1])
    except PermissionError:
        print(f'Вероятно, файл {argv[1]} открыт в другой программе. Закройте файл и перезапустите скрипт.')
        return
    except FileNotFoundError:
        print('Имя файла указано неверно! Попробуйте еще раз.')
        return
    shops = {}
    for row in inp_df.iterrows():
        shops[row[1]['Город']] = (row[1]['Логин'], row[1]['Пароль'], row[1]['FirstDate'], 
                                  row[1]['LastDate'], row[1]['Группа'])
    
    results = {}
    print('Запрашиваю данные...')
    for city, (login, password, start_date, end_date, group) in shops.items():
        print(f'Идет запрос данных для города {city}...')
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
                        
        HEADERS_AUTH = {'origin': 'https://auth.dodopizza.ru',
                        'referer': 'https://auth.dodopizza.ru/Authenticate/LogOn'}
        
        # присваиваем переменной user_agent значение случайно выбранного элемента из списка                
        user_agent = random.choice(USER_AGENTS)

        # добавляем в словарь HEADERS_AUTH значение user_agent
        HEADERS_AUTH['User-Agent'] = user_agent

        # создаем новую сессию
        session = requests.Session()

        # отправляем запрос post
        response = session.post('https://auth.dodopizza.ru/Authenticate/LogOn', data=AUTH_PAYLOAD, headers=HEADERS_AUTH, allow_redirects=True, timeout=TIMEOUT_CONNECT)
      
        if response.ok:
            
            r = session.get('https://officemanager.dodopizza.ru/Reports/ClientsStatistic', timeout=TIMEOUT_CONNECT)
            soup = BeautifulSoup(bs_preprocess(r.text), 'html.parser')
                      
            sel = soup.find('select', id='unitsIds')
            optgroup = sel.find('optgroup')
            if optgroup:
                unit_ids = [child['value'] for child in sel.optgroup.children]
            else:
                unit_ids = sel.option['value']
                
            # divide the date interval into three months and iterate
            
            interval_start_date = start_date
            
            iter = 1
            
            dfs = []
            
            while interval_start_date < end_date:
                                
                interval_end_date = min(interval_start_date + timedelta(days=INTERVAL_DELTA),
                                        end_date)
                                        
                print(f'Идет запрос блока данных №{iter} ({interval_start_date.strftime("%d.%m.%Y")} - {interval_end_date.strftime("%d.%m.%Y")})')
                iter += 1
            
                data = {'unitsIds': unit_ids, 'beginDate': interval_start_date.strftime('%d.%m.%Y'), 
                        'endDate': interval_end_date.strftime('%d.%m.%Y'), 'hidePhoneNumbers': False}
                
                r = session.post('https://officemanager.dodopizza.ru/Reports/ClientsStatistic/Export',
                                  data=data, timeout=TIMEOUT_REQUEST)
                
                interval_start_date += timedelta(days=INTERVAL_DELTA + 1)
                                  
                if r.ok:
                    print('Идет чтение данных...')
                    try:
                        with io.BytesIO(r.content) as fh:
                            
                            df = pd.read_excel(fh, skiprows=10, dtype=str)
                    
                    except:
                        print('Неизвестная ошибка Додо ИС! Попробуйте еще раз!')
                        
                    print('Идет обработка данных...')
                    df.drop(df[~df['№ телефона'].str.startswith('+79')].index, inplace=True)
                    
                    df.drop(df[~df['Отдел первого заказа'].str.startswith(city)].index, inplace=True)
                    df.drop(df[~df['Отдел последнего заказа'].str.startswith(city)].index, inplace=True)
                    
                    df['Дата первого заказа'] = pd.to_datetime(df['Дата первого заказа'])
                    df['Дата последнего заказа'] = pd.to_datetime(df['Дата последнего заказа'])
                    
                    df.drop(df[df['Дата последнего заказа'] > end_date].index, inplace=True)
                    
                    df['Кол-во заказов'] = df['Кол-во заказов'].astype(int)
                    df['Сумма заказа'] = df['Сумма заказа'].astype(int)
                    
                    df['Имя клиента'] = "'" + df['Имя клиента']
                    
                    dfs.append(df)
                    
                else:
                    print('Неизвестная ошибка Додо ИС! Попробуйте позже.')
                    return
                    
            df = pd.concat(dfs, ignore_index=True)
            del dfs
            df_cols = df.columns
                    
            group_cols = ['№ телефона', 'Отдел первого заказа', 'Дата первого заказа', 'Отдел последнего заказа', 'Дата последнего заказа', 'Направление первого заказа']
            df = df.groupby(group_cols).agg({'Кол-во заказов': 'sum', 'Сумма заказа': 'sum', 'Имя клиента': 'last', 'Отдел': 'first'}).reset_index()
            df = df[df_cols]  # reorder columns back
                    
            if group in results.keys():
                results[group] = pd.concat([results[group], df.copy()], ignore_index=True)
            else:
                results[group] = df.copy()
                                
    print('Сохраняем данные...')
    fnames = []
    for group, df in results.items():
        fname = f'{argv[2]}_group{group}.xlsx'
        fnames.append(fname)
        df.to_excel(fname, index=False)
    print('Готово!')
    print(f'Файлы сохранены: {", ".join(fnames)}')


if __name__ == '__main__':
    main(sys.argv)
