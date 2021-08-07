#!/usr/bin/env python
# coding: utf-8

# In[10]:


from bs4 import BeautifulSoup
from pprint import pprint
import multiprocessing
import lxml
import os
import datetime
import requests
from requests import Session
import sqlite3


# In[11]:


time_now = datetime.datetime.now().strftime("%d.%m.%Y %H-%M-%S")   ### создаем папку для базы данных с названием времени создания
os.mkdir(f"{time_now}")
connection = sqlite3.connect(f'{time_now}/apartments.db')  ### создаем файл БД в папке


# In[12]:


### функция для создания таблиц в БД файле
def create_table_db(time_now, table_name):   
    connection = sqlite3.connect(f'{time_now}/apartments.db')  ### подключаемся к БД
    cursor = connection.cursor()
    cursor.execute(f'''CREATE TABLE {table_name} (
        id            INTEGER PRIMARY KEY
                              UNIQUE,
        address       TEXT,
        square        DECIMAL,
        floor         TEXT,
        year_delivery INTEGER,
        price         INTEGER,
        material      TEXT,
        [price(m2)]   INTEGER
    );
    ''')
    connection.commit()


# In[13]:


headers = { 'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/69.0'}


# In[14]:


def parse_house(link, time_now, table_name):    ### функция парсит описание недвижимости
    url = link
    response = requests.get(url, headers)
    soup = BeautifulSoup(response.content, 'lxml')
    address = soup.find_all('span', class_='ui-kit-link__inner')[-3].text
    square = soup.findAll('span', {'data-test': 'offer-card-param-total-area'})[0].text
    floor = soup.findAll('span', {'data-test': 'offer-card-param-floor'})[0].text
    
    ### year delivery
    year_delivery = '-'
    tags_find = soup.findAll('li', {'class': 'factoid'})
    for tag in tags_find:
        if len(tag.div.text) == 4 and tag.find('div', class_='caption').text == 'год сдачи':
            year_delivery = tag.div.text
        
    price = soup.find('span', class_='price').text
    
    try:
        material = soup.findAll('span', {'data-test': 'offer-card-param-house-material-type'})[0].text
    except:
        material = '-'
        
    price_m2 = soup.findAll('div', {'class': 'part-price'})[0].text   ### цена за м2
    
    
    ### Вывод данных
    
    print(address)
    print(square)
    print(floor)
    print(year_delivery)
    print(price)
    print(material)
    print(price_m2)
    print('---------------------------------')
    
    #### Добавляем недвижимость в БД 
    sqlite_connection = sqlite3.connect(f'{time_now}/apartments.db')
    cursor = sqlite_connection.cursor()

    sqlite_insert_query = f"""INSERT INTO {table_name}
                          ('address', 'square', 'floor', 'year_delivery', 'price', 'material', 'price(m2)')  VALUES (?, ?, ?, ?, ?, ?, ?)"""

    count = cursor.execute(sqlite_insert_query, (address, square, floor, year_delivery, price, material, price_m2))
    sqlite_connection.commit()


# ## Студии

# In[15]:


### создаем Таблицу в БД для студий
create_table_db(time_now, 'studios')

all_links = []
s = Session()     ### Сессия для парсинга
s.headers.update(headers)
for i in range(1, 1000):
    response = s.get(f'https://novosibirsk.n1.ru/kupit/kvartiry/type-studiya/?page={i}&limit=100&floors_count_min=10&floors_count_max=10')
    print(response)
    soup_page = BeautifulSoup(response.content, "lxml")
    links_page = [f"https://novosibirsk.n1.ru/{l['href']}" for l in soup_page.find_all("a", class_="link", target='_blank')]
    if links_page != []:
        all_links.extend(links_page)
    else:
        break
for i in all_links:   ### парсим каждую недвижимость и добавляем в БД
    parse_house(i, time_now, 'studios')


# ## 1-комнатные

# In[16]:


### создаем Таблицу в БД для однокомнатных
create_table_db(time_now, 'odnokomnatnye')
all_links = []    ### список для ссылок каждой недвижимости
s = Session()
s.headers.update(headers)
for i in range(1, 1000):    ### находим страницы неджвижимостей
    response = s.get(f'https://novosibirsk.n1.ru/kupit/kvartiry/rooms-odnokomnatnye/?page={i}&limit=100&floors_count_min=10&floors_count_max=10')
    print(response)
    soup_page = BeautifulSoup(response.content, "lxml")
    links_page = [f"https://novosibirsk.n1.ru/{l['href']}" for l in soup_page.find_all("a", class_="link", target='_blank')]
    if links_page != []:
        all_links.extend(links_page)
    else:
        break
for i in all_links:    ### парсим каждую недвижимость и добавляем в БД
    parse_house(i, time_now, 'odnokomnatnye')


# ## 2-комнатные

# In[17]:


### создаем Таблицу в БД для двухкомнатных
create_table_db(time_now, 'dvuhkomnatnye')
all_links = []   ### список для ссылок каждой недвижимости
s = Session()
s.headers.update(headers)

for i in range(1, 1000):   ### находим страницы неджвижимостей
    response = s.get(f'https://novosibirsk.n1.ru/kupit/kvartiry/rooms-dvuhkomnatnye/?page={i}&limit=100&floors_count_min=10&floors_count_max=10')
    print(response)
    soup_page = BeautifulSoup(response.content, "lxml")
    links_page = [f"https://novosibirsk.n1.ru/{l['href']}" for l in soup_page.find_all("a", class_="link", target='_blank')]
    if links_page != []:
        all_links.extend(links_page)
    else:
        break

for i in all_links:   ### парсим каждую недвижимость и добавляем в БД
    parse_house(i, time_now, 'dvuhkomnatnye')


# ## 3-комнатные

# In[18]:


### создаем Таблицу в БД для двухкомнатных
create_table_db(time_now, 'trehkomnatnye')
all_links = []
s = Session()
s.headers.update(headers)
for i in range(1, 1000):    ### находим страницы неджвижимостей
    response = s.get(f'https://novosibirsk.n1.ru/kupit/kvartiry/rooms-trehkomnatnye/?page={i}&limit=100&floors_count_min=10&floors_count_max=10')
    print(response)
    soup_page = BeautifulSoup(response.content, "lxml")
    links_page = [f"https://novosibirsk.n1.ru/{l['href']}" for l in soup_page.find_all("a", class_="link", target='_blank')]
    if links_page != []:
        all_links.extend(links_page)
    else:
        break
for i in all_links:    ### парсим каждую недвижимость и добавляем в БД
    parse_house(i, time_now, 'trehkomnatnye')


# ## 4-комнатные

# In[19]:


### создаем Таблицу в БД для четырехкомнатных
create_table_db(time_now, 'chetyrehkomnatnye')
all_links = []
s = Session()
s.headers.update(headers)
for i in range(1, 1000):    ### находим страницы неджвижимостей
    response = s.get(f'https://novosibirsk.n1.ru/kupit/kvartiry/rooms-chetyrehkomnatnye/?page={i}&limit=100&floors_count_min=10&floors_count_max=10')
    print(response)
    soup_page = BeautifulSoup(response.content, "lxml")
    links_page = [f"https://novosibirsk.n1.ru/{l['href']}" for l in soup_page.find_all("a", class_="link", target='_blank')]
    if links_page != []:
        all_links.extend(links_page)
    else:
        break
for i in all_links:   ### парсим каждую недвижимость и добавляем в БД
    parse_house(i, time_now, 'chetyrehkomnatnye')


# ## Многокомнатные

# In[20]:


### создаем Таблицу в БД для многокомнатных
create_table_db(time_now, 'mnogokomnatnye')
all_links = []
s = Session()
s.headers.update(headers)
for i in range(1, 1000):   ### находим страницы неджвижимостей
    response = s.get(f'https://novosibirsk.n1.ru/kupit/kvartiry/rooms-mnogokomnatnye/?page={i}&limit=100&floors_count_min=10&floors_count_max=10')
    print(response)
    soup_page = BeautifulSoup(response.content, "lxml")
    links_page = [f"https://novosibirsk.n1.ru/{l['href']}" for l in soup_page.find_all("a", class_="link", target='_blank')]
    if links_page != []:
        all_links.extend(links_page)
    else:
        break
for i in all_links:   ### парсим каждую недвижимость и добавляем в БД
    parse_house(i, time_now, 'mnogokomnatnye')


# ## Свободная планировка

# In[23]:


### создаем Таблицу в БД 
create_table_db(time_now, 'layout_free')
all_links = []
s = Session()
s.headers.update(headers)
for i in range(1, 1000):   ### находим страницы неджвижимостей
    response = s.get(f'https://novosibirsk.n1.ru/kupit/kvartiry/?page={i}&limit=100&floors_count_min=10&floors_count_max=10&layout_type=free')
    print(response)
    soup_page = BeautifulSoup(response.content, "lxml")
    links_page = [f"https://novosibirsk.n1.ru/{l['href']}" for l in soup_page.find_all("a", class_="link", target='_blank')]
    if links_page != []:
        all_links.extend(links_page)
    else:
        break
for i in all_links:  ### парсим каждую недвижимость и добавляем в БД
    parse_house(i, time_now, 'layout_free')


# In[ ]:




