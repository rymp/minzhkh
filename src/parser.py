#!/bin/python3
from os.path import dirname, abspath, join
import re
import yaml

from bs4 import BeautifulSoup
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from sqlalchemy import create_engine, exc


class Parser:
    def __init__(self):
        self.basedir = dirname(abspath(__file__))
        # коннекты к постгре
        with open(join(dirname(self.basedir), 'conf', 'db.yaml')) as f:
            db_yaml = yaml.load(f, Loader=yaml.FullLoader)
            self.pg_conn = psycopg2.connect(
                user=db_yaml['user'],
                password=db_yaml['password'],
                host=db_yaml['host'],
                port=int(db_yaml['port']),
                database=db_yaml['database'])
            self.pg_cursor = self.pg_conn.cursor()
            self.pg_engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(
                user=db_yaml['user'],
                password=db_yaml['password'],
                host=db_yaml['host'],
                port=int(db_yaml['port']),
                database=db_yaml['database']
            ))

        self.url = "https://dom.mingkh.ru"
        self.profile = webdriver.FirefoxProfile()
        self.profile.set_preference("general.useragent.override", "[user-agent string]")
        # Below is tested line
        self.profile.set_preference("general.useragent.override",
                                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:63.0) Gecko/20100101 Firefox/63.0")
        self.options = Options()
        self.options.headless = True
        self.driver = webdriver.Firefox(self.profile, options=self.options)

        with open(join(dirname(self.basedir), 'sql', 'info', 'realty.sql'), 'r') as sql:
            self.realty_col = sqlio.read_sql_query(sql.read(), self.pg_conn)['column_name'].values

        with open(join(dirname(self.basedir), 'conf', 'regions.txt')) as f:
            self.regions = f.read().splitlines()

        for self.region in self.regions:
            print(self.region)
            self.data = dict([(i, []) for i in self.realty_col])
            while True:
                self.crawler()
                if len(self.links) > 10:
                    break
            for self.link in self.links:
                if self.link['href'] == '#':
                    continue
                self.page_parse()
            self.save_data()


    def crawler(self):
        url = self.url + "/" + self.region
        self.driver.get(url)
        buttom = self.driver.find_element_by_xpath('/html/body/div[1]/div[2]/div[2]/div[8]/div/div/div[2]/div/button')
        buttom.click()
        buttom_all = self.driver.find_element_by_xpath(
            '/html/body/div[1]/div[2]/div[2]/div[8]/div/div/div[2]/div/ul/li[4]/a')
        buttom_all.click()
        soup = BeautifulSoup(self.driver.page_source, features='html.parser')
        table = soup.find('tbody')
        self.links = table.find_all("a", href=True)


    def page_parse(self):
        def get_value_dt_dd(soup, key):
            try:
                return clear_text(soup.find('dt', text=key).findNext('dd').getText())
            except AttributeError:
                return None

        def get_value_td_td(soup, key):
            try:
                return clear_text(soup.find('td', text=key).findNext('td').getText())
            except AttributeError:
                return None

        def get_value_td_td_td(soup, key):
            try:
                return clear_text(soup.find('td', text=key).findNext('td').findNext('td').getText())
            except AttributeError:
                return None

        def clear_text(text):
            return ' '.join(re.sub(r"[^А-яЁё№\d\. ]", ' ', text).split())


        self.data['region'].append(self.region)
        self.data['address'].append(self.link.text)
        self.data['city'].append(self.link["href"].split('/')[2])
        self.data['id'].append(int(self.link["href"].split('/')[3]))
        url = self.url + self.link["href"]
        self.driver.get(url)
        soup = BeautifulSoup(self.driver.page_source.replace("<sup>2</sup>", "")
                             # this magic I can't explain, find text with <sup> inside doesn't work
                             , features='html.parser')

        dt_dd_dict = {
            "cadastral_id": "Кадастровый номер",
            "year": "Год постройки",
            "floors": "Количество этажей",
            "estate_type": "Тип дома",
            "rooms": "Жилых помещений",
            "type": "Серия, тип постройки",
            "playground": "Детская площадка",
            "sports_ground": "Спортивная площадка",
            "company": "Управляющая компания",
            "flooring_type": "Тип перекрытий",
            "walls_type": "Материал несущих стен",
            "garbage_disposal_type": "Тип мусоропровода",
            "is_unsafe": "Дом признан аварийным",
        }
        td_td = {
            "space_living": "Площадь жилых помещений м",
            "space": "Площадь нежилых помещений м",
            "space_common": "Площадь нежилых помещений м",
            "energy_efficient": "Класс энергетической эффективности",
        }
        td_td_td = {
            "inputs": "Количество вводов в дом, ед.",
            "gas": "Газоснабжение",
            "sewer": "Водоотведение",
            "hot_water": "Горячее водоснабжение",
            "cold_water": "Холодное водоснабжение",
            "heating": "Теплоснабжение",
            "electricity": "Электроснабжение",
        }
        for key, value in dt_dd_dict.items():
            self.data[key].append(get_value_dt_dd(soup, value))
        for key, value in td_td.items():
            self.data[key].append(get_value_td_td(soup, value))
        for key, value in td_td_td.items():
            self.data[key].append(get_value_td_td_td(soup, value))

    def save_data(self):
        self.data = pd.DataFrame.from_dict(self.data, orient='index').T[self.realty_col]
        try:
            self.data.to_sql(schema='dom',
                             name='realty',
                             con=self.pg_engine,
                             index=False,
                             if_exists='append',
                             method='multi')
        except exc.IntegrityError:
            pass


if __name__ == '__main__':
    Parser()