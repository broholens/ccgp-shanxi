import time

import pdfplumber
import pandas as pd
import sqlite3
import random
from selenium import webdriver
from selenium.webdriver.common.keys import Keys


def load_pdf(filename):
    company_list = []

    with pdfplumber.open(filename) as pdf:
        for page in pdf.pages:
            for company in page.extract_table()[1:]:
                company_list.append(company[1:])

    return company_list


def pdf_2_excel(filename):
    company_list = load_pdf(filename)
    df = pd.DataFrame(company_list, columns=['name', 'code', 'address'])
    excel_filename = filename.split('.')[0] + '.xlsx'
    df.to_excel(excel_filename, index=False)
    return excel_filename

# pdf_2_excel("科小陕西名单.pdf")


class DBConnection:
    def __init__(self):
        self.db_name = 'shuidi.db'
        self._init_db()

    def _init_db(self):
        self.con = sqlite3.connect(self.db_name)
        self.cur = self.con.cursor()
        self.cur.execute("CREATE TABLE IF NOT EXISTS company(name varchar(256), code varchar(128), address varchar(256), visited boolean, contactor varchar(16), phone varchar(128))")
        self.con.commit()

    def select_non_queried(self):
        self.cur.execute('select code from company where visited = 0')
        return self.cur.fetchall()

    def insert(self, data):
        self.cur.execute("INSERT INTO company values(?,?,?,?,?,?)", data)
        self.con.commit()

    def update(self, data):
        self.cur.execute("UPDATE company SET visited=?, contactor=?, phone=? WHERE code=?", data)
        self.con.commit()

# 企业信息从PDF入库
# db = DBConnection()
#
# for company in load_pdf("科小陕西名单.pdf"):
#     db.insert(company+[False, '', ''])


# ['3', '西安西翼智能科技有限公司', '91610131729965202Q', '陕西省西安市雁塔区']


class ShuiDiCrawler:
    def __init__(self, username, password):
        self.d = webdriver.Chrome()
        self.d.maximize_window()
        self.db_con = DBConnection()
        self.login(username, password)

    def login(self, username, password):
        self.d.get('https://shuidi.cn/')
        self.d.find_element_by_class_name('login-btn').click()
        time.sleep(.3)
        self.d.find_element_by_partial_link_text('密码登录').click()
        uname, pwd = self.d.find_elements_by_xpath('//div[contains(@class, "password-login")]//input')[:2]
        uname.clear()
        uname.send_keys(username)
        pwd = self.d.find_element_by_name('password')
        pwd.clear()
        pwd.send_keys(password)
        pwd.send_keys(Keys.ENTER)
        time.sleep(10)  # 手动验证

    def query_one(self, code):
        search_input = self.d.find_element_by_xpath('//input[contains(@class, "search-key")]')
        search_input.clear()
        search_input.send_keys(code)
        search_input.send_keys(Keys.ENTER)
        time.sleep(1)
        legal_name = self.d.find_element_by_class_name('legal_name').text
        phone = self.d.find_element_by_xpath('//div[contains(@class, "add-phone")]/span').text
        phone = phone.split('：')[-1]
        print(legal_name, phone, code)
        self.db_con.update((1, legal_name, phone, code))

    def query(self):
        for code in self.db_con.select_non_queried():
            self.query_one(code[0])
            time.sleep(random.random()*10)


sd = ShuiDiCrawler(username='', password='')
sd.query()
