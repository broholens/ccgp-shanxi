import time
import sqlite3
import traceback
from functools import wraps
from threading import Lock, Thread

import grequests
import pandas as pd
from pyfunctions import fun
from selenium.webdriver.common.keys import Keys

###########
### 河南 ###
###########
# driver = fun.make_driver()
# driver.get('http://222.143.21.205:8081/onlineRetailers/cslist')
#
# result = []
#
# for i in range(2, 53):
#     for tr in driver.find_element_by_class_name('goods_particulars').find_elements_by_tag_name('tr'):
#         item = [td.text.strip() for td in tr.find_elements_by_tag_name('td')]
#         if item:
#             result.append(item)
#     driver.execute_script(f'page({i},20,'');')
#     time.sleep(1)
#
# pd.DataFrame(result, columns=['厂商名称', '联系人', '联系方式',	'固话',	'传真']).to_excel('河南.xlsx', index=False)


# 广东


# class SqliteMultiThread(Thread):
#     """
#     Wrap sqlite connection in a way that allows concurrent requests from multiple threads.
#
#     This is done by internally queueing the requests and processing them sequentially
#     in a separate thread (in the same order they arrived).
#
#     """
#     def __init__(self, filename, autocommit, journal_mode):
#         super().__init__()
#         self.filename = filename
#         self.autocommit = autocommit
#         self.journal_mode = journal_mode
#         self.reqs = Queue()
#         self.setDaemon(True)
#         self.start()
#
#     def run(self):
#         if self.autocommit:
#             conn = sqlite3.connect(self.filename, isolation_level=None, check_same_thread=False)
#         else:
#             conn = sqlite3.connect(self.filename, check_same_thread=False)
#         conn.execute('PRAGMA journal_mode = %s' % self.journal_mode)
#         conn.text_factory = str
#         cursor = conn.cursor()
#         cursor.execute('PRAGMA synchronous=OFF')
#         while True:
#             req, arg, res = self.reqs.get()
#             if req == '--close--':
#                 break
#             elif req == '--commit--':
#                 conn.commit()
#             else:
#                 cursor.execute(req, arg)
#                 if res:
#                     for rec in cursor:
#                         res.put(rec)
#                     res.put('--no more--')
#                 if self.autocommit:
#                     conn.commit()
#         conn.close()
#
#     def execute(self, req, arg=None, res=None):
#         """
#         `execute` calls are non-blocking: just queue up the request and return immediately.
#
#         """
#         self.reqs.put((req, arg or tuple(), res))
#
#     def executemany(self, req, items):
#         for item in items:
#             self.execute(req, item)
#
#     def select(self, req, arg=None):
#         """
#         Unlike sqlite's native select, this select doesn't handle iteration efficiently.
#
#         The result of `select` starts filling up with values as soon as the
#         request is dequeued, and although you can iterate over the result normally
#         (`for res in self.select(): ...`), the entire result will be in memory.
#
#         """
#         res = Queue() # results of the select will appear as items in this queue
#         self.execute(req, arg, res)
#         while True:
#             rec = res.get()
#             if rec == '--no more--':
#                 break
#             yield rec
#
#     def select_one(self, req, arg=None):
#         """Return only the first row of the SELECT, or None if there are no matching rows."""
#         try:
#             return iter(self.select(req, arg)).next()
#         except StopIteration:
#             return None
#
#     def commit(self):
#         self.execute('--commit--')
#
#     def close(self):
#         self.execute('--close--')


def sqlite_conn_provider(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        kwargs['conn'] = sqlite3.connect('zb.db')
        kwargs['cursor'] = kwargs['conn'].cursor()
        return func(*args, **kwargs)
    return wrapper


class GuangDong:
    def __init__(self):
        self.db_lock = Lock()
        self.db_name = 'guangdong'

    @sqlite_conn_provider
    def create_table(self, conn=None, cursor=None):
        with self.db_lock:
            cursor.execute(f'CREATE TABLE IF NOT EXISTS {self.db_name} (url varchar(256), visited int default 0, company varchar(256), person varchar(16), tel varchar(128), code varchar(256))')
            conn.commit()

    @sqlite_conn_provider
    def get_urls(self, conn=None, cursor=None):
        """获取招标页面的URL"""
        driver = fun.make_driver()
        driver.get('https://gdgpo.czt.gd.gov.cn/cms-gd/site/guangdong/gysml/index.html')
        time.sleep(1)
        driver.find_element_by_id('Inquire').click()
        time.sleep(4)

        total_pages = 13600
        for i in range(1, total_pages):
            print(f'{i}/{total_pages}')
            try:
                data = self._generate_init_data(driver)
            except Exception:
                print(f'{i} failed, {traceback.format_exc()}')
                continue

            with self.db_lock:
                cursor.executemany('insert into guangdong(url) values (?)', data)
                conn.commit()

            page_nu = driver.find_element_by_xpath('//input[@type="number"]')
            page_nu.clear()
            page_nu.send_keys(i+1)
            page_nu.send_keys(Keys.ENTER)
            time.sleep(4)

    @staticmethod
    def _generate_init_data(driver):
        """返回解析完URL后插入数据库中的数据"""
        data = []
        for a in driver.find_elements_by_xpath('//ul[@id="agencyList"]//a'):
            _id = a.get_attribute('href').split('id=')[-1]
            data.append((_id,))
        return data

    @sqlite_conn_provider
    def get_data(self, batch_size=10, conn=None, cursor=None):
        while 1:
            with self.db_lock:
                cursor.execute(f'select url from {self.db_name} where visited != 1 limit {batch_size}')
                urls = [url[0] for url in cursor.fetchall()]

            if not urls:
                time.sleep(5)
                continue

            reqs = [grequests.get(f'https://gdgpo.czt.gd.gov.cn/gateway/gpbs-supplier/rest/v1/supplier/supplierinfo/info/supplierinfo/{url}') for url in urls]

            update_data = []

            for resp, url in zip(grequests.map(reqs), urls):
                try:
                    data = resp.json()['data']['supplierInfo']
                    company, person = data['supplyCn'] or data['supplyCnnick'], data['personName'] or data['legalPerson']
                    tel, code = data['supplyTel'], data['createUserId']
                    update_data.append((company, person, tel, code, url))
                except Exception:
                    print(f'parse failed {traceback.format_exc()}')
                    continue

            with self.db_lock:
                sql = f"update {self.db_name} set visited=1, company=?, person=?, tel=?, code=? where url=?"
                cursor.executemany(sql, update_data)
                conn.commit()
            print(urls)

    @sqlite_conn_provider
    def check_data(self, conn=None, cursor=None):  # TODO: close conn
        while 1:
            with self.db_lock:
                cursor.execute(f'select count(*) from {self.db_name} where visited != 1')
                print(f'data to fetch: {cursor.fetchone()}')
            time.sleep(30)


if __name__ == '__main__':
    gd = GuangDong()
    gd.create_table()

    t_get_urls = Thread(target=gd.get_urls)
    t_get_urls.setDaemon(True)
    t_get_urls.start()

    t_get_data = Thread(target=gd.get_data)
    t_get_data.setDaemon(True)
    t_get_data.start()

    t_check_data = Thread(target=gd.check_data)
    t_check_data.setDaemon(True)
    t_check_data.start()

    t_get_urls.join()
    t_get_data.join()
    t_check_data.join()

