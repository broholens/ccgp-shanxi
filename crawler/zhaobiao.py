import sqlite3
from math import ceil

import grequests
import requests
import urllib3
import pandas as pd
from pyfunctions import fun

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

urllib3.disable_warnings()


class GuangDong:
    def __init__(self):
        self.db_name = 'guangdong'
        self.conn = sqlite3.connect('zb.db')
        self.cursor = self.conn.cursor()
        self.base_url = 'https://gdgpo.czt.gd.gov.cn/gateway/gpbs-supplier/rest/v1/supplier/supplierinfo/shortlisted/'
        self.items_per_page = 100

    def create_table(self):
        self.cursor.execute(f'CREATE TABLE IF NOT EXISTS {self.db_name} (url varchar(256), company varchar(256), person varchar(16), tel varchar(128), code varchar(256))')
        self.conn.commit()

    @staticmethod
    def _request(url):
        return requests.post(url, timeout=10, verify=False, json={}).json()['data']

    @staticmethod
    def _grequest(urls):
        items = []
        reqs = [grequests.post(url, timeout=30, verify=False, json={}) for url in urls]
        for resp in grequests.map(reqs):
            items.extend(resp.json()['data']['list'])
        return items

    def get_data(self):
        # 总记录数
        total_count = int(self._request(f'{self.base_url}1/1')['total'])
        print(f'总数据量：{total_count}')
        # 分页查询
        pages = ceil(total_count / self.items_per_page)

        batch_urls = []
        update_data = []
        for i in range(1, pages+1):
            if i % 10 == 0:  # 每10个URL请求一次
                self._get_data(batch_urls, update_data)
                batch_urls, update_data = [], []
            else:
                batch_urls.append(f'{self.base_url}{i}/{self.items_per_page}')
        self._get_data(batch_urls, update_data)

    def _get_data(self, batch_urls, update_data):
        for item in self._grequest(urls=batch_urls):
            url, code, company = item['id'], item['orgCode'], item['supplyCn'] or item['supplyCnnick']
            tel, person = item['supplyTel'], item['personName'] or item['legalPerson']
            update_data.append((url, company, person, tel, code))
        sql = f"insert into {self.db_name} values (?,?,?,?,?)"
        self.cursor.executemany(sql, update_data)
        self.conn.commit()
        print(batch_urls)


if __name__ == '__main__':
    gd = GuangDong()
    gd.create_table()
    gd.get_data()

