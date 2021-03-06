import re
import requests
import glob
from datetime import date, timedelta
import pandas as pd


class CCGPCrawler:
    HEADER = {
        'Accept': 'text/html, */*; q=0.01', 'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'http://ccgp-shaanxi.gov.cn',
        'Referer': 'http://ccgp-shaanxi.gov.cn/notice/list.do?noticetype=5&index=5&province=province',
        'Accept-Encoding': 'gzip, deflate', 'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'JSESSIONID=ECF24CAD475B0D9665E27E54A8FF20D2; HasLoaded=true'
    }

    URL_REX = re.compile(f'href="(.*?)" target="_blank"')
    NOTICE_HEADER_REX = re.compile('<h1 class="content-tit">(.*?)</h1>')
    # TODO: 中标单位
    SUPPLIER_REX = re.compile('供应商(.*?)</p>')
    ADDRESS_REX = re.compile('地址(.*?)</p>')
    CONTACTS_REX = re.compile('联系人(.*?)</p>')
    TEL_REX = re.compile('联系电话(.*?)</p>')

    def __init__(self, start_date, end_date, region_id):
        self._session = None
        self._start_date = start_date
        self._end_date = end_date
        self._region_id = region_id
        self._notice_urls = set()

    @property
    def session(self):
        if not self._session:
            self._session = requests.session()
            self._session.headers = self.HEADER
        return self._session

    def _fetch_one_page_notice(self, page_num=None):
        # time.sleep(.3)
        return self.session.post(
            'http://ccgp-shaanxi.gov.cn/notice/noticeaframe.do?noticetype=5&isgovertment=',
            headers=self.HEADER,
            data=self._generate_post_data(page_num=page_num)
        )

    def _get_notice_pages(self):
        resp = self._fetch_one_page_notice()
        if resp.status_code != 200:
            print('get posts failed!')
            return 0
        max_page_num = int(resp.text.rsplit("javascript:toPage('',", 1)[-1].split(')', 1)[0])
        print(f'find pages: {max_page_num}')
        return max_page_num

    def _generate_post_data(self, page_num=None):
        post_data = {
            "parameters['startdate']": self._start_date,
            "parameters['enddate']": self._end_date,
            "parameters['regionguid']": self._region_id,
        }
        if page_num:
            post_data.update({'page.pageNum': page_num})
        return post_data

    def _fetch_pages(self):
        notices = []
        max_page_num = self._get_notice_pages()
        for page_num in range(1, max_page_num + 1):
            one_notice_page_resp = self._fetch_one_page_notice(page_num)
            self._parse_notice_url(one_notice_page_resp)
        print(f'find notices: {len(self._notice_urls)}')
        for index, notice_url in enumerate(self._notice_urls):
            print(f'left: {len(self._notice_urls) - index}; parsing {notice_url}')
            notice_detail_resp = self.session.get(notice_url)
            # time.sleep(.3)
            notice_detail = self._parse_notice_detail(notice_detail_resp)
            notice_detail.append(notice_url)
            notices.append(notice_detail)
        return notices

    def _parse_notice_url(self, resp):
        """解析每一页中的招标信息链接"""
        for notice_url in self.URL_REX.findall(resp.text):
            if 'noticeguid' in notice_url:
                self._notice_urls.add(notice_url)

    def _parse_notice_detail(self, resp):
        """解析详细的中标信息"""
        # TODO: 当前仅解析一个中标单位， 可能存在多个中标单位的情况
        notice_content = resp.text.replace('中标单位', '供应商')
        notice_title = self.NOTICE_HEADER_REX.findall(notice_content)[0]
        supplier = '\n'.join(self.SUPPLIER_REX.findall(notice_content)).strip().strip('：').strip(':').strip()
        # 默认其他信息在供应商信息之后
        notice_content = notice_content.split(supplier, 1)[-1]
        notice = [notice_title, supplier]
        for ptn in (self.ADDRESS_REX, self.CONTACTS_REX, self.TEL_REX):
            notice.append('\n'.join(ptn.findall(notice_content)).strip().strip('：').strip(':').strip())
        return notice

    def run(self):
        print(f'start date: {self._start_date}, end date: {self._end_date}')
        notices = self._fetch_pages()
        df = pd.DataFrame(data=notices, columns=['标题', '供应商', '供应商地址', '联系人', '联系方式', '中标链接'])
        df.to_excel(f'{self._start_date}_{self._end_date}_{self._region_id}.xlsx', index=False, encoding='utf-8')

    def get_suppliers(self):
        base_url = 'http://www.ccgp-shaanxi.gov.cn//supplier/supplierList.do'
        supplier_ptn = re.compile('<td align="center">(.*?)</td>')

        def _parse_suppliers(ptn, text):
            found = list(ptn.findall(text))
            suppliers_count = len(found) // 5
            suppliers = [[] for _ in range(suppliers_count)]
            for index, item in enumerate(found):
                suppliers[index // 5].append(item)
            return suppliers

        def _get_suppliers(supplier_type):
            if supplier_type == '公示中':
                supplier_state = 9
                page_num_key = 'page.pageNum',
                total_page = 42
            else:
                supplier_state = 10
                page_num_key = 'grid.page.pageNum',
                total_page = 6576
            all_suppliers = []
            for i in range(1, total_page):
                print(f'{supplier_type} process page: {i}')
                resp = self.session.post(base_url, data={"parameters['supplierstate']": supplier_state, page_num_key: i})
                suppliers = _parse_suppliers(supplier_ptn, resp.text)
                all_suppliers.extend(suppliers)
            df = pd.DataFrame(data=all_suppliers, columns=['序号', '供应商名称', '地址', '联系人', '联系方式'])
            df.drop(columns=['序号'], axis=1, inplace=True)
            df.to_excel(f'{supplier_type}供应商.xlsx', index=False, encoding='utf-8')

        for supplier_type in ('公示中', '在册'):
            _get_suppliers(supplier_type)


def grid_crawl(start_date, end_date, region_id):
    _start_date = date(*[int(i) for i in start_date.split('-')])
    _end_date = date(*[int(i) for i in end_date.split('-')])

    while 1:
        tmp_end_date = _start_date + timedelta(days=30)
        if tmp_end_date <= _end_date:
            CCGPCrawler(str(_start_date), str(tmp_end_date), region_id).run()
            _start_date = tmp_end_date
        else:
            CCGPCrawler(str(_start_date), str(end_date), region_id).run()
            break


def concat_excels():
    files = glob.glob('*.xlsx')
    dfs = [pd.read_excel(file) for file in files]
    all_df = pd.concat(dfs, ignore_index=True)
    all_df.to_excel('中标信息.xlsx', index=False, encoding='utf-8')


if __name__ == '__main__':
    # start_date = '2010-07-01'
    # end_date = '2020-07-01'
    # region_id = '610001'
    #
    # grid_crawl(start_date, end_date, region_id)
    # concat_excels()
    c = CCGPCrawler('', '', '')
    c.get_suppliers()
