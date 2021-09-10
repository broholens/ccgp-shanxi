import re
import time
import requests
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
        for page_num in range(1, max_page_num+1):
            one_notice_page_resp = self._fetch_one_page_notice(page_num)
            self._parse_notice_url(one_notice_page_resp)
        print(f'find notices: {len(self._notice_urls)}')
        for index, notice_url in enumerate(self._notice_urls):
            print(f'left: {len(self._notice_urls)-index}; parsing {notice_url}')
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
        notice = []
        for ptn in (self.NOTICE_HEADER_REX, self.SUPPLIER_REX, self.ADDRESS_REX, self.CONTACTS_REX, self.TEL_REX):
            notice.append('\n'.join(ptn.findall(resp.text)).strip().strip('：').strip(':').strip())
        return notice

    def run(self):
        notices = self._fetch_pages()
        df = pd.DataFrame(data=notices, columns=['标题', '供应商', '供应商地址', '联系人', '联系方式', '中标链接'])
        df.to_excel(f'{self._start_date}_{self._end_date}_{self._region_id}.xlsx', index=False, encoding='utf-8')


if __name__ == '__main__':
    start_date = '2018-07-01'
    end_date = '2020-07-01'
    region_id = '610001'

    from datetime import date, timedelta

    _start_date = date(*[int(i) for i in start_date.split('-')])
    _end_date = date(*[int(i) for i in end_date.split('-')])

    while 1:
        tmp_end_date = _start_date + timedelta(days=30)
        if tmp_end_date <= _end_date:
            print(f'start date: {_start_date}, end date: {tmp_end_date}')
            c = CCGPCrawler(str(_start_date), str(tmp_end_date), region_id)
            c.run()
            _start_date = tmp_end_date
        else:
            print(f'start date: {_start_date}, end date: {end_date}')
            c = CCGPCrawler(str(_start_date), str(end_date), region_id)
            c.run()
            break
