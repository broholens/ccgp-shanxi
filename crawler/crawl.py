import re
import time
import requests
import pandas as pd


class CCGPCrawler:
    MAIN_PAGE = 'http://ccgp-shaanxi.gov.cn/notice/list.do?noticetype=5&index=5&province=province'
    HEADER = {
        'Accept': 'text/html, */*; q=0.01', 'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'http://ccgp-shaanxi.gov.cn',
        'Referer': 'http://ccgp-shaanxi.gov.cn/notice/list.do?noticetype=5&index=5&province=province',
        'Accept-Encoding': 'gzip, deflate', 'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cookie': 'JSESSIONID=F9522ECDFCC0F7CE042B76F0BA6422ED; HasLoaded=true'
    }

    URL_REX = re.compile(f'href="(.*?)" target="_blank"')
    NOTICE_HEADER_REX = re.compile('<h1 class="content-tit">(.*?)</h1>')
    NOTICE_DETAIL_REX = re.compile('九、凡对本次公告内容提出询问，请按以下方式联系。(.*?)<p class="num">十、附件：</p>', re.DOTALL)

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
        time.sleep(.3)
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
            post_data.update({'page.pageNum:': page_num})
        return post_data

    def _fetch_pages(self):
        notices = []
        max_page_num = self._get_notice_pages()
        for page_num in range(1, max_page_num+1):
            one_notice_page_resp = self._fetch_one_page_notice(page_num)
            print(f'visit page: {page_num}')
            self._parse_notice_url(one_notice_page_resp)
        for notice_url in self._notice_urls:
            print(f'parsing {notice_url}')
            notice_detail_resp = self.session.get(notice_url)
            time.sleep(.3)
            try:
                notice_detail = self._parse_notice_detail(notice_detail_resp)
                notice_detail.append(notice_url)
                notices.append(notice_detail)
            except:
                with open('error.log', 'a+')as f:
                    f.write(notice_url+'\n')
        return notices

    def _parse_notice_url(self, resp):
        """解析每一页中的招标信息链接"""
        for notice_url in set(self.URL_REX.findall(resp.text)):
            if 'noticeguid' in notice_url:
                self._notice_urls.add(notice_url)

    def _parse_notice_detail(self, resp):
        """解析详细的招标信息"""
        notice_title = self.NOTICE_HEADER_REX.findall(resp.text)[0]
        notice = [notice_title]
        notice_detail = self.NOTICE_DETAIL_REX.findall(resp.text)[0].replace('<p>', '').replace('</p>', '')
        purchaser, other = notice_detail.split('2、项目联系方式：')
        # purchaser
        for i in purchaser.strip().strip('1、').split('\n'):
            notice.append(i.split('：')[-1].strip())

        project, proxy = other.split('3、采购代理机构：')
        # project
        for i in project.strip().split('\n'):
            notice.append(i.split('：')[-1].strip())
        # proxy
        for i in proxy.strip().split('\n'):
            notice.append(i.split('：')[-1].strip())

        return notice

    def run(self):
        notices = self._fetch_pages()
        df = pd.DataFrame(data=notices, columns=['标题', '采购人信息', '采购联系人', '采购联系地址', '采购联系电话', '项目联系人', '项目电话', '项目传真', '采购代理机构名称', '采购代理机构地址', '采购代理联系方式', '招标链接'])
        df.to_excel('招标信息.xlsx', index=False, encoding='utf-8')


if __name__ == '__main__':
    c = CCGPCrawler('2010-07-01', '2020-12-31', '610001')
    c.run()