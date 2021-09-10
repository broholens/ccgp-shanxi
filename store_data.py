import sqlite3
from crawler.crawl import CCGPCrawler


class CCGPStore:
    def __init__(self):
        self._conn = None
        self._cur = None
        self._init_table()
        self.crawler = CCGPCrawler('', '', '610001')

    @property
    def cur(self):
        if not self._conn:
            self._conn = sqlite3.connect('ccgp.db')
            self._cur = self._conn.cursor()
        return self._cur

    def _init_table(self):
        self.cur.execute(
            "CREATE TABLE IF NOT EXISTS ccgp(id INTEGER PRIMARY KEY,region_id VARCHAR,notice_id VARCHAR, content TEXT)"
        )
        self._conn.commit()

    def __del__(self):
        self._cur.close()
        self._conn.close()

    def close(self):
        self._cur.close()
        self._conn.close()

    def store(self):
        max_page_num = self.crawler._get_notice_pages()
        for page_num in range(1, max_page_num+1):
            print(f'left {max_page_num-page_num}')
            one_notice_page_resp = self.crawler._fetch_one_page_notice(page_num)
            self.crawler._parse_notice_url(one_notice_page_resp)
        for index, url in enumerate(self.crawler._notice_urls):
            print(f'left: {len(self.crawler._notice_urls) - index}; storing {url}')
            content = self.crawler.session.get(url).text
            notice_id = url.split('noticeguid=')[-1]
            self._cur.execute(f'insert into ccgp(region_id, notice_id, content) values ({self.crawler._region_id}, {notice_id}, {content})')
            self._conn.commit()


if __name__ == '__main__':
    CCGPStore().store()