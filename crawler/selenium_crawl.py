import time
import pandas as pd
from pyfunctions import fun

driver = fun.make_driver()
driver.get('http://www.ccgp-shaanxi.gov.cn/supplier/supplierTab.do')
# total_page = driver.find_elements_by_xpath('//div[@id="supplierList"]/div[@class="list-box"]//a')

f = open('result.csv', 'w')

def get_suppliers(total_page):
    time.sleep(.5)
    suppliers = []
    for i in range(1, int(total_page)+1):
        print(f'page: {i}')
        page = driver.find_element_by_id('infoNoticeInputPage')
        page.clear()
        page.send_keys(i)
        driver.execute_script("toPageInput('','#infoNoticeInputPage',toPage)")
        time.sleep(.3)
        trs = driver.find_elements_by_xpath('//div[@class="list-box"]//tr')
        for tr in trs[1:]:
            f.write(tr.text+'\n')
            _item = tr.text.split(' ')
            suppliers.append(_item)
    return pd.DataFrame(suppliers, columns=['序号', '供应商名称', '地址', '联系人', '联系电话'])


ing_df = get_suppliers(2)
ing_df.drop(columns=['序号'], axis=1, inplace=True)
ing_df.to_excel('公示中供应商.xlsx', index=False)

driver.execute_script("changeSupplier(1,10)")

ed_df = get_suppliers(6623)
f.close()
ed_df.drop(columns=['序号'], axis=1, inplace=True)
ed_df.to_excel('在册供应商.xlsx', index=False)


