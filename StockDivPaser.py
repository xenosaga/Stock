import requests
from urllib.parse import quote
import json

from testdata import testdata

class StockDivParser():

    def __init__(self):
        self._url_base = 'marketinfo.api.cnyes.com/mi/api/v1/'
        self._url_proto = 'https://'
        self._headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"}
        pass

    # get devided data
    def get_divided_data(self, snum):
        self._stock_number = snum
        url = self.handle_url()
        res = requests.get(url, headers=self._headers)
        data = json.loads(res.text)
        return data['data']['divides']

    def handle_url(self):
        url = self._url_base + 'TWS:' + str(self._stock_number) + ':STOCK/divided'
        url = quote(url)
        url = self._url_proto + url
        return url
    
    def merge_dividen(self, data):
        ddvin = {}
        for row in data:
            y = row['formatDate'][:4]
            if( y not in ddvin ):
                # 還沒配息時
                if(len(row['formatPreClose']) == 0):
                    continue
                ddvin[y] = [float(row['formatPreClose']), float(row['formatCashDividend']), float(row['formatDividendYield']), 1]      
            else:
                ld = ddvin[y]
                ld[0] += float(row['formatPreClose'])
                ld[1] += float(row['formatCashDividend'])
                ld[2] += float(row['formatDividendYield'])
                ld[3] += 1
                ddvin[y] = ld
        
        # calc average
        for key, val in ddvin.items():
            val[0] = val[0] / val[3]
            val[1] = val[1] / val[3]
            val[2] = val[2] / val[3]
            ddvin[key] = val
        
        return ddvin

if __name__ == '__main__':
    sdp = StockDivParser()
    # data = sdp.get_divided_data(2330)
    # print(testdata)
    data = sdp.merge_dividen(testdata)
    print(data)
    # for row in data:
    #     print(row['formatDate'], row['formatPreClose'], row['formatCashDividend'])
