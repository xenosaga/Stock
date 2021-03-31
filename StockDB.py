import pymysql
from mod.models import SqlBase

class StockDb():
    
    def __init__(self, host, user, pw, dbname):
        self.__host = host
        self.__user = user
        self.__pw = pw
        self.__dbname = dbname
        self.__sb = None
        
    def connect_db(self):
        self.__db = pymysql.connect(self.__host, 
                                    self.__user, 
                                    self.__pw, 
                                    self.__dbname, 
                                    charset='utf8')
        self.__cursor = self.__db.cursor()
        self.__sb = SqlBase(self.__cursor)

    def insert(self, tname, data):
        pass
    
    def delete(self, tname, stemt):
        pass

    def update(self, tname, data):
        pass
    
    def query(self, tname, data):
        self.__sb.reset_query()
        self.__sb.get_where(tname, data)
        sql = self.__sb.get_compiled_select()
        print(sql)
        pass

    def test(self):
        sb = SqlBase(self.__cursor)

        # sb.insert('stock_status', {'stock_num':str(2330),
        #                            'last_update':'now()'})
        # sql = sb.get_compiled_insert()
        # print(sql)

        # sb.reset_query()
        # sb.where('stock_num', str(2330))
        # sb.get('stock_status')
        # sql = sb.get_compiled_select()
        # print(sql)

        # sb.reset_query()
        # sb.update('stock_status', {'last_update':'now()'}, 'stock_num = 2330')
        # sql = sb.get_compiled_update()
        # print(sql)

        # sb.reset_query()
        # sb.delete('stock_status', 'stock_num = 2330') 
        # sql = sb.get_compiled_delete()
        # print(sql)

        sb.reset_query()
        sb.insert('stock_dividen', {'stock_num':str(2330),
                                    'div_date':'\'2008-02-03\'',
                                    'div_price':str(233.2),
                                    'div_amount':str(2.5),
                                    'div_precent':str(5.2)})
        sql = sb.get_compiled_insert()
        res = sb.execute()
        self.__db.commit()

        print(sql)

        sb.reset_query()
        # sb.insert(2330, self.__cursor)
        # self.__db.commit()

        # c = ss.query(2330, self.__cursor)
        # print(c.fetchone())

        # ss.update(2330, self.__cursor)
        # self.__db.commit()

        # c = ss.query(2330, self.__cursor)
        # print(c.fetchone())

        # ss.delete(2330, self.__cursor)
        # self.__db.commit()

    def disconnext_db(self):
        self.__db.close()

if __name__ == '__main__':
    sd = StockDb("localhost", "stockuser", "stock", "STOCK")
    sd.connect_db()
    sd.query('stock', {'stock_num':str(2330),
                        'div_amount >=':str(2.0)})
    sd.disconnext_db()