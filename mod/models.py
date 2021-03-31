import datetime

class SqlBase():

    def __init__(self, cur):
        self.__sql = ''
        self.__sel_str = ''
        self.__from_str = ''
        self.__where_str = ''
        self.__group_str = ''
        self.__distince = ''
        self.__join_str = ''
        self.__cursor = cur

    def get(self, tname=''):
        if(len(tname) != 0):
            self.__from_str = tname

        if(len(self.__sel_str) == 0) :
            self.__sql = 'SELECT ' + self.__distince +' * FROM ' + tname
        else:
            self.__sql = 'SELECT ' + self.__distince + ' ' + self.__sel_str + ' FROM ' + tname

        # where statement
        if(len(self.__where_str) != 0):
            self.__sql = self.__sql + ' ' + self.__where_str

        # join statement
        if(len(self.__join_str) != 0):
            self.__sql = self.__sql + ' ' + self.__join_str

        self.__sql += ';'   # end of statemet

    def get_limit(self, tname, limit, offset):
        self.__sql = 'SELECT * FROM `' + tname + '` LIMIT ' + str(limit) + ', ' + str(offset) + ';'

    def get_where(self, tname, cond_list):
        self.__sql = 'SELECT * FROM ' + tname + ' '
    
        for (key, val) in cond_list.items():
            self.where(key, val)
        self.__sql += self.__where_str + ';'

    def select(self, col_list):
        self.__sel_str = ''
        for item in col_list:
            self.__sel_str += '`' + item +'`,'
        self.__sel_str = self.__sel_str[:-1]
    
    # raw statement
    def select_raw(self, selcond):
        self.__sel_str = selcond

    def select_max(self, col_name, alian=''):
        self.__sel_str = ''    
        if(len(alian) == 0):
            self.__sel_str = 'MAX(' + col_name + ') AS ' + col_name
        else:
            self.__sel_str = 'MAX(' + col_name + ') AS ' + alian

    def select_min(self, col_name, alian=''):
        self.__sel_str = ''    
        if(len(alian) == 0):
            self.__sel_str = 'MIN(' + col_name + ') AS ' + col_name
        else:
            self.__sel_str = 'MIN(' + col_name + ') AS ' + alian

    def select_avg(self, col_name, alian=''):
        self.__sel_str = ''    
        if(len(alian) == 0):
            self.__sel_str = 'AVG(' + col_name + ') AS ' + col_name
        else:
            self.__sel_str = 'AVG(' + col_name + ') AS ' + alian
    
    def select_sum(self, col_name, alian=''):
        self.__sel_str = ''    
        if(len(alian) == 0):
            self.__sel_str = 'SUM(' + col_name + ') AS ' + col_name
        else:
            self.__sel_str = 'SUM(' + col_name + ') AS ' + alian

    def select_from(self, tname):
        self.__from_str = tname

    def table_join(self, tname, jcond, jtype=''):
        if(len(jtype) == 0):
            self.__join_str = 'JOIN ' + tname + ' ON ' + jcond
        elif(jtype == 'left'):
            self.__join_str = 'LEFT JOIN ' + tname + ' ON ' + jcond
        elif(jtype == 'right'):
            self.__join_str = 'RIGHT JOIN ' + tname + ' ON ' + jcond
        elif(jtype == 'inner'):
            self.__join_str = 'INNER JOIN ' + tname + ' ON ' + jcond

    def where(self, key, val):
        if(len(self.__where_str) == 0):
            self.__where_str = 'WHERE'
        else:
            self.__where_str += ' AND'
        self.__where_str += self.__cond_compile(key, val)

    def or_where(self, key, val):
        self.__where_str += self.__cond_compile(key, val)

    # raw statement
    def where_raw(self, whcond):
        self.__where_str = whcond

    def where_in(self, val, cols):
        if(len(self.__where_str) == 0):
            self.__where_str = 'WHERE ' + val
        else:
            self.__where_str += ' AND ' + val
        
        self.__where_str += self.__in_statement(cols)
    
    def or_where_in(self, val, cols):
        self.__where_str += ' OR ' + val
        self.__where_str += self.__in_statement(cols)
        
    def where_not_in(self, val, cols):
        if(len(self.__where_str) == 0):
            self.__where_str = 'WHERE ' + val + ' NOT'
        else:
            self.__where_str += ' AND ' + val + ' NOT'
        self.__where_str += self.__in_statement(cols)

    def or_where_not_in(self, val, cols):
        self.__where_str += ' OR ' + val + ' NOT'
        self.__where_str += self.__in_statement(cols)

    def like(self, key, val, pos='both'):
        if(len(self.__where_str) == 0):
            self.__where_str = 'WHERE'
        else:
            self.__where_str += ' AND' 

        self.__where_str += ' `' + key + '` LIKE `' + self.__wildcard(val, pos) + '` ESCAPE \'!\''

    def or_like(self, key, val, pos='both'):
        self.__where_str += ' OR'
        self.__where_str += '`' + key + '` LIKE `' + self.__wildcard(val, pos) + '` ESCAPE \'!\''

    def or_not_like(self, key, val, pos='both'):
        self.__where_str += ' OR '
        self.__where_str += '`' + key + '` NOT LIKE `' + self.__wildcard(val, pos) + '` ESCAPE \'!\''

    def group_by(self, cols):
        if(type(cols) == str):
            self.__group_str = 'GROUP BY ' + cols
        else:
            self.__group_str = 'GROUP BY '
            col_str = ''
            for item in cols:
                col_str = col_str + item + ', '
            col_str = col_str[-2]
            self.__group_str += col_str
    
    def distince(self):
        self.__distince = 'DISTINCT'

    def having(self, stmt):
        self.__having_str = 'HAVING ' + stmt

    def or_having(self, stmt):
        self.__having_str = self.__having_str + ' OR ' + stmt

    def order_by(self, col, orde):
        if(len(self.__order_str) == 0):
            self.__order_str = 'ORDER BY `' + col + '` ' + orde
        else:
            self.__order_str = self.__order_str + ', `' + col + '` ' + orde

        # use param 1 as random seed
        if(orde == 'RANDOM'):
            if(type(col) == int):
                self.__order_str = 'ORDER BY RAND(' + str(col) + ')'
            else:
                self.__order_str = 'ORDER BY RAND()'
    
    def limit(self, amt, offset=0):
        if(offset == 0):
            self.__limit_str = 'LIMIT ' + str(amt)
        else:
            self.__limit_str = 'LIMIT ' + str(offset) + ', ' + str(amt)

    def insert(self, tname, dict_val):
        self.__sql = 'INSERT INTO ' + tname + '('
        col_str = ''
        val_str = ''
        for k,v in dict_val.items():
            col_str = col_str + k + ', '
            val_str = val_str + '' + v + ', '
        col_str = col_str[:-2]
        val_str = val_str[:-2]
        self.__sql = self.__sql + col_str + ') VALUES( ' + val_str + ');'


    def insert_batch(self, tname, dict_list):
        self.__sql = 'INSERT INTO ' + tname + '('
        
        # columns 
        col_str = ''
        for k, v in dist_list[0].items():
            col_str = col_str + k + ', '
        col_str = col_str[:-2]

        # values 
        val_str = ''
        for d_item in dict_list:
            tmp_str = ''
            for k, v in d_item.items():
                tmp_str = tmp_str + '`' + v + '`, '
            tmp_str = tmp_str[:-2] 
            tmp_str = '(' + tmp_str + ')'
            val_str = val_str + tmp_str + ', '
        val_str = val_str[:-2]

        self.__sql = self.__sql + col_str + ') VALUES' + val_str + ';'

    def update(self, tname, data, stamt=''):
        self.__sql = 'UPDATE ' + tname + ' SET '
        upd_str = ''
        for k, v in data.items():
            upd_str = upd_str + k + ' = ' + v + ', '
        upd_str = upd_str[:-2]
        self.__sql += upd_str

        if(len(stamt) == 0):
            self.__sql = self.__sql + ' ' + self.__where_str + ';'
        else:
            self.__sql = self.__sql + ' WHERE ' + stamt + ';'

    def delete(self, tname, stamt=''):
        self.__sql = 'DELETE FROM ' + tname
        if(len(stamt) == 0):
            self.__sql = self.__sql + ' ' + self.__where_str + ';'
        else:
            self.__sql = self.__sql + ' WHERE ' + stamt + ';'
    
    def reset_query(self):
        self.__sql = ''
        self.__sel_str = ''
        self.__from_str = ''
        self.__where_str = ''
        self.__group_str = ''
        self.__distince = ''

    def execute(self):
        return self.__cursor.execute(self.__sql)

    def empty_table(self, tname):
        self.__sql = 'DELETE FROM ' + tname + ';'

    def truncate(self, tname):
        self.__sql = 'TRUNCATE ' + tname + ';'

    def get_compiled_delete(self):
        return self.__sql

    def get_compiled_update(self):
        return self.__sql

    def get_compiled_insert(self):
        return self.__sql
    
    def get_compiled_select(self):
        return self.__sql

    # private
    # cond compile format : key = `val`, key > `val`
    def __cond_compile(self, key, val):
        cond = ''
        if(key[-1] == '=' or key[-1] == '<' or key[-1] == '>'):
            cond = ' ' + key + ' ' + val
        else:
            cond =  ' ' + key + ' = ' + val
        return cond

    # private
    # IN format: IN (`col1`, `col2`, ...)
    def __in_statement(self, cols):
        st = ' IN ('
        for item in cols:
            st = st + '`' + item + '`,'
        st = st[:-1]    #remove last ,
        st += ')'
        return st

    # private
    # wilcard format: %val, val%, %val%
    def __wildcard(sel, val, pos='both'):
        match_w = pos
        if(pos == 'before'):
            match_w = '%' + match_w
        elif(pos == 'after'):
            match_w += '%'
        else:
            match_w = '%' + match_w + '%'

        return match_w




# class StockStatus():
    
#     def __init__(self):
#         self.__tablename__ = 'stock_status'

#         sql = '''CREATE TABLE `STOCK`.`stock_status` ( 
#                 `stock_num` INT NOT NULL,
#                 `last_update` DATETIME NULL,
#                 PRIMARY KEY (`stock_num`));'''
        
#     def query(self, st_num, cursor):
#         sql = "SELECT * FROM " + self.__tablename__ + \
#               " WHERE stock_num = " + str(st_num) + ";"
#         cursor.execute(sql)
#         # print(sql)
#         return cursor
        
#     def insert(self, st_num, cursor):
#         sql = "INSERT INTO `STOCK`.`" + self.__tablename__ + "` (`stock_num`, `last_update`) " + \
#               "VALUES (" + str(st_num) + ", now());"
#         res = self.query(st_num, cursor).fetchone()
#         # print(sql)
#         if(res is None):
#             cursor.execute(sql)
#         else:
#             self.update(st_num, cursor)
#         # print(sql)

#     def update(self, st_num, cursor):
#         sql = "UPDATE `STOCK`.`" + self.__tablename__ + "`" +\
#               " SET `last_update` = now() " + \
#               " WHERE `stock_num` = " + str(st_num) + ';'
#         cursor.execute(sql)
#         # print(sql)

#     def delete(self, st_num, cursor):
#         sql = "DELETE FROM `STOCK`.`" + self.__tablename__ + "`" + \
#               " WHERE `stock_num` = " + str(st_num) + ";"
#         cursor.execute(sql)
#         # print(sql)

# class StockDividen():

#     def __init__(self):
#         self.__tablename__ = 'stock_dividen'

#         sql = '''CREATE TABLE `stock_dividen` (
#                     `index` int(11) NOT NULL AUTO_INCREMENT,
#                     `stock_num` int(11) NOT NULL,
#                     `div_date` date NOT NULL,
#                     `div_price` double NOT NULL,
#                     `div_amount` double NOT NULL,
#                     `div_precent` double NOT NULL,
#                     PRIMARY KEY (`index`)
#                 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;'''
    
#     def query(self):
#         self.__sql = "SELECT * FROM `STOCK`.`" + self.__tablename__ + "`"
    
#     def insert(self):
#         self.__sql = "INSERT INTO `STOCK`.`" + self.__tablename__ + "`"
    
#     def update(self):
#         self.__sql = "UPDATE `STOCK`.`" + self.__tablename__ + "`"
    
#     def delete(self):
#         self.__sql = "DELETE FROM `STOCK`.`" + self.__tablename__ + "`"
    
#     def where(self, param):
#         pass