import enum
from typing import List, Union
from enum import Enum

import pyodbc

class BaseMethod(Enum):
    CREATE = 0
    SELECT = 1
    INSERT = 2
    UPDATE = 3
    DELETE = 4
    DROP = 5

class DBObj(Enum):
    DATABASE = 0
    TABLE = 1

class Method(Enum):
    FROM = 0
    INTO = 1
    IF = 2
    SET = 3
    WHERE = 4
    VALUES = 5
    AND = 6
    OR = 7
    EQUAL = 8
    QUES_MARK = 9
    
class Mark(Enum):
    EQUAL = 0
    QUEST = 1
    EXCLAM = 2
    
class Database:
    def __init__(self, driver='{ODBC Driver 17 for SQL Server}', server='(localdb)\MSSQLLocalDB', database_name='master', username=None, password=None,
                 create_db_if_notexists:bool=True) -> None:        
        self.driver = driver
        self.driver_string = f'DRIVER={self.driver}'
        self.server = server
        self.server_string = f'SERVER={self.server}'
        self.database = database_name
        self.database_string = f'DATABASE={self.database}'
        connect_string = [self.driver_string,
                          self.server_string]
        self.connect_string_without_database = ';'.join(connect_string)
        self.connect_str = self.connect_string_without_database
        if create_db_if_notexists:
            self.check_and_create_database()
        else:
            if not self.check_database():
                raise ConnectionError(f"Not Exists Database name '{self.database}',Use 'create_db_if_notexists = True' to create database")
            connect_string.append(self.database_string)
        
        self.userId = username
        if self.userId:
            connect_string.append(f'UID={self.userId}')
        self.password = password
        if self.password:
            connect_string.append(f'PWD={self.password}')
        
        self.connect_str = ';'.join(connect_string)

    def __to_sql_string(self, sql_list:list) -> str:
        sql = []
        for sql_item in sql_list:
            if isinstance(sql_item,BaseMethod) or isinstance(sql_item,DBObj) or isinstance(sql_item,Method) or isinstance(sql_item, Mark):
                sql.append(sql_item.name)
            else:
                sql.append(sql_item)
        return " ".join(sql)

    def __edit_database(self, base_method):
        sql = [base_method, DBObj.DATABASE, "["+self.database+"]"]
        return self.__execute(self.__to_sql_string(sql),autocommit=True) 

    def create_database(self) -> None:
        return self.__edit_database(BaseMethod.CREATE)
    
    def drop_database(self):
        self.connect_str = self.connect_string_without_database
        if self.check_database():
            return self.__edit_database(BaseMethod.DROP)
        print (f"Database '{self.database}' not exists.")
    
    def check_database(self) -> bool:
        cond = {"name":self.database}
        result = self.select_items(self.__master_database(), "name", cond)
        if result:
            return True
        else:
            return False
        
    def list_database_name(self) -> List[str]:
        return self.select_items(self.__master_database(), "name")
        
    def check_and_create_database(self) -> None:
        if self.check_database():
            return
        return self.create_database()
    
    def __master_database(self) -> str:
        return "master.dbo.sysdatabases"
    
    def __database_table(self, name:str)->str:
        return "dbo."+ name
    
    def __condition_string(self, conditions, return_values=False):
        sql = [Method.WHERE]
        values = []
        for idx, (k,v) in enumerate(conditions.items()):
            if return_values:
                cond_sql = f"{k} = ?"
            else:
                cond_sql = f"{k}='{v}'"
            sql.append(cond_sql)
            if len(conditions) > 1 & idx != len(conditions)-1:
                sql.append(Method.AND)
            values.append(v)
        if return_values:
            return sql, values    
        return sql
    
    def __select_items_condition_sql(self, table:str, items:list, conditions:dict=None, base_method:BaseMethod=BaseMethod.SELECT):
        if isinstance(items,str):
            pass
        elif isinstance(items,list) or isinstance(items, tuple):
            items = ','.join(items)
        else: raise ValueError("'item' value must be string or list of string or tuple of string")
        sql = [base_method, items, Method.FROM, table]
        if conditions != None:
            if isinstance(conditions, dict) and len(conditions) > 0:
                sql.extend(self.__condition_string(conditions, return_values=False))
            else:
                raise ValueError("'conditions' value must be condition dict")
        return sql
    
    def select_items(self, table:str, items:Union[str,list,tuple], conditions:dict=None):
        sql = self.__select_items_condition_sql(table, items, conditions)
        return self.__execute(self.__to_sql_string(sql), fetchall=True)
    
    def __update_item(self, table, update_item, update_value, conditions:dict=None, base_method:BaseMethod=BaseMethod.UPDATE):
        sql = [base_method, table, Method.SET, update_item, "=", "?"]
        values = [update_value]
        if isinstance(conditions, dict) and len(conditions) > 0:
            condition_sql, condition_values = self.__condition_string(conditions, return_values=True)
            sql.extend(condition_sql)
            values.extend(condition_values)
            self.__execute(" ".join(sql), values)
        return
            
    def update_item(self, table:str, update_item:list, update_value:list, conditions:dict=None):
        self.__update_item(table, update_item, update_value, conditions)
        return
    
    def __insert_item_sql(self, table, items:list, values:list, base_method:BaseMethod=BaseMethod.INSERT):
        if isinstance(items,str):
            pass
        if isinstance(items, list) or isinstance(items, tuple):
            items = ','.join(items)
        else:
            raise ValueError("'item' value must be string or list of string or tuple")
        sql = [base_method, Method.INTO, table, "("+items+")", Method.VALUES, "("+",".join(["?"]*len(values))+")"]
        return sql
    
    def insert_item(self, table, items, values):
        sql = self.__insert_item_sql(table, items, values)
        self.__execute(self.__to_sql_string(sql), values) 
    
    def __delete_item_condition(self, table, conditions:dict, base_method:BaseMethod=BaseMethod.DELETE):
        if isinstance(conditions, dict) and len(conditions) > 0:
            sql = [base_method, Method.FROM, table]
            condition_sql, values = self.__condition_string(conditions, return_values=True)
            sql.extend(condition_sql)
            self.__execute(self.__to_sql_string(sql), values)
        else:
            raise ValueError("'conditions' value must be condition dict and at least one condition")
    
    def delete_item(self, table:str, conditions:dict):
        return self.__delete_item_condition(table, conditions)
    
    def __execute(self,sql, *args, many=False, fetchall=False, autocommit=False):
        with pyodbc.connect(self.connect_str, autocommit=autocommit) as conn:
            cursor = conn.cursor()
            if len(args) > 0:
                if many:
                    cursor.executemany(sql,*args)
                else:
                    cursor.execute(sql,*args)
            else:
                cursor.execute(sql)
                
            if fetchall:
                return cursor.fetchall()
            else:
                cursor.commit()
                
if __name__ == "__main__":
    db = Database(database_name="Hello", create_db_if_notexists=True)
    pass