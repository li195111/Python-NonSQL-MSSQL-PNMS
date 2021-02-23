from typing import List
from enum import Enum

import pyodbc

class BaseMethod(Enum):
    SELECT = 0
    INSERT = 1
    UPDATE = 2
    DELETE = 3

class Method(Enum):
    FROM = 0
    INTO = 1
    SET = 2
    WHERE = 3
    VALUES = 4
    AND = 5
    OR = 6
    

class Database:
    def __init__(self, driver='{ODBC Driver 17 for SQL Server}', server='(localdb)\MSSQLLocalDB', database_name='master', username=None, password=None) -> None:        
        self.driver = driver
        self.server = server
        self.database = database_name
        connect_string = [f'DRIVER={self.driver}',
                          f'SERVER={self.server}',
                          f'DATABASE={self.database}']
        
        self.userId = username
        if self.userId:
            connect_string.append(f'UID={self.userId}')
        self.password = password
        if self.password:
            connect_string.append(f'PWD={self.password}')
        
        self.connect_str = ';'.join(connect_string)

    def __database_table(self, name:str)->str:
        return "dbo."+ name
    
    def __condition_string(self, conditions, return_values=False):
        sql = f' {Method.WHERE.name} '
        values = []
        for idx, (k,v) in enumerate(conditions.items()):
            sql += f"{k} = ?" if return_values else f"{k}='{v}'"
            if len(conditions) > 1 & idx != len(conditions)-1:
                sql += f" {Method.AND.name} "
            values.append(v)
        if return_values:
            return sql, values    
        return sql
    
    def __select_items_condition_sql(self, table:str, items:list, conditions:dict=None, base_method:BaseMethod=BaseMethod.SELECT):
        if isinstance(items,str):
            pass
        if isinstance(items,list) or isinstance(items, tuple):
            items = ','.join(items)
        else: raise ValueError("'item' value must be string or list of string or tuple of string")
        
        sql = f"{base_method.name} {items} {Method.FROM.name} {table}"
        if conditions != None:
            if isinstance(conditions, dict) and len(conditions) > 0:
                sql += self.__condition_string(conditions, return_values=False)
            else:
                raise ValueError("'conditions' value must be condition dict")
        return sql
    
    def select_items(self, table:str, items:list, conditions:dict=None):
        sql = self.__select_items_condition_sql(table, items, conditions)
        rows = self.__execute(sql, fetchall=True)
        return rows
    
    def __update_item(self, table, update_item, update_value, conditions:dict=None, base_method:BaseMethod=BaseMethod.UPDATE):
        sql = f"{base_method.name} {table} {Method.SET.name} {update_item} = ?"
        values = [update_value]
        if isinstance(conditions, dict) and len(conditions) > 0:
            condition_sql, condition_values = self.__condition_string(conditions, return_values=True)
            sql += condition_sql
            values += condition_values
            self.__execute(sql, values)
            
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
        sql = f"{base_method.name} {Method.INTO.name} {table} ({items}) {Method.VALUES.name} (" + ",".join(["?"] * len(values)) + ")"
        return sql
    
    def insert_item(self, table, items, values):
        sql = self.__insert_item_sql(table, items, values)
        self.__execute(sql, values) 
    
    def __delete_item_condition(self, table, conditions:dict, base_method:BaseMethod=BaseMethod.DELETE):
        if isinstance(conditions, dict) and len(conditions) > 0:
            sql = f'{base_method.name} {Method.FROM.name} {table}'
            condition_sql, values = self.__condition_string(conditions, return_values=True)
            sql += condition_sql
            self.__execute(sql, values)
        else:
            raise ValueError("'conditions' value must be condition dict and at least one condition")
    
    def delete_item(self, table:str, conditions:dict):
        return self.__delete_item_condition(table, conditions)
    
    def __execute(self,sql, *args, many=False, fetchall=False):
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()
            if len(args) > 0:
                if many:
                    cursor.executemany(sql,*args)
                else:
                    cursor.execute(sql,*args)
            else:
                cursor.execute(sql)
            cursor.commit()
            if fetchall:
                return cursor.fetchall()
            
if __name__ == "__main__":
    pass