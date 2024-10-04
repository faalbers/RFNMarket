import sqlite3, json
from pprint import pp
import pandas as pd
import numpy as np
from . import log

# https://www.sqlite.org/lang_select.html

class Database():
    __sqlDataTypes = {
        int:  'INTEGER',
        float:  'REAL',
        bool:  'BOOLEAN',
        str: 'TEXT',
        dict: 'JSON',
        list: 'JSON',
    }
    __sqlDataTypesPD = {
        np.int64:  'INTEGER',
        np.float64:  'REAL',
        float:  'REAL',
        bool:  'BOOLEAN',
        str: 'TEXT',
        dict: 'JSON',
        list: 'JSON',
    }
    def __init__(self, name):
        self.name = name
        self.connection = sqlite3.connect('database/%s.db' % name)
        log.info('Database: open  : %s' % self.name)

    def __del__(self):
        self.connection.commit()
        log.info('Database: del commit: %s' % self.name)
        self.connection.close()
        log.info('Database: del close : %s' % self.name)
    
    def close(self):
        self.connection.close()
        self.connection = None
        log.info('Database: call close : %s' % self.name)

    def getConnection(self):
        return self.connection
    
    def getCursor(self):
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()
        log.info('Database: call commit: %s' % self.name)
       
    def getTableNames(self):
        cursor = self.connection.cursor()
        names = [ x[0] for x in cursor.execute("SELECT name FROM sqlite_schema WHERE type='table'")]
        cursor.close()
        return names
    
    def getTableInfo(self, tableName):
        if not self.tableExists(tableName): return None

        tableInfo = {
            'columns': [],
            'primaryKeyColumns': [],
            'columnTypes': {},
            'rows': 0,
            'sql': '',
        }

        cursor = self.connection.cursor()
        tableColumns = cursor.execute("PRAGMA table_info(%s)" % tableName).fetchall()
        rowCount = cursor.execute("SELECT COUNT(*) FROM %s" % tableName).fetchone()
        sql = cursor.execute("SELECT sql FROM sqlite_schema WHERE type='table' AND name='%s'" % tableName).fetchone()
        cursor.close()

        for tableColumn in tableColumns:
            # print(tableColumn)
            tableInfo['columns'].append(tableColumn[1])
            if tableColumn[5]: tableInfo['primaryKeyColumns'].append(tableColumn[1])
            tableInfo['columnTypes'][tableColumn[1]] = tableColumn[2]
        tableInfo['sql'] = sql[0]
        tableInfo['rows'] = rowCount[0]

        return tableInfo

    def tableRename(self, tableName, newTableName):
        if not self.tableExists(tableName): return
        cursor = self.connection.cursor()
        cursor.execute("ALTER TABLE '%s' RENAME TO '%s'" % (tableName, newTableName))
        cursor.close()
    
    def tableColumnRename(self, tableName, columnName, newColumnName):
        if not self.tableColumnExists(tableName, columnName): return
        cursor = self.connection.cursor()
        cursor.execute("ALTER TABLE '%s' RENAME COLUMN [%s] TO '%s'" % (tableName, columnName, newColumnName))
        cursor.close()
    
    def tableColumnDrop(self, tableName, columnName):
        if not self.tableColumnExists(tableName, columnName): return
        cursor = self.connection.cursor()
        cursor.execute("ALTER TABLE '%s' DROP COLUMN [%s]" % (tableName, columnName))
        cursor.close()

    def tableExists(self, tableName):
        return tableName in self.getTableNames()
    
    def getTableColumnNames(self, tableName):
        if not self.tableExists(tableName): return []
        cursor = self.connection.cursor()
        tableInfo = cursor.execute("PRAGMA table_info(%s)" % tableName).fetchall()
        cursor.close()
        return [x[1] for x in tableInfo]

    def tableColumnExists(self, tableName, columnName):
        return columnName in self.getTableColumnNames(tableName)

    def getTable(self, tableName):
        if not self.tableExists(tableName): return pd.DataFrame()
        return pd.read_sql("SELECT * FROM '%s'" % tableName, self.connection)

    def idxTableGetKeys(self, tableName):
        # check if table exists
        if not self.tableExists(tableName): return []
        
        cursor = self.connection.cursor()
        # find keys column and column types
        tableColumns = cursor.execute("PRAGMA table_info(%s)" % tableName).fetchall()
        keyName = None
        for columnInfo in tableColumns:
            if columnInfo[5] == 1:
                keyName = columnInfo[1]
                break
        if keyName == None:
            return []

        keyValues = cursor.execute("SELECT %s FROM %s" % (keyName, tableName)).fetchall()
        cursor.close()
        keyValues = set([x[0] for x in keyValues])

        return keyValues
    
    def idxTableReadData(self, tableName, keyValues=[], columns=[], dataType='dataframe'):
        # dataType is 'dataframe' or 'dict'
        data = None
        if dataType == 'dataframe':
            data = pd.DataFrame()
        elif dataType == 'dataframe':
            data = {}

        # return emty data if table does not exist
        if not self.tableExists(tableName): return data
        
        cursor = self.connection.cursor()
        
        # find keys column and column types
        tableColumns = cursor.execute("PRAGMA table_info(%s)" % tableName).fetchall()
        keyName = None
        sqlDTypes = {}
        for columnInfo in tableColumns:
            sqlDTypes[columnInfo[1]] = columnInfo[2]
            if columnInfo[5] == 1:
                keyName = columnInfo[1]
        if keyName == None:
            return data
        
        if len(columns) == 0:
            columnsString = '*'
        else:
            columnsString = ','.join(['[%s]'%keyName]+['[%s]'%x for x in columns])
        
        execString = "SELECT %s FROM %s" % (columnsString, tableName)
        if len(keyValues) > 0:
            keysString = ','.join(['?']*len(keyValues))
            execString += " WHERE [%s] IN (%s)" % (keyName, keysString)
            result = cursor.execute(execString, tuple(keyValues))
        else:
            result = cursor.execute(execString)
        
        # get columnNames and rowValues
        columnNames = [x[0] for x in result.description]
        rowValues = result.fetchall()
        dataRows = []
        for values in rowValues:
            rowData = {}
            index = 0 
            for columnName in columnNames:
                if sqlDTypes[columnName] == 'JSON':
                    rowData[columnName] = json.loads(values[index])
                else:
                    rowData[columnName] = values[index]
                index += 1
            if dataType == 'dict':
                key = rowData.pop(keyName)
                data[key] = rowData
            if dataType == 'dataframe':
                dataRows.append(rowData)

        if dataType == 'dataframe':
            data = pd.DataFrame(dataRows)
            if len(data) != 0:
                data.set_index(keyName, inplace=True)

        return data

    def idxTableWriteTable(self, data, tableName, keyName=None, method='append'):
        # methodes are update, append, replace

        sqlDTypes = {}
        # get sql types for columns
        if isinstance(data, list):
            if keyName == None:
                raise ValueError('idxTableWriteTable: keyName not entered with dict data')
            # handle 
            for row in data:
                for columnName, value in row.items():
                    sqlDTypes[columnName] = self.__sqlDataTypes[type(value)]
            keySqlDType = sqlDTypes.pop(keyName)
            data = pd.DataFrame(data)
            data.set_index(keyName, inplace=True)
        elif isinstance(data, pd.DataFrame):
            keyName = data.index.name
            keySqlDType = self.__sqlDataTypesPD[data.index.dtype.type]
            for columnName in data.columns:
                cType = data[columnName].dtype.type
                if cType == np.object_:
                    colCheck = data[columnName].dropna()
                    if len(colCheck) == 0: continue
                    cType = type(colCheck.iloc[0])
                sqlDTypes[columnName] = self.__sqlDataTypesPD[cType]

        else:
            raise TypeError('idxTableWriteTable: wrong data type: %s' % type(data))

        # prepare data frame
        for columnName, sqlType in sqlDTypes.items():
            if sqlType == 'JSON':
                # chamge column into JSON string if needed
                data[columnName] = data[columnName].apply(json.dumps)
               
        cursor = self.connection.cursor()
        
        # drop table first if we need replace
        if method == 'replace' and self.tableExists(tableName):
            cursor.execute("DROP TABLE '%s'" % tableName)

        # only create table if not exist, add primary key column
        cursor.execute("CREATE TABLE IF NOT EXISTS %s  ([%s] %s PRIMARY KEY)" % (tableName, keyName, keySqlDType))

        # table columns status as we build
        tableColumns = cursor.execute("PRAGMA table_info(%s)" % tableName).fetchall()
        tableColumns = set([x[1] for x in tableColumns])

        # add table columns if neeed
        addTableColumns = set(sqlDTypes.keys()).difference(tableColumns)
        for columnName in addTableColumns:
            cursor.execute("ALTER TABLE %s ADD COLUMN [%s] %s" % (tableName, columnName, sqlDTypes[columnName]))
            # add column name to the table column status
            tableColumns.add(columnName)

        # apply method to dataframe
        if method in ['append', 'update']:
            dfTableDB = self.idxTableReadData(tableName)
            if len(dfTableDB) == 0:
                data.to_sql(tableName, self.connection, if_exists='append', index=True)
            elif method == 'append':
                data = data[~data.index.isin(dfTableDB.index)]
                if len(data) > 0:
                    data.to_sql(tableName, self.connection, if_exists='append', index=True)
            elif method == 'update':
                # not impleneted yet
                pass
        elif method == 'replace':
            data.to_sql(tableName, self.connection, if_exists='append', index=True)

        cursor.close()

    def idxTableWriteRow(self, data, tableName, keyName, keyValue, method='update'):
        # data is only dict for now
        cursor = self.connection.cursor()

        # get key info
        keySqlDType = self.__sqlDataTypes[type(keyValue)]

        # only create if not exist, add primary key column
        cursor.execute("CREATE TABLE IF NOT EXISTS %s  ([%s] %s PRIMARY KEY)" % (tableName, keyName, keySqlDType))

        # table columns status as we build
        tableColumns = cursor.execute("PRAGMA table_info(%s)" % tableName).fetchall()
        tableColumns = set([x[1] for x in tableColumns])

        # table indices as we build
        keyValues = cursor.execute("SELECT %s FROM %s" % (keyName, tableName)).fetchall()
        keyValues = set([x[0] for x in keyValues])

        # decide early if we need to do something
        if method == 'append' and keyValue in keyValues:
            cursor.close()
            return

        # check for JSON columns and check if we need to add columns
        addTableColumns = set(data.keys()).difference(tableColumns)
        columns = []
        values = []
        for columnName, value in data.items():
            # skip None values
            if value == None: continue

            # get sql data type
            sqlDType = self.__sqlDataTypes[type(value)]

            # create the column if needed 
            if columnName in addTableColumns:
                cursor.execute("ALTER TABLE %s ADD COLUMN [%s] %s" % (tableName, columnName, sqlDType))
                # add column name to the table column status
                tableColumns.add(columnName)

            # fill in column
            columns.append(columnName)

            # if sql type is JSON, we have to json dump to a string
            if sqlDType == 'JSON': value = json.dumps(value)

            # fill in value
            values.append(value)

        if keyValue in keyValues:
            columnsString = ','.join('[%s]'%x for x in columns)
            valHolderString = ','.join(['?']*len(columns))
            execString = "UPDATE %s SET (%s) = (%s) WHERE [%s] = '%s'" % (tableName, columnsString, valHolderString, keyName, keyValue)
            cursor.execute(execString, tuple(values))
        else:
            columns = [keyName]+columns
            values = [keyValue]+values
            columnsString = ','.join('[%s]'%x for x in columns)
            valHolderString = ','.join(['?']*len(columns))
            execString = "INSERT OR IGNORE INTO %s (%s) VALUES (%s)" % (tableName, columnsString, valHolderString)
            cursor.execute(execString, tuple(values))

        cursor.close()
