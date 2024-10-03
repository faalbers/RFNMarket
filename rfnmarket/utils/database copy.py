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
        int:  'INTEGER',
        np.int64:  'INTEGER',
        np.float64:  'REAL',
        np.bool:  'BOOLEAN',
        float: 'FLOAT',
        str: 'TEXT',
        bool: 'BOOLEAN',
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

    # def getTableDtype(self, tableName):
    #     data = {}
    #     cursor = self.connection.cursor()
    #     result = cursor.execute("SELECT sql FROM sqlite_schema WHERE (type,name)=('table','%s')" % tableName).fetchone()
    #     if result == None:
    #         cursor.close()
    #         return data
    #     for dtype in result[0].split('\n')[1:-1]:
    #         dtype = dtype.strip().strip(',')
    #         splits = dtype.split()
    #         paramName = splits[0].strip('"')
    #         dtype = ' '.join(splits[1:])
    #         data[paramName] = dtype
    #     cursor.close()
    #     return data

    def getColumnNames(self, tableName):
        columns = list(self.getSqlDataTypes(tableName).keys())
        columns.sort()
        return columns

    def columnExists(self, tableName, column):
        return (column in self.getColumnNames(tableName))

    # def getSchema(self):
    #     cursor = self.connection.cursor()
    #     execString = "SELECT * FROM sqlite_schema"
    #     result = cursor.execute(execString)
    #     description = result.description
    #     data = result.fetchall()
    #     cursor.close()
    #     schemaList = []
    #     rowIndex = 0
    #     for row in data:
    #         columnIndex = 0
    #         schema = {}
    #         for value in row:
    #             schema[description[columnIndex][0]] = value
    #             columnIndex += 1
    #         rowIndex += 1
    #         schemaList.append(schema)
    #     return schemaList
    
    def getRows(self, tableName, columns=[], whereColumns=[], areValues=[]):
        if self.tableExists(tableName):
            columnsString = '*'
            if len(columns) > 0:
                columnsString = ','.join([("[%s]"%x) for x in columns])
            execString = "SELECT %s FROM '%s'" % (columnsString, tableName)
            if len(whereColumns) > 0 and len(whereColumns) == len(areValues):
                whereString = "[%s]" % whereColumns[0]
                areString = "'%s'" % areValues[0]
                if len(whereColumns) > 1:
                    whereString = "(%s)"  % ','.join([("[%s]"%x) for x in whereColumns])
                    areString = "(%s)"  % ','.join([("'%s'"%x) for x in areValues])
                execString += " WHERE %s = %s" % (whereString, areString)
            print(execString)
            cursor = self.connection.cursor()
            dataFound = cursor.execute(execString)
            paramsFound = [x[0] for x in dataFound.description]
            valuesFound = [list(x) for x in dataFound.fetchall()]
            cursor.close()
            if len(valuesFound) == 0: paramsFound = []
            return valuesFound, paramsFound
        return [], []

    def getColumn(self, tableName, columnName):
        # pd.read_sql("SELECT date FROM '%s'" % type, self.db.getConnection())['date']
        execString = "SELECT [%s] FROM '%s'" % (columnName, tableName)
        return pd.read_sql(execString, self.connection)[columnName]

    def getTable(self, tableName, columns=[], whereColumns=[], areValues=[]):
        columnsString = '*'
        if len(columns) > 0:
            columnsString = ','.join([("[%s]"%x) for x in columns])
        execString = "SELECT %s FROM '%s'" % (columnsString, tableName)
        if len(whereColumns) > 0 and len(whereColumns) == len(areValues):
            whereString = "[%s]" % whereColumns[0]
            areString = "'%s'" % areValues[0]
            if len(whereColumns) > 1:
                whereString = "(%s)"  % ','.join([("[%s]"%x) for x in whereColumns])
                areString = "(%s)"  % ','.join([("'%s'"%x) for x in areValues])
            execString += " WHERE %s = %s" % (whereString, areString)
        return pd.read_sql(execString, self.connection)

    def getMaxColumnValue(self, tableName, column=None):
        if column != None and self.tableExists(tableName):
            cursor = self.connection.cursor()
            execString = "SELECT MAX("+column+") FROM "+tableName
            result = cursor.execute(execString).fetchall()
            return result[0][0]
        return None


    def dropTable(self, tableName):
        if not self.tableExists(tableName): return
        cursor = self.connection.cursor()
        execString = "DROP TABLE '"+tableName+"' "
        cursor.execute(execString)
        cursor.close()

    def addColumn(self, tableName, columnName, sqlType):
        if not self.tableExists(tableName) or self.columnExists(tableName, columnName): return
        cursor = self.connection.cursor()
        execString = 'ALTER TABLE "%s" ADD COLUMN "%s" %s' % (tableName, columnName, sqlType)
        cursor.execute(execString)
        cursor.close()
    
    
    # def deleteRow(self, tableName, whereColumns=[], areValues=[]):
    #     if not self.tableExists(tableName): return
    #     execString = "DELETE FROM '%s'" % tableName
    #     if len(whereColumns) > 0 and len(whereColumns) == len(areValues):
    #         cursor = self.connection.cursor()
    #         whereString = "[%s]" % whereColumns[0]
    #         areString = "'%s'" % areValues[0]
    #         if len(whereColumns) > 1:
    #             whereString = "(%s)"  % ','.join([("[%s]"%x) for x in whereColumns])
    #             areString = "(%s)"  % ','.join([("'%s'"%x) for x in areValues])
    #         execString += " WHERE %s = %s" % (whereString, areString)
    #         cursor.execute(execString)
    #         cursor.close



    def update(self, table, id, idValue, columns, values):
        if not table in self.getTableNames(): return
        if not isinstance(values, tuple): return
        if len(columns) != len(values): return
        cursor = self.connection.cursor()
        execString = "UPDATE '"+table+"' SET "
        execString += " ("+','.join([("[%s]"%x) for x in columns])+")"
        execString += " ="
        execString += " ("+",".join(['?']*len(columns))+")"
        if isinstance(idValue, str):
            execString += " WHERE "+id+" = '"+idValue+"'"
        else:
            execString += " WHERE "+id+" = '"+str(idValue)+"'"
        cursor.execute(execString, values)
        cursor.close


    # def insertOrIgnoreOld(self, table, columns, values):
    #     if not table in self.getTableNames():
    #         return
    #     if isinstance(values, tuple):
    #         if len(columns) != len(values): return
    #     elif isinstance(values, list):
    #         if len(columns) != len(values[0]): return
    #     else: return

    #     cursor = self.connection.cursor()
    #     execString = "INSERT OR IGNORE INTO '"+table+"'"
    #     execString += " ("+",".join(["'"+x+"'" for x in columns])+")"
    #     execString += " VALUES"
    #     execString += " ("+",".join(['?']*len(columns))+")"
    #     if isinstance(values, tuple):
    #         cursor.execute(execString, values)
    #     elif isinstance(values, list):
    #         cursor.executemany(execString, values)
    #     cursor.close

    # def insertOrReplace(self, table, columns, values):
    #     if not table in self.getTableNames():
    #         return
    #     if isinstance(values, tuple):
    #         if len(columns) != len(values): return
    #     elif isinstance(values, list):
    #         if len(columns) != len(values[0]): return
    #     else: return

    #     cursor = self.connection.cursor()
    #     execString = "INSERT OR REPLACE INTO '"+table+"'"
    #     execString += " ("+",".join(["'"+x+"'" for x in columns])+")"
    #     execString += " VALUES"
    #     execString += " ("+",".join(['?']*len(columns))+")"
    #     if isinstance(values, tuple):
    #         cursor.execute(execString, values)
    #     elif isinstance(values, list):
    #         cursor.executemany(execString, values)
    #     cursor.close

    # def addFromDatabase(self, name):
    #     cursor = self.connection.cursor()
    #     execString = "ATTACH DATABASE '%s' AS new_db" % ('database/%s.db' % name)
    #     cursor.execute(execString)
    #     execString = "SELECT name, sql FROM new_db.sqlite_schema WHERE type='table'"
    #     tables = cursor.execute(execString).fetchall()
    #     count = 0
    #     for table in tables:
    #         execString = table[1]
    #         execString = execString.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
    #         cursor.execute(execString)
    #         execString = 'INSERT OR IGNORE INTO %s SELECT * FROM new_db.[%s]' % (table[0], table[0])
    #         try:
    #             cursor.execute(execString)
    #         except:
    #             print(execString)
    #         count += 1
    #     cursor.close
    #     return count
    
    
    # ************** new data management *************
    





    
    def tableExists(self, tableName):
        cursor = self.connection.cursor()
        execString = "SELECT name FROM sqlite_schema WHERE type='table' AND name='%s'" % tableName
        if cursor.execute(execString).fetchone() != None:
            cursor.close()
            return True
        cursor.close()
        return False

    def getSqlDataTypes(self, tableName):
        sqlDataTypes = {}

        cursor = self.connection.cursor()
        execString = "SELECT sql FROM sqlite_schema WHERE type='table' AND name='%s'" % tableName
        print(execString)
        result = cursor.execute(execString).fetchone()
        if result != None:
            result = result[0].replace('\n', '').split('(')[1].strip(')')
            for columnData in result.split(','):
                columnData = columnData.strip()
                columnData = columnData.split()
                nameSql = columnData[0].strip('"')
                typeSql = ' '.join(columnData[1:])
                sqlDataTypes[nameSql] = typeSql

        cursor.close()
        return sqlDataTypes
    
    def createTable(self, tableName, columns=None):
        if columns == None: return
        cursor = self.connection.cursor()
        execString = "CREATE TABLE IF NOT EXISTS '"+tableName+"' "
        if len(columns) > 0:
            execString += " ("+",".join(columns)+")"
        print(execString)
        cursor.execute(execString)
        cursor.close()

    def addColumns(self, tableName, columnTypes):
        if self.tableExists(tableName):
            cursor = self.connection.cursor()
            for columnName, columnType in columnTypes.items():
                cursor.execute('ALTER TABLE "%s" ADD COLUMN "%s" %s' % (tableName, columnName, columnType))
            cursor.close()


        if not self.tableExists(tableName) or self.columnExists(tableName, columnName): return
        cursor = self.connection.cursor()
        execString = 'ALTER TABLE "%s" ADD COLUMN "%s" %s' % (tableName, columnName, sqlType)
        cursor.execute(execString)
        cursor.close()
    
    def insertOrIgnore(self, tableName, columns=[], values=[]):
        if not self.tableExists(tableName):
            raise ValueError('Table does not exist')
        execString = "INSERT OR IGNORE INTO '"+tableName+"'"
        execString += " ("+",".join(["'"+x+"'" for x in columns])+")"
        execString += " VALUES"
        execString += " ("+",".join(['?']*len(columns))+")"
        cursor = self.connection.cursor()
        print(execString)
        print(values)
        cursor.executemany(execString, values)
        cursor.close
        
    def updateRow(self, tableName, columns=[], values=tuple(), whereColumns=[], areValues=[]):
        if not self.tableExists(tableName):
            raise ValueError('Table does not exist')
        execString = "UPDATE '%s' SET " % tableName
        execString += " (%s)" % (','.join([("[%s]"%x) for x in columns]))
        execString += " ="
        execString += " (%s)" % (",".join(['?']*len(columns)))
        whereString = "(%s)"  % (','.join([("[%s]"%x) for x in whereColumns]))
        areString = "(%s)"  % (','.join([("'%s'"%x) for x in areValues]))
        execString += " WHERE %s = %s" % (whereString, areString)

        cursor = self.connection.cursor()
        print(execString)
        print(tuple(values))
        cursor.execute(execString, tuple(values))
        cursor.close
                
    def idxTableClearRow(self, tableName, indexName, index):
        dbColumnTypes = self.getSqlDataTypes(tableName)
        if not indexName in dbColumnTypes:
            raise ValueError('index column: "%s" not found in table: "%s"' % (indexName, tableName))
        dbColumnTypes.pop(indexName)
        columnsString = ','.join(['[%s]'%x for x in dbColumnTypes.keys()])
        valuesString = ','.join(['NULL'] * len(dbColumnTypes))
        execString = "UPDATE '%s' SET  (%s) = (%s) WHERE ([%s]) = ('%s')" % (tableName, columnsString, valuesString, indexName, index)
        cursor = self.connection.cursor()
        cursor.execute(execString)
        cursor.close()

        "UPDATE table_name SET column_name = NULL WHERE (Specify condition)"
        "UPDATE 'testA' SET  ([weather],[age],[weight],[family],[hobbies],[fat]) = (NULL,NULL,NULL,NULL,NULL,NULL) WHERE ([name]) = ('Frank')"

    def idxTableGetData(self, tableName, indexName, indices=[]):
        if not self.tableExists(tableName): return pd.DataFrame()
        execString = "SELECT * FROM '%s'" % tableName
        if len(indices) > 0:
            indicesString = ["'%s'"%x for x in indices]
            execString += " WHERE [%s] IN (%s)" % (indexName, ','.join(indicesString))
        with pd.option_context('future.no_silent_downcasting', True): # to remove warnings, not sure how long it will work
            print(execString)
            data = pd.read_sql(execString, self.connection, index_col=indexName).fillna(value=np.nan)
            # turn JSON and BOOLEAN data back to original data
            dbColumnTypes = self.getSqlDataTypes(tableName)
            for columnName, sqlType in dbColumnTypes.items():
                if sqlType == 'JSON':
                    data[columnName] = data[columnName].apply(lambda x: json.loads(x) if not pd.isna(x) else x)
                if sqlType == 'BOOLEAN':
                    data[columnName] = data[columnName].apply(lambda x: bool(x) if not pd.isna(x) else x)
        return data

    def idxTableGetIndices(self, tableName, indexName):
        values, params = self.getRows(tableName, columns=[indexName])
        return [x[0] for x in values]

    def idxTableAddIndices(self, tableName, indexName, indices):
        self.insertOrIgnore(tableName, [indexName], [(i,) for i in indices])
    
    def idxTableRemoveIndices(self, tableName, indexName, indices):
        execString = "DELETE FROM '%s'" % tableName
        if len(indices) > 0:
            indicesString = ["'%s'"%x for x in indices]
            execString += " WHERE [%s] IN (%s)" % (indexName, ','.join(indicesString))
        cursor = self.connection.cursor()
        print(execString)
        cursor.execute(execString)
        cursor.close()

    def idxTableAddColumns(self, tableName, columnTypes):
        # read table columns and check if it is an index column
        dbColumnTypes = self.getSqlDataTypes(tableName)
        indexColumn = None
        for columnName, columnType in dbColumnTypes.items():
            if columnType.endswith('PRIMARY KEY'):
                indexColumn = columnName
                break
        if indexColumn == None:
            raise ValueError('could not find index column in table: %s' % tableName)
        else:
            # pop out the index column
            dbColumnTypes.pop(indexColumn)
              
        # add new columns if needed
        columnsAdd = set(columnTypes.keys()).difference(dbColumnTypes)
        if len(columnsAdd) > 0:
            columnAddTypes = {}
            for columnName in columnsAdd:
                columnAddTypes[columnName] = columnTypes[columnName]
            self.addColumns(tableName, columnAddTypes)

    def idxTableCreate(self, tableName, indexName, indexType, columnTypes):
        tableColumns = []
        tableColumns.append('"%s" %s PRIMARY KEY' % (indexName, indexType))
        for columnName, columnType in columnTypes.items():
            tableColumns.append('"%s" %s' % (columnName, columnType))
        self.createTable(tableName, tableColumns)

    def __getColumnSqlType(self, column):
        if isinstance(column, pd.core.indexes.base.Index) or isinstance(column, pd.core.series.Series):
            cType = column.dtype.type
        else:
            cType = type(column)
        if cType == np.object_:
            cTypes = set([type(x) for x in column.dropna().values])
            if len(cTypes) > 1:
                raise TypeError('too many different types in column %s' % column.name)
            elif len(cTypes) == 0:
                return None
            cType = cTypes.pop()
        return self.__sqlDataTypesPD[cType]

    def idxTableUpdateRow(self, tableName, indexName, index, updateColumns, updateValues, preClear=False):
        if preClear:
            self.idxTableClearRow(tableName, indexName, index)
        self.updateRow(tableName, updateColumns, updateValues, whereColumns=[indexName], areValues=[index])

    def idxTableWriteRow(self, data, tableName, keyName, keyValue, method):
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
        if method == 'append' and keyValue in keyValues: return

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

    def idxTableWriteRowOld(self, data, tableName, indexLabel, indexLabelNew=None, method='append'):
        # methodes are 'append', 'update', 'replace'

        columnTypes = {}
        indices = []
        indexName = None
        indexType = None

        # print('\nOriginalData:')
        # print(data)

        # prepare DataFrame or Series for writing
        if isinstance(data, pd.Series):
            # drop all nans
            with pd.option_context('future.no_silent_downcasting', True): # to remove warnings, not sure how long it will work
                data = data.fillna(value=np.nan).dropna()

            data = data.to_frame().T

        # only indexed data
        if not indexLabel in data.columns:
            raise ValueError('pandas.DataFrame has no column: %s' % indexLabel)
        # set up index in DataFrame
        data.set_index(indexLabel, inplace=True)
        indexName = indexLabel
        if indexLabelNew != None:
            data.rename_axis(indexLabelNew, inplace=True)
            indexName = indexLabelNew
        # make sure index is unique
        if not data.index.is_unique:
            data = data[~data.index.duplicated(keep='first')]

        # handle method
        existingIndices = self.idxTableGetIndices(tableName, indexName)
        if method == 'append':
            # we only add what is not there yet
            data = data[~data.index.isin(existingIndices)]
        elif method == 'update':
            # we get what exists and overwrite only the non nan values with new values
            # probably never gonna be used , but hey
            dataExists = self.idxTableGetData(tableName, indexName, data.index)
            if len(dataExists) > 0:
                dataInDb = data[data.index.isin(dataExists.index)]
                dataNotInDb = data[~data.index.isin(dataExists.index)]
                for index, row in dataInDb.iterrows():
                    for param in row.dropna().index:
                        dataExists.loc[index, param] = dataInDb.loc[index, param]
                if len(dataNotInDb) == 0:
                    data = dataExists
                else:
                    data = pd.concat([dataExists, dataNotInDb])
        # with replace we do nothing to it

        # no point continuing if nothing to do
        if len(data) == 0: return

        # drop columns that have all nans
        data = data.dropna(axis=1, how='all')

        # set data info
        indices = list(data.index)
        indexType = self.__getColumnSqlType(data.index)

        # get column data types and match them with sql data types
        for column in data.columns:
            columnTypes[column] = self.__getColumnSqlType(data[column])
            # do a json dump string if needed
            if columnTypes[column] == 'JSON':
                for index in data[column].dropna().index:
                    data.loc[index, column] = json.dumps(data.loc[index, column])
            
        # print()
        # print('indexName: %s' % indexName)
        # print('indexType: %s' % indexType)
        # print('indices  : %s' % indices)
        # print('columnTypes:')
        # pp(columnTypes)

        # print('\nNew Data:')
        # print(data)

        # create table if it does not exist
        if method in ['append', 'update', 'replace']:
            self.idxTableCreate(tableName, indexName, indexType, columnTypes)

        # add missing columns
        if method in ['append', 'update', 'replace']:
            self.idxTableAddColumns(tableName, columnTypes)

        # update table values
        if len(data) > 1:
            # remove index rows if needed
            if method in ['update', 'replace']:
                self.idxTableRemoveIndices(tableName, indexName, indices)
            data.to_sql(tableName, self.connection, if_exists='append', index=True, dtype=columnTypes)
        else:
            # get list of columns and their values to be updated
            # this is way faster then using  data.to_sql
            updateColumns = []
            updateValues = []
            for param in data.loc[indices[0]].index:
                updateColumns.append(param)
                updateValues.append(data.loc[indices[0], param])
            preClear = False
            if method == 'replace': preClear = True
            self.idxTableAddIndices(tableName, indexName, indices)
            self.idxTableUpdateRow(tableName, indexName, indices[0], updateColumns, updateValues, preClear=preClear)


