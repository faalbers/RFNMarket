import sqlite3, json
from pprint import pp
import pandas as pd
import numpy as np
from . import log

# https://www.sqlite.org/lang_select.html

class Database():
    __sqlDataTypes = {
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

    def getTableDtype(self, tableName):
        data = {}
        cursor = self.connection.cursor()
        result = cursor.execute("SELECT sql FROM sqlite_schema WHERE (type,name)=('table','%s')" % tableName).fetchone()
        if result == None:
            cursor.close()
            return data
        for dtype in result[0].split('\n')[1:-1]:
            dtype = dtype.strip().strip(',')
            splits = dtype.split()
            paramName = splits[0].strip('"')
            dtype = ' '.join(splits[1:])
            data[paramName] = dtype
        cursor.close()
        return data

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
        result = cursor.execute("SELECT sql FROM sqlite_schema WHERE type='table' AND name='%s'" % tableName).fetchone()
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
        
    def getColumnNames(self, tableName):
        columns = list(self.getSqlDataTypes(tableName).keys())
        columns.sort()
        return columns

    def columnExists(self, tableName, column):
        return (column in self.getColumnNames(tableName))

    def getSchema(self):
        cursor = self.connection.cursor()
        execString = "SELECT * FROM sqlite_schema"
        result = cursor.execute(execString)
        description = result.description
        data = result.fetchall()
        cursor.close()
        schemaList = []
        rowIndex = 0
        for row in data:
            columnIndex = 0
            schema = {}
            for value in row:
                schema[description[columnIndex][0]] = value
                columnIndex += 1
            rowIndex += 1
            schemaList.append(schema)
        return schemaList
    
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

    def test(self):
        # execString = "SELECT ('quoteType','longName') FROM 'quoteType' WHERE 'symbol' = 'VITAX'"
        execString = "SELECT 'quoteType','longname' FROM 'quoteType' WHERE ('symbol') = ('VITAX')"
        cursor = self.connection.cursor()
        cursor.execute(execString)
        cursor.close()


    def getMaxColumnValue(self, tableName, column=None):
        if column != None and self.tableExists(tableName):
            cursor = self.connection.cursor()
            execString = "SELECT MAX("+column+") FROM "+tableName
            result = cursor.execute(execString).fetchall()
            return result[0][0]
        return None

    def createTable(self, tableName, columns=None):
        if columns == None: return
        cursor = self.connection.cursor()
        execString = "CREATE TABLE IF NOT EXISTS '"+tableName+"' "
        if len(columns) > 0:
            execString += " ("+",".join(columns)+")"
        cursor.execute(execString)
        cursor.close()

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
    
    def deleteRow(self, tableName, whereColumns=[], areValues=[]):
        if not self.tableExists(tableName): return
        execString = "DELETE FROM '%s'" % tableName
        if len(whereColumns) > 0 and len(whereColumns) == len(areValues):
            cursor = self.connection.cursor()
            whereString = "[%s]" % whereColumns[0]
            areString = "'%s'" % areValues[0]
            if len(whereColumns) > 1:
                whereString = "(%s)"  % ','.join([("[%s]"%x) for x in whereColumns])
                areString = "(%s)"  % ','.join([("'%s'"%x) for x in areValues])
            execString += " WHERE %s = %s" % (whereString, areString)
            cursor.execute(execString)
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
        cursor.execute(execString, tuple(values))
        cursor.close


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

    def insertOrIgnore(self, tableName, columns=[], values=[]):
        if not self.tableExists(tableName):
            raise ValueError('Table does not exist')
        execString = "INSERT OR IGNORE INTO '"+tableName+"'"
        execString += " ("+",".join(["'"+x+"'" for x in columns])+")"
        execString += " VALUES"
        execString += " ("+",".join(['?']*len(columns))+")"
        cursor = self.connection.cursor()
        cursor.executemany(execString, values)
        cursor.close

    def insertOrIgnoreOld(self, table, columns, values):
        if not table in self.getTableNames():
            return
        if isinstance(values, tuple):
            if len(columns) != len(values): return
        elif isinstance(values, list):
            if len(columns) != len(values[0]): return
        else: return

        cursor = self.connection.cursor()
        execString = "INSERT OR IGNORE INTO '"+table+"'"
        execString += " ("+",".join(["'"+x+"'" for x in columns])+")"
        execString += " VALUES"
        execString += " ("+",".join(['?']*len(columns))+")"
        if isinstance(values, tuple):
            cursor.execute(execString, values)
        elif isinstance(values, list):
            cursor.executemany(execString, values)
        cursor.close

    def insertOrReplace(self, table, columns, values):
        if not table in self.getTableNames():
            return
        if isinstance(values, tuple):
            if len(columns) != len(values): return
        elif isinstance(values, list):
            if len(columns) != len(values[0]): return
        else: return

        cursor = self.connection.cursor()
        execString = "INSERT OR REPLACE INTO '"+table+"'"
        execString += " ("+",".join(["'"+x+"'" for x in columns])+")"
        execString += " VALUES"
        execString += " ("+",".join(['?']*len(columns))+")"
        if isinstance(values, tuple):
            cursor.execute(execString, values)
        elif isinstance(values, list):
            cursor.executemany(execString, values)
        cursor.close

    def addFromDatabase(self, name):
        cursor = self.connection.cursor()
        execString = "ATTACH DATABASE '%s' AS new_db" % ('database/%s.db' % name)
        cursor.execute(execString)
        execString = "SELECT name, sql FROM new_db.sqlite_schema WHERE type='table'"
        tables = cursor.execute(execString).fetchall()
        count = 0
        for table in tables:
            execString = table[1]
            execString = execString.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
            cursor.execute(execString)
            execString = 'INSERT OR IGNORE INTO %s SELECT * FROM new_db.[%s]' % (table[0], table[0])
            try:
                cursor.execute(execString)
            except:
                print(execString)
            count += 1
        cursor.close
        return count
    
    def test(self):
        symbols = self.getRows('stocklist', columns=['keySymbol'])
        foundSymbols = {}
        for symbol in symbols[0]:
            symbol = symbol[0]
            if not symbol.isalnum():
                if '.' in symbol: continue
                elif '-' in symbol: continue
                elif ':' in symbol:
                    items = symbol.split(':')
                    if not items[0] in foundSymbols:
                        foundSymbols[items[0]] = set()
                    foundSymbols[items[0]].add(items[1])
                else:
                    print(symbol)
                    break

        print(foundSymbols.keys())
        print(foundSymbols['I'])
    
    # ************** new data management *************
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

    def idxTableUpdateRow(self, tableName, indexName, index, updateColumns, updateValues, preClear=True):
        if preClear:
            self.idxTableClearRow(tableName, indexName, index)
        self.updateRow(tableName, updateColumns, updateValues, whereColumns=[indexName], areValues=[index])

    def idxTableAddIndices(self, tableName, indexName, indices):
        self.insertOrIgnore(tableName, [indexName], [(i,) for i in indices])

    def idxTableAddMissingColumns(self, tableName, columnTypes):
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
            cType = cTypes.pop()
        return self.__sqlDataTypes[cType]
    
    def writeData(self, data, tableName, index=None, axis=0, preClear=True):
        columnTypes = {}
        indices = []
        indexName = None
        indexType = None

        if isinstance(data, pd.DataFrame):
            with pd.option_context('future.no_silent_downcasting', True):
                data = data.fillna(value=np.nan)
            # gather data frame info
            indices = list(data.index)
            indexName = data.index.name
            indexType = self.__getColumnSqlType(data.index)
            # get column data types and match them with sql data types
            for column in data.columns:
                columnTypes[column] = self.__getColumnSqlType(data[column])
        elif isinstance(data, pd.Series):
            with pd.option_context('future.no_silent_downcasting', True):
                data = data.fillna(value=np.nan).dropna()

            # gather series info
            if index == None:
                raise ValueError('index name must be given when using pandas.Series')
            indices = [data[index]]
            indexName = index
            indexType = self.__getColumnSqlType(data[index])
            data.pop(index)
            # get column data types and match them with sql data types
            for column in data.index:
                columnTypes[column] = self.__getColumnSqlType(data[column])
        
        # create table if it does not exist
        self.idxTableCreate(tableName, indexName, indexType, columnTypes)

        # add missing columns
        self.idxTableAddMissingColumns(tableName, columnTypes)

        # add new indices if needed
        self.idxTableAddIndices(tableName, indexName, indices)

        # update table values
        if isinstance(data, pd.DataFrame):
            for index, values in data.iterrows():
                # get list of columns and their values to be updated
                updateColumns = []
                updateValues = []
                for param in values.dropna().index:
                    updateColumns.append(param)
                    if columnTypes[param] == 'JSON':
                        # do a json dump string if needed
                        updateValues.append(json.dumps(values[param]))
                    else:
                        updateValues.append(values[param])
                self.idxTableUpdateRow(tableName, indexName, index, updateColumns, updateValues, preClear=preClear)
        elif isinstance(data, pd.Series):
            # get list of columns and their values to be updated
            updateColumns = []
            updateValues = []
            for param in data.dropna().index:
                updateColumns.append(param)
                if columnTypes[param] == 'JSON':
                    # do a json dump string if needed
                    updateValues.append(json.dumps(data[param]))
                else:
                    updateValues.append(data[param])
            self.idxTableUpdateRow(tableName, indexName, indices[0], updateColumns, updateValues, preClear=preClear)


