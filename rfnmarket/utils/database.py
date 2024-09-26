import sqlite3, re
from pprint import pp
import pandas as pd
from . import log

# https://www.sqlite.org/lang_select.html

class Database():
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

    def tableExists(self, tableName):
        cursor = self.connection.cursor()
        execString = "SELECT name FROM sqlite_master WHERE type='table' AND name='%s'" % tableName
        if cursor.execute(execString).fetchone() != None: return True
        return False

    def getColumnNames(self, tableName):
        if not self.tableExists(tableName): return pd.DataFrame()
        df = pd.read_sql("SELECT * FROM '%s' LIMIT 1" % tableName, self.connection)
        columns = list(df)
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
        print(execString)
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
        print(execString)
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

    def addColumn(self, tableName, column, type):
        if not self.tableExists(tableName) or self.columnExists(tableName, column): return
        cursor = self.connection.cursor()
        execString = "ALTER TABLE '%s' ADD COLUMN '%s' %s" % (tableName, column, type)
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

    def insertOrIgnore(self, table, columns, values):
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

def createDatabase(name, createDict):
    db = Database(name)
    for tableName, tableParams in createDict.items():
        db.createTable(tableName, tableParams)


