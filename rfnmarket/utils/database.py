import sqlite3
from pprint import pp

class Database():
    def __init__(self, name):
        self.name = name
        self.connection = sqlite3.connect('database/%s.db' % name)

    def __del__(self):
        self.connection.commit()
        self.connection.close()

    def addTable(self, table, columns):
        cursor = self.connection.cursor()
        execString = "CREATE TABLE IF NOT EXISTS '"+table+"' "
        if len(columns) > 0:
            execString += " ("+",".join(columns)+")"
        cursor.execute(execString)
        cursor.close()
    
    def getTableNames(self):
        cursor = self.connection.cursor()
        names = [ x[0] for x in cursor.execute("SELECT name FROM sqlite_schema WHERE type='table'")]
        cursor.close()
        return names
    
    def getColumnNames(self, table):
        if not table in self.getTableNames():
            return []
        cursor = self.connection.cursor()
        foundData = cursor.execute("SELECT * FROM '%s' LIMIT 1" % table)
        columns = [ x[0] for x in foundData.description]
        cursor.close
        return columns

    def addColumnIfNotExists(self, table, column, type):
        if not table in self.getTableNames():
            return
        if self.columnExists(table, column): return
        
        cursor = self.connection.cursor()
        cursor.execute("ALTER TABLE '%s' ADD COLUMN '%s' %s" % (table, column, type))
        cursor.close()

    def columnExists(self, table, column):
        if not table in self.getTableNames():
            False
        columns = self.getColumnNames(table)
        return(column in columns)

    def update(self, table, id, idValue, columns, values):
        if not table in self.getTableNames(): return
        if not isinstance(values, tuple): return
        if len(columns) != len(values): return
        cursor = self.connection.cursor()
        execString = "UPDATE '"+table+"' SET "
        execString += " ("+",".join(["'"+x+"'" for x in columns])+")"
        execString += " ="
        execString += " ("+",".join(['?']*len(columns))+")"
        if isinstance(idValue, str):
            execString += " WHERE "+id+" = '"+idValue+"'"
        else:
            execString += " WHERE "+id+" = "+str(idValue)
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
    
    def getRows(self, table=None, columns=None):
        if not table in self.getTableNames():
            return [], []
        cursor = self.connection.cursor()
        if columns == None:
            paramsString = "*"
        else:
            paramsString = ",".join(columns)
        execString = "SELECT "+paramsString+" FROM '"+table+"'"
        dataFound = cursor.execute(execString)
        paramsFound = tuple([x[0] for x in dataFound.description])
        valuesFound = dataFound.fetchall()

        cursor.close
        return [list(x) for x in valuesFound], list(paramsFound)

    def getMaxValues(self, tables, columns):
        existingTables = self.getTableNames()

        cursor = self.connection.cursor()

        execString = "SELECT MAX("+"),MAX(".join(columns)+") FROM '"
        values = []
        for table in tables:
            if not table in existingTables: continue
            values.append(cursor.execute(execString+table+"'").fetchall()[0])

        cursor.close

        values = [x[0] for x in values]
        return values

def createDatabase(name, createDict):
    db = Database(name)
    for tableName, tableParams in createDict.items():
        db.addTable(tableName, tableParams)


