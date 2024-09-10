import sqlite3
from pprint import pp

class Database():
    def __init__(self, name):
        self.name = name
        self.connection = sqlite3.connect('database/%s.db' % name)

    def __del__(self):
        self.connection.commit()
        self.connection.close()

    def addTable(self, table=None, params=None):
        cursor = self.connection.cursor()
        execString = "CREATE TABLE IF NOT EXISTS '"+table+"' "
        execString += " ("+",".join(params)+")"
        # execString += " ("+",".join(["'"+x+"'" for x in params])+")"
        cursor.execute(execString)
        cursor.close()
    
    def getTableNames(self):
        cursor = self.connection.cursor()
        names = [ x[0] for x in cursor.execute("SELECT name FROM sqlite_schema WHERE type='table'")]
        cursor.close()
        return names

    def insertOrReplace(self, table=None, params=None, values=None):
        if isinstance(values, tuple):
            if len(params) != len(values): return
        elif isinstance(values, list):
            if len(params) != len(values[0]): return
        else: return

        cursor = self.connection.cursor()
        execString = "INSERT OR REPLACE INTO '"+table+"'"
        execString += " ("+",".join(["'"+x+"'" for x in params])+")"
        execString += " VALUES"
        execString += " ("+",".join(['?']*len(params))+")"
        if isinstance(values, tuple):
            cursor.execute(execString, values)
        elif isinstance(values, list):
            cursor.executemany(execString, values)
        cursor.close

    def getRows(self, table=None, params=None):
        if not table in self.getTableNames():
            return [], []
        cursor = self.connection.cursor()
        if params == None:
            paramsString = "*"
        else:
            paramsString = ",".join(params)
        execString = "SELECT "+paramsString+" FROM '"+table+"'"
        dataFound = cursor.execute(execString)
        paramsFound = tuple([x[0] for x in dataFound.description])
        valuesFound = dataFound.fetchall()

        cursor.close
        return valuesFound, paramsFound

    def getMaxValues(self, tables=None, params=None):
        cursor = self.connection.cursor()

        execString = "SELECT MAX("+"),MAX(".join(params)+") FROM '"
        values = []
        for table in tables:
            values.append(cursor.execute(execString+table+"'").fetchall()[0])

        cursor.close
        return values
