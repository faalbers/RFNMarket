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
        cursor.execute(execString)
        cursor.close()
    
    def insertOrReplace(self, table=None, params=None, values=None):
        if len(params) != len(values): return

        cursor = self.connection.cursor()
        execString = "INSERT OR REPLACE INTO '"+table+"'"
        execString += " ("+",".join(params)+")"
        execString += " VALUES"
        execString += " ("+",".join(['?']*len(params))+")"
        if isinstance(values, tuple):
            cursor.execute(execString, values)
        elif isinstance(values, list):
            cursor.executemany(execString, values)
        cursor.close

    def getRows(self, table=None, params=None):
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
