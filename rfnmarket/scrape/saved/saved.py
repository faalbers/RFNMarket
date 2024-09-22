import glob, os
import pandas as pd
import sqlite3
from pathlib import Path
from ...utils import log, database
from datetime import datetime
from pprint import pp

class Saved():
    dbName = 'saved'

    def __init__(self, symbols=[], tables=[], forceUpdate=False):

        self.dataPath = 'database/'
        self.readCSV()
        self.readQuicken()

    def readQuicken(self):
        # get status first
        db = database.Database(self.dbName)
        fileDates , dataNames = db.getRows('status_db', whereColumns=['rowid'], areValues=[1])
        statusDates = {}
        index = 0
        for dataName in dataNames:
            statusDates[dataName] = fileDates[0][index]
            index += 1

        # find QIF files and parse them
        qifFiles = glob.glob(self.dataPath+'*.QIF')
        paramsCreate = ["'timestamp' TIMESTAMP", "'security' TEXT", "'symbol' TEXT", "'type' TEXT", "'transaction' TEXT",
            "'shares' FLOAT", "'price' FLOAT", "'costBasis' FLOAT", "'description' TEXT"]
        params = ['timestamp', 'security', 'symbol', 'type', 'transaction', 'shares', 'price', 'costBasis', 'description']
        writeValues = []
        for qifFile in qifFiles:
            fileDate = int(os.path.getmtime(qifFile))
            dataName = Path(qifFile).stem
            tableName = 'QUICKEN_'+dataName

            # only read data from newer files
            if tableName in statusDates and fileDate <= statusDates[tableName]: continue

            log.info('Update saved: %s' % dataName)
            
            db.dropTable(tableName)
            db.createTable(tableName, paramsCreate)
            timestamp = None
            securities = {}
            lineBefore = ''
            with open(qifFile, 'r') as f:
                line = f.readline()
                while line:
                    if line == '!Type:Security\n':
                        securityName = f.readline().strip('\n').lstrip('N')
                        if securityName != 'Cash' and securityName != 'CD':
                            securities[securityName] = {}
                            securities[securityName]['symbol'] = f.readline().strip('\n').lstrip('S')
                            securities[securityName]['type'] = f.readline().strip('\n').lstrip('T')
                    lineBefore = line
                    line = f.readline()
            lineBefore = ''
            with open(qifFile, 'r') as f:
                line = f.readline()
                while line:
                    if line.startswith('D') and '/' in line and line[1].isnumeric():
                        date = line.strip('\n').lstrip('D')
                        date = date.split('/')
                        month = int(date[0].strip())
                        date = date[1].split("'")
                        day = int(date[0].strip())
                        year = int(date[1].strip())+2000
                        timestamp = datetime(day=day, month=month, year=year).timestamp()
                    elif line.strip('\n').lstrip('Y') in securities:
                        values = {}
                        for param in params: values[param] = None
                        # log.info('')
                        # log.info(datetime.fromtimestamp(timestamp))
                        values['timestamp'] = int(timestamp)
                        securityName = line.strip('\n').lstrip('Y')
                        values['security'] = securityName
                        values['symbol'] = securities[securityName]['symbol']
                        values['type'] = securities[securityName]['type']
                        transaction = lineBefore.strip('\n').lstrip('N')
                        values['transaction'] = transaction
                        # log.info(securityName)
                        # log.info(typeName)
                        lineBefore = line
                        line = f.readline()
                        while line and line.strip('\n') != '^':
                            valueString = line.strip('\n')
                            code = valueString[0]
                            if code != 'C':
                                if code == 'M':
                                    description = valueString[1:]
                                    # log.info('description: %s' % description)
                                    values['description'] = description
                                elif code == 'I':
                                    price = float(valueString[1:].replace(',',''))
                                    # log.info('price: %s' % price)
                                    values['price'] = price
                                elif code == 'Q':
                                    shares = float(valueString[1:].replace(',',''))
                                    # log.info('price: %s' % shares)
                                    values['shares'] = shares
                                elif code == 'T':
                                    costBasis = float(valueString[1:].replace(',',''))
                                    # log.info('price: %s' % costBasis)
                                    values['costBasis'] = costBasis
                            lineBefore = line
                            line = f.readline()
                        rowValues = []
                        for param in params:
                            rowValues.append(values[param])
                        writeValues.append(tuple(rowValues))

                    lineBefore = line
                    line = f.readline()
            # for rowValues in writeValues:
            #     log.info(rowValues)
            db.insertOrIgnore(tableName, params, writeValues)
            
            # update status
            db.createTable('status_db', ["'%s' TIMESTAMP" % tableName])
            db.addColumn('status_db', tableName, 'TIMESTAMP')
            db.insertOrIgnore('status_db', ['rowid'], (1,))
            db.update( 'status_db', 'rowid', 1, [tableName], (fileDate,) )

    def readCSV(self):
        cvsFiles = glob.glob(self.dataPath+'*.csv')
        db = database.Database(self.dbName)
        # get status first
        fileDates , dataNames = db.getRows('status_db', whereColumns=['rowid'], areValues=[1])
        statusDates = {}
        index = 0
        for dataName in dataNames:
            statusDates[dataName] = fileDates[0][index]
            index += 1
        # now lets retrieve the data if needed
        connection = db.getConnection()
        filesRead = 0
        for cvsFile in cvsFiles:
            fileDate = int(os.path.getmtime(cvsFile))
            dataName = Path(cvsFile).stem
            
            # only read data from newer files
            if dataName in statusDates and fileDate <= statusDates[dataName]: continue
            
            log.info('Update saved: %s' % dataName)

            # read file into database
            data = pd.read_csv(cvsFile)
            data.to_sql(dataName, con=connection, index=False, if_exists='replace')
            filesRead += 1

            # update status
            db.createTable('status_db', ["'%s' TIMESTAMP" % dataName])
            db.addColumn('status_db', dataName, 'TIMESTAMP')
            db.insertOrIgnore('status_db', ['rowid'], (1,))
            db.update( 'status_db', 'rowid', 1, [dataName], (fileDate,) )

        if filesRead > 0:
            log.info('CSV files read and updated: %s' % filesRead)

