import glob, os
import pandas as pd
import sqlite3
from pathlib import Path
from ...utils import log, database
from datetime import datetime
from pprint import pp

class Saved():
    dbName = 'saved'

    def updateTables(self, tables):
        if tables == None: return
        
        print('tables: %s' % tables)

    def __init__(self, symbols=[], tables=[]):

        self.updateTables(tables)
        
        return
    
        self.dataPath = 'database/'
        self.readCSV()
        self.readQuicken()

    def readQuicken(self):
        # get status first
        db = database.Database(self.dbName)
        fileDates , dataNames = db.getRows(self.dbName, whereColumns=['rowid'], areValues=[1])
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
            if tableName in statusDates and fileDate >= statusDates[tableName]: continue
            
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
            if dataName in statusDates and fileDate >= statusDates[dataName]: continue
            
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

    def getQuickenInvestments(self, withShares=True):
        investments = {}
        db = database.Database(self.dbName)

        # find QUICKEN table
        db.getTableNames()
        tableNames = list(filter(lambda item: item.startswith('QUICKEN'), db.getTableNames()))
        if len(tableNames) == 0: return investments

        tableName = tableNames[0]

        # find all symbols
        values, params = db.getRows(tableName, columns=['symbol'])
        symbols = list(set([x[0] for x in values]))
        symbols = list(filter(lambda item: not ' ' in item, symbols))
        symbols = set(symbols)

        # find ingoung params and outgoing params
        values, params = db.getRows(tableName, columns=['transaction'])
        transactionParams = list(set([x[0] for x in values]))
        sharesInParams = set(list(filter(lambda item: item.startswith('Reinv'), transactionParams))+['Buy', 'ShrsIn'])
        sharesOutParams = set(['Sell', 'ShrsOut'])

        if withShares:
            # only get symbols that still are invested
            symbolsWithShares = set()
            for symbol in symbols:
                # add incoming shares and substract outgoing shares
                shares = 0
                for tParam in sharesInParams:
                    values, params = db.getRows(tableName, columns=['shares'], whereColumns=['symbol', 'transaction'], areValues=[symbol, tParam])
                    paramValues = [x[0] for x in values]
                    paramValues = list(filter(lambda item: item is not None, paramValues))
                    if len(paramValues) == 0: continue
                    shares += sum(paramValues)
                for tParam in sharesOutParams:
                    values, params = db.getRows(tableName, columns=['shares'], whereColumns=['symbol', 'transaction'], areValues=[symbol, tParam])
                    paramValues = [x[0] for x in values]
                    paramValues = list(filter(lambda item: item is not None, paramValues))
                    if len(paramValues) == 0: continue
                    shares -= sum(paramValues)
                
                # if we still have shares, we keep them
                if shares > 0.001:
                    symbolsWithShares.add(symbol)

            symbols = symbolsWithShares

        # get investment data of symbols
        for symbol in symbols:
            investments[symbol] = {}
            values, params = db.getRows(tableName, ['timestamp', 'transaction', 'shares', 'price', 'costBasis'],
                whereColumns=['symbol'], areValues=[symbol])
            
            for value in values:
                # skip the ones with no shares transaction
                if value[2] == None: continue
                data = investments[symbol][value[0]] = {}
                index = 1
                for param in params[1:]:
                    data[param] = value[index]
                    index += 1
        
        return investments
