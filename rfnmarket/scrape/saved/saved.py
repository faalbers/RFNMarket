import glob, os, fnmatch
import pandas as pd
import sqlite3
from pathlib import Path
from ...utils import log, database
from datetime import datetime
from pprint import pp

class Saved():
    dbName = 'saved'
    @staticmethod

    def getTableNames(tableName):
        if tableName == 'all':
            db = database.Database(Saved.dbName)
            return db.getTableNames()
        elif tableName.endswith('*'):
            db = database.Database(Saved.dbName)
            tableNames = db.getTableNames()
            return fnmatch.filter(tableNames, tableName)
        return [tableName]

    def __init__(self, symbols=[], tables=[], forceUpdate=False):
        self.db = database.Database(self.dbName)
        self.dataPath = 'database/'
        self.readCSV()
        self.readSPDRS()
        self.readNASDAQ()
        self.readQuicken()

    def readNASDAQ(self):
        dfSS = pd.DataFrame(self.db.tableRead('NASDAQ_STOCK_SCREENER', handleKeyValues=False))
        dfSS = dfSS[['Symbol','Name','Country','Sector','Industry']]
        dfSS.set_index('Symbol', verify_integrity=True, inplace=True)
        dfSS.index.name = 'keySymbol'
        dfSS['type'] = 'STOCK'
        
        dfETFS = pd.DataFrame(self.db.tableRead('NASDAQ_ETFS_SCREENER', handleKeyValues=False))
        # remove last row
        dfETFS.drop(dfETFS.tail(1).index,inplace=True)
        dfETFS = dfETFS[['SYMBOL','NAME']]
        dfETFS.set_index('SYMBOL', verify_integrity=True, inplace=True)
        dfETFS.index.name = 'keySymbol'
        dfETFS.rename(columns={'NAME': 'Name'}, inplace=True)
        dfETFS['type'] = 'ETF'

        df = pd.concat([dfSS, dfETFS], verify_integrity=True)
        df.sort_index(inplace=True)
        df = df[df.index.notnull()]
        self.db.tableWriteDF('NASDAQ', df)

    def readSPDRS(self):
        sectors = {
            'XLB': 'Materials',
            'XLC': 'Communication Services',
            'XLE': 'Energy',
            'XLF': 'Financials',
            'XLI': 'Industrials',
            'XLK': 'Information Technology',
            'XLP': 'Consumer Staples',
            'XLRE': 'Real Estate',
            'XLU': 'Utilities',
            'XLV': 'Healthcare',
            'XLY': 'Consumer Discretionary',
        }
        columnRename = {'Symbol': 'keySymbol', 'Company Name': 'Name', 'Index Weight': 'SP500weight'}
        tableNames = self.db.getTableNames()
        tableNames = fnmatch.filter(self.db.getTableNames(), 'SPDRS_*')
        if len(tableNames) == 0: return
        dfAll = pd.DataFrame()
        for tableName in tableNames:
            sector = sectors[tableName.split('_')[1]]
            sData = self.db.tableRead(tableName, handleKeyValues=False)
            cMatch = sData[0]
            rows = []
            for row in sData[1:]:
                newRow = {}
                for param, value in row.items():
                    if cMatch[param] in columnRename:
                        newRow[columnRename[cMatch[param]]] = value
                newRow['SP500weight'] = float(newRow['SP500weight'].rstrip('%'))/100.0
                newRow['SP500sector'] = sector
                rows.append(newRow)
            df = pd.DataFrame(rows)
            df.set_index('keySymbol', verify_integrity=True, inplace=True)
            dfAll = pd.concat([dfAll, df], verify_integrity=True)
        dfAll.sort_index(inplace=True)
        self.db.tableWriteDF('SPDRS', dfAll)

    def readQuicken(self):
        # get status first
        statusDb = self.db.tableRead('status_db', keyValues=['ALLSYMBOLS'])
        if 'ALLSYMBOLS' in statusDb:
            statusDb = statusDb['ALLSYMBOLS']

        # find QIF files and parse them
        qifFiles = glob.glob(self.dataPath+'*.QIF')
        params = ['timestamp', 'security', 'symbol', 'type', 'transaction', 'shares', 'price', 'costBasis', 'description']
        connection = self.db.getConnection()
        for qifFile in qifFiles:
            writeData = []
            fileDate = int(os.path.getmtime(qifFile))
            dataName = Path(qifFile).stem
            tableName = 'QUICKEN_'+dataName

            # only read data from newer files
            if tableName in statusDb and fileDate <= statusDb[tableName]: continue

            log.info('Creating Quicken data: %s' % dataName)
            
            timestamp = None
            securities = {}
            lineBefore = ''
            with open(qifFile, 'r') as f:
                line = f.readline()
                while line:
                    if line == '!Type:Security\n':
                        securityName = f.readline().strip('\n')[1:]
                        if securityName != 'Cash' and securityName != 'CD':
                            securities[securityName] = {}
                            securities[securityName]['symbol'] = f.readline().strip('\n')[1:]
                            securities[securityName]['type'] = f.readline().strip('\n')[1:]
                    lineBefore = line
                    line = f.readline()
            lineBefore = ''
            with open(qifFile, 'r') as f:
                line = f.readline()
                while line:
                    if line.startswith('D') and '/' in line and line[1].isnumeric():
                        date = line.strip('\n')[1:]
                        date = date.split('/')
                        month = int(date[0].strip())
                        date = date[1].split("'")
                        day = int(date[0].strip())
                        year = int(date[1].strip())+2000
                        timestamp = datetime(day=day, month=month, year=year).timestamp()
                    elif line.startswith('Y') and line.strip('\n')[1:] in securities:
                        values = {}
                        for param in params: values[param] = None
                        # log.info('')
                        # log.info(datetime.fromtimestamp(timestamp))
                        values['timestamp'] = int(timestamp)
                        securityName = line.strip('\n')[1:]
                        values['security'] = securityName
                        values['symbol'] = securities[securityName]['symbol']
                        values['type'] = securities[securityName]['type']
                        transaction = lineBefore.strip('\n')[1:]
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
                        writeData.append(values)

                    lineBefore = line
                    line = f.readline()
            dfWrite = pd.DataFrame(writeData)
            dfWrite.to_sql(tableName, con=connection, index=False, if_exists='replace')

            # update status
            self.db.tableWrite('status_db', {'ALLSYMBOLS': {tableName: fileDate}}, 'keySymbol', method='update')

    def readCSV(self):
        cvsFiles = glob.glob(self.dataPath+'*.csv')
        # get status first
        statusDb = self.db.tableRead('status_db', keyValues=['ALLSYMBOLS'])
        if 'ALLSYMBOLS' in statusDb:
            statusDb = statusDb['ALLSYMBOLS']
        
        # now lets retrieve the data if needed
        connection = self.db.getConnection()
        filesRead = 0
        for cvsFile in cvsFiles:
            fileDate = int(os.path.getmtime(cvsFile))
            dataName = Path(cvsFile).stem
            
            # only read data from newer files
            if dataName in statusDb and fileDate <= statusDb[dataName]: continue
            
            log.info('Update saved: %s' % dataName)

            # read file into database
            data = pd.read_csv(cvsFile)
            data.to_sql(dataName, con=connection, index=False, if_exists='replace')
            filesRead += 1

            # update status
            self.db.tableWrite('status_db', {'ALLSYMBOLS': {dataName: fileDate}}, 'keySymbol', method='update')

        if filesRead > 0:
            log.info('CSV files read and updated: %s' % filesRead)

