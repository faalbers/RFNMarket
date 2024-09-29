from ...utils import log, database
from .base import Base
from pprint import pp
from datetime import datetime
import json, copy
from . import const
import pandas as pd
import numpy as np

# https://yahooquery.dpguthrie.com/guide/ticker/modules/

class StatusData():
    def __init__(self, symbol, modules):
        self.symbol = symbol
        
        timestamp = int(datetime.now().timestamp())
        dictData = {}
        for module in modules:
            dictData[module] = timestamp
        dataSeries = pd.Series(dictData)
        
        # make a one row dataFrame with symbol as index under name keySymbol
        self.dataDf = pd.DataFrame([dataSeries], index=[self.symbol])
        self.dataDf.rename_axis('keySymbol', inplace=True)

        # make a sql data types dict for all columns and turn into JSON if needed
        self.sqlDataTypes = {}
        for columnName in self.dataDf.columns:
            self.sqlDataTypes[columnName] = 'TIMESTAMP'

    def appendToSqlTable(self, tableName, db):
        if db.tableExists(tableName):
            # get sql data types the table we are going to append to
            sqlDataTypes = db.getSqlDataTypes(tableName)
            sqlDataTypes.pop('keySymbol')
            
            # check if we need to add columns before appending our data
            creatColumns = set(self.dataDf.columns).difference(sqlDataTypes.keys())
            for creatColumn in creatColumns:
                db.addColumn(tableName, creatColumn, self.sqlDataTypes[creatColumn])

            # set write df
            writeDataDf = self.dataDf
            
            # copy existing values if they exist, overwrite the new ones on it
            values, params = db.getRows(tableName, whereColumns=['keySymbol'], areValues=[self.symbol])
            if len(values) > 0:
                readDataSeries = pd.Series()
                index = 0
                for param in params:
                    readDataSeries[param] = values[0][index]
                    index += 1
                readDataSeries = readDataSeries.fillna(value=np.nan).dropna()
                writeDataDf = pd.DataFrame([readDataSeries])
                writeDataDf.set_index('keySymbol', inplace=True)

                # add the new ones
                for symbol, value in self.dataDf.iterrows():
                    for param in value.index:
                        writeDataDf.loc[self.symbol, param] = value[param]

            # finally remove existing keySymbol row if it exists and append the new one
            db.deleteRow(tableName, ['keySymbol'], [self.symbol])
            writeDataDf.to_sql(tableName, db.getConnection(), if_exists='append')
        else:
            # create the new table with this data row
            sqlDataTypes = copy.deepcopy(self.sqlDataTypes)
            sqlDataTypes['keySymbol'] = 'STRING PRIMARY KEY'
            self.dataDf.to_sql(tableName, db.getConnection(), dtype=sqlDataTypes)
    
class SymbolData():
    __sqlDataTypes = {
        int:  'INTEGER',
        np.int64:  'INTEGER',
        np.float64:  'REAL',
        np.bool:  'BOOLEAN',
        float: 'FLOAT',
        str: 'TEXT',
        bool: 'BOOLEAN',
        list: 'JSON',
        dict: 'JSON',
    }

    def __init__(self, symbol, dictData):
        self.symbol = symbol
        
        # turn all Nones into nan and drop them
        dataSeries = pd.Series(dictData).fillna(value=np.nan).dropna()

        # make a one row dataFrame with symbol as index under name keySymbol
        self.dataDf = pd.DataFrame([dataSeries], index=[self.symbol])
        self.dataDf.rename_axis('keySymbol', inplace=True)

        # make a sql data types dict for all columns and turn into JSON if needed
        self.sqlDataTypes = {}
        for columnName in self.dataDf.columns:
            self.sqlDataTypes[columnName] = self.__sqlDataTypes[type(self.dataDf.loc[symbol, columnName])]
            if self.sqlDataTypes[columnName] == 'JSON':
                self.dataDf.loc[symbol, columnName] = json.dumps(self.dataDf.loc[symbol, columnName])

    def appendToSqlTable(self, tableName, db):
        if db.tableExists(tableName):
            # get sql data types the table we are going to append to
            sqlDataTypes = db.getSqlDataTypes(tableName)
            sqlDataTypes.pop('keySymbol')
            
            # check if we need to add columns before appending our data
            creatColumns = set(self.dataDf.columns).difference(sqlDataTypes.keys())
            for creatColumn in creatColumns:
                db.addColumn(tableName, creatColumn, self.sqlDataTypes[creatColumn])

            # finally remove existing keySymbol row if it exists and append the new one
            db.deleteRow(tableName, ['keySymbol'], [self.symbol])
            self.dataDf.to_sql(tableName, db.getConnection(), if_exists='append')
        else:
            # create the new table with this data row
            sqlDataTypes = copy.deepcopy(self.sqlDataTypes)
            sqlDataTypes['keySymbol'] = 'STRING PRIMARY KEY'
            self.dataDf.to_sql(tableName, db.getConnection(), dtype=sqlDataTypes)
        
        # values, params = db.getRows(tableName, columns=['pipi'], whereColumns=['keySymbol'], areValues=[self.symbol])
        # print(values)
        # print(params)
    
    def test(self):
        pass
        # print(self.sqlDataTypes)
        # print(self.dataDf)

class QuoteSummary(Base):
    dbName = 'yahoo_quotesummary'

    @staticmethod
    def getTableNames(tableName):
        if tableName == 'all':
            return list(const.QUOTESUMMARY_MODULES.keys())
        return [tableName]

    @staticmethod
    def getModuleUpdatePeriods(forceUpdate):
        mult = 1
        if forceUpdate:
            mult = 0
        # maybe use actual dates instead of time differences from now ?
        moduleUpdatePeriods = {
            'default': mult*60*60*24,
            'price': mult*60*60*24,
            'defaultKeyStatistics': mult*60*60*24,
            'summaryDetail': mult*60*60*24,
            'quoteType': mult*60*60*24*31*3,
            'assetProfile': mult*60*60*24*31*3,
            'fundProfile': mult*60*60*24*31*3,
        }
        return moduleUpdatePeriods
    
    def getSymbolModules(self, symbols, tables, forceUpdate):
        modules = set(tables)
        moduleUpdatePeriods = self.getModuleUpdatePeriods(forceUpdate)

        # symbolModules = {}
        # for symbol in symbols:
        #     symbolModules[symbol] = modules
        # return symbolModules

        # get status
        status = 'status_db'
        dfStatus = None
        now = datetime.now()
        if self.db.tableExists(status):
            dfStatus = pd.read_sql("SELECT * FROM '%s'" % status, self.db.getConnection(), index_col='keySymbol')
        
        # check all requested symbols
        symbolModules = {}
        for symbol in symbols:
            if isinstance(dfStatus, pd.DataFrame):
                updateModules = set(dfStatus.columns)
                symbolModulesToDo = modules.difference(updateModules)
                for module in modules.intersection(updateModules):
                    moduleTimestamp = dfStatus[module].get(symbol, None)
                    if moduleTimestamp == None:
                        symbolModulesToDo.add(module)
                        continue
                    else:
                        updateTimestamp = int(now.timestamp())-moduleUpdatePeriods['default']
                        if module in moduleUpdatePeriods:
                            # we found it, use that one
                            updateTimestamp = int(now.timestamp())-moduleUpdatePeriods[module]
                        # add module if update timestamp is lower or equal then the module update timestamp
                        if updateTimestamp >= moduleTimestamp:
                            symbolModulesToDo.add(module)
                if len(symbolModulesToDo) > 0: symbolModules[symbol] = symbolModulesToDo

            else:
                symbolModules[symbol] = set().union(modules)

        return symbolModules

    def __init__(self, symbols=[], tables=[], forceUpdate=False):
        super().__init__()
        self.db = database.Database(self.dbName)
        
        # update if needed 
        # modules not used , might as well remove it, it's always empty
        symbolModules = self.getSymbolModules(symbols, tables, forceUpdate=forceUpdate)

        # dont'run  update if no symbols
        if len(symbolModules) == 0: return
        
        log.info('QuoteSummary update')
        log.info('requested modules  : %s' % " ".join(tables))
        log.info('symbols processing : %s' % len(symbolModules))

        # update procs need these
        self.symbols = [] # accessed by index
        self.symbolModules = symbolModules

        requestArgsList = []
        modulesProcessed = set()
        for symbol, modules in symbolModules.items():
            modulesProcessed = modulesProcessed.union(modules)
            modulesString = ",".join(modules)
            requestArgs = {
                'url': 'https://query2.finance.yahoo.com/v10/finance/quoteSummary/'+symbol.upper(),
                'params': {
                    'modules': modulesString,
                    'corsDomain': 'finance.yahoo.com',
                    'formatted': 'false',
                },
                'timeout': 30,
            }
            requestArgsList.append(requestArgs)
            self.symbols.append(symbol)
        log.info('modules processing : %s' % " ".join(modulesProcessed))
        self.multiRequest(requestArgsList, blockSize=100)
    
    def pushAPIData(self, symbolIndex, response):
        pushStart = datetime.now()
        symbol = self.symbols[symbolIndex]
        if response.headers.get('content-type').startswith('application/json'):
            symbolData = response.json()
            if 'quoteSummary' in symbolData:
                # handle API response
                symbolData = symbolData['quoteSummary']
                if symbolData['error'] != None:
                    # handle error response
                    symbolData = symbolData['error']
                elif symbolData['result'] != None:
                    # handle data return response
                    symbolData = symbolData['result'][0]
                    modulesStart = datetime.now()
                    for module, moduleData in symbolData.items():
                        moduleData['keySymbol'] = symbol
                        pdData = pd.Series(moduleData)
                        self.db.writeData(pdData, module, index='keySymbol')
                        
                        # self.db.writeData(module, )
                        # sData = SymbolData(symbol, moduleData)
                        # sData.appendToSqlTable(module, self.db)
        # update status
        status = {'keySymbol': symbol}
        for module in self.symbolModules[symbol]:
            status[module] = int(datetime.now().timestamp())
        pdData = pd.Series(status)
        self.db.writeData(pdData, 'status_db', index='keySymbol', preClear=False)
        # statusData = StatusData(symbol, self.symbolModules[symbol])
        # statusData.appendToSqlTable('status_db', self.db)
    
    def dbCommit(self):
        # call from base to commit
        self.db.commit()
        
