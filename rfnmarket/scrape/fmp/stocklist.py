from .base import Base
from ...utils import log, database
from pprint import pp
from datetime import datetime

# https://financialmodelingprep.com

class StockList(Base):
    dbName = 'fmp_stocklist'
    
    @staticmethod
    def getTableNames(tableName):
        if tableName == 'all':
            return ['stocklist']
        return [tableName]

    def __init__(self, symbols=[], tables=[], forceUpdate=False):
        super().__init__()
        self.db = database.Database(self.dbName)

        # check if we need to update stocklist, maybe once every half a year
        mult = 1
        if forceUpdate: mult = 0
        updateTime = int(datetime.now().timestamp() - (60*60*24*31*6*mult))
        # updateTime = int(datetime.now().timestamp())
        lastUpdateTime = self.db.getMaxColumnValue('status_db', 'timestamp')
        
        if lastUpdateTime != None and lastUpdateTime > updateTime: return
        
        log.info('FMP StockList updating')
        log.info('Last time updated: %s' % datetime.fromtimestamp(lastUpdateTime))
   
        requestArgs = {
            'url': 'https://financialmodelingprep.com/api/v3/stock/list',
            'params': {},
            'timeout': 30,
        }
        response = self.requestCallLimited(requestArgs)
      
        if response.headers.get('content-type').startswith('application/json'):
            responseData = response.json()
            timestamp = int(datetime.now().timestamp())
            self.db.createTable('stocklist', ["'keySymbol' TEXT PRIMARY KEY", "'timestamp' TIMESTAMP", "'name' TEXT", "'price' FLOAT",
                "'exchange' TEXT", "'exchangeShortName' TEXT", "'type' TEXT"])
            for entry in responseData:
                params = ['timestamp']
                values = [timestamp]
                symbol = None
                for param, value in entry.items():
                    if param == 'symbol':
                        symbol = value
                        self.db.insertOrIgnore('stocklist', ['keySymbol'], (symbol,))
                        continue
                    params.append(param)
                    values.append(value)
                self.db.update( 'stocklist', 'keySymbol', symbol, params, tuple(values) )
            self.db.createTable('status_db', ["'timestamp' TIMESTAMP"])
            self.db.insertOrIgnore('status_db', ['rowid', 'timestamp'], (1, int(datetime.now().timestamp()),))
            self.db.update( 'status_db', 'rowid', 1, ['timestamp'], (int(datetime.now().timestamp()),) )
    
