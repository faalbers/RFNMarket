from .base import Base
from ...utils import log, database
from pprint import pp
from datetime import datetime

class StockList(Base):
    def __init__(self):
        super().__init__()
        self.dbName = 'fmp_stocklist'
        
        # check if we need to update stocklist, maybe once every half a year
        updateTime = int(datetime.now().timestamp() - (60*60*24*31*6))
        # updateTime = int(datetime.now().timestamp())
        db = database.Database(self.dbName)
        lastUpdateTime = db.getMaxValue('status_db', 'timestamp')
        
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
            db.addTable('stocklist', ["'keySymbol' TEXT PRIMARY KEY", "'timestamp' TIMESTAMP", "'name' TEXT", "'price' FLOAT",
                "'exchange' TEXT", "'exchangeShortName' TEXT", "'type' TEXT"])
            for entry in responseData:
                params = ['timestamp']
                values = [timestamp]
                symbol = None
                for param, value in entry.items():
                    if param == 'symbol':
                        symbol = value
                        db.insertOrIgnore('stocklist', ['keySymbol'], (symbol,))
                        continue
                    params.append(param)
                    values.append(value)
                db.update( 'stocklist', 'keySymbol', symbol, params, tuple(values) )
            db.addTable('status_db', ["'timestamp' TIMESTAMP"])
            db.insertOrIgnore('status_db', ['rowid', 'timestamp'], (1, int(datetime.now().timestamp()),))
            db.update( 'status_db', 'rowid', 1, ['timestamp'], (int(datetime.now().timestamp()),) )

    def getStockSymbols(self):
        db = database.Database(self.dbName)
        values, params = db.getRows('stocklist', ['keySymbol'])
        return [x[0] for x in values]