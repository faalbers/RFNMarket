from .base import Base
from ...utils import log, database
from pprint import pp
from datetime import datetime

# https://financialmodelingprep.com

class StockList(Base):
    dbName = 'fmp_stocklist'

    def __init__(self, symbols=[], tables=[]):
        super().__init__()

        # check if we need to update stocklist, maybe once every half a year
        updateTime = int(datetime.now().timestamp() - (60*60*24*31*6))
        # updateTime = int(datetime.now().timestamp())
        db = database.Database(self.dbName)
        lastUpdateTime = db.getMaxColumnValue('status_db', 'timestamp')
        
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
            db.createTable('stocklist', ["'keySymbol' TEXT PRIMARY KEY", "'timestamp' TIMESTAMP", "'name' TEXT", "'price' FLOAT",
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
            db.createTable('status_db', ["'timestamp' TIMESTAMP"])
            db.insertOrIgnore('status_db', ['rowid', 'timestamp'], (1, int(datetime.now().timestamp()),))
            db.update( 'status_db', 'rowid', 1, ['timestamp'], (int(datetime.now().timestamp()),) )

    def getStocks(self, type=None, exchangeCountry=None):
        db = database.Database(self.dbName)
        if type == None:
            slvalues, slparams = db.getRows('stocklist', columns=['keySymbol', 'exchangeShortName'])
        else:
            slvalues, slparams = db.getRows('stocklist', columns=['keySymbol', 'exchangeShortName'], whereColumns=['type'], areValues=[type])
        
        symbols = []
        if exchangeCountry != None:
            dbSaved = database.Database('saved')
            acvalues, acparams = dbSaved.getRows('ISO10383_MIC', columns=['ACRONYM', 'ISO COUNTRY CODE (ISO 3166)'])
            acronyms = {}
            for value in acvalues:
                acronyms[value[0]] = value[1]
            for value in slvalues:
                if value[1] in acronyms and acronyms[value[1]] == exchangeCountry:
                    symbols.append(value[0])
        else:
            symbols = [x[0] for x in slvalues]
       
        return symbols
    
