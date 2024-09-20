from .base import Base
from ...utils import log, database
from pprint import pp
from datetime import datetime
import json

# https://polygon.io/
class Tickers(Base):
    dbName = 'polygon_tickers'

    def updateTables(self, tables):
        if tables == None: return
        
        print('tables: %s' % tables)

    def __init__(self, symbols=[], tables=[]):
        super().__init__()

        self.updateTables(tables)
        
        # check if we need to update stocklist, maybe once every half a year
        updateTime = int(datetime.now().timestamp() - (60*60*24*31*6))
        db = database.Database(self.dbName)
        lastUpdateTime = db.getMaxColumnValue('status_db', 'timestamp')

        if lastUpdateTime != None and lastUpdateTime > updateTime: return
   
        log.info('Polygon Tickers updating')
        log.info('Last time updated: %s' % datetime.fromtimestamp(lastUpdateTime))

        return
    
        db = database.Database(self.dbName)

        nextRequestArgs = {
            'url': 'https://api.polygon.io/v3/reference/tickers/types',
        }
        response = self.requestCallLimited(nextRequestArgs)
        if response.headers.get('content-type').startswith('application/json'):
            responseData = response.json()
            types = responseData['results']
            db.createTable('types', ["'code' TEXT PRIMARY KEY", "'asset_class' TEXT", "'description' TEXT", "'locale' TEXT"])
            values = []
            for type in types:
                value = (type['code'], type['asset_class'], type['description'], type['locale'] )
                values.append(value)
            db.insertOrIgnore('types', ['code', 'asset_class', 'description', 'locale'], values)

        nextRequestArgs = {
            'url': 'https://api.polygon.io/v3/reference/tickers',
            'params': {
                'limit': 1000,
            },
        }
        tickerCount = 0
        while nextRequestArgs != None:
            response = self.requestCallLimited(nextRequestArgs)
            if response.headers.get('content-type').startswith('application/json'):
                responseData = response.json()
                tickersList = responseData['results']
                db.createTable('tickers', ["'keySymbol' TEXT PRIMARY KEY", "'timestamp' TIMESTAMP"])
                for ticker in tickersList:
                    symbol = ticker.pop('ticker').upper()
                    timestamp = int(datetime.now().timestamp())
                    if 'last_updated_utc' in ticker:
                        lastUpdateUtc = ticker.pop('last_updated_utc')
                        timeformat = '%Y-%m-%dT%H:%M:%SZ'
                        if '.' in lastUpdateUtc: timeformat = '%Y-%m-%dT%H:%M:%S.%fZ'
                        timestamp = int(datetime.strptime(lastUpdateUtc, timeformat) .timestamp())
                    db.insertOrIgnore('tickers', ['keySymbol'], (symbol,))
                    params = ['timestamp']
                    values = [timestamp]
                    for param, value in ticker.items():
                        if isinstance(value, int):
                            db.addColumn('tickers', param, 'INTEGER')
                            params.append(param)
                            values.append(value)
                        elif isinstance(value, float):
                            db.addColumn('tickers', param, 'FLOAT')
                            params.append(param)
                            values.append(value)
                        elif isinstance(value, str):
                            db.addColumn('tickers', param, 'TEXT')
                            params.append(param)
                            values.append(value)
                        elif isinstance(value, bool):
                            db.addColumn('tickers', param, 'BOOLEAN')
                            params.append(param)
                            values.append(value)
                        elif isinstance(value, list):
                            db.addColumn('tickers', param, 'JSON')
                            params.append(param)
                            values.append(json.dumps(value))
                        elif isinstance(value, dict):
                            db.addColumn('tickers', param, 'JSON')
                            params.append(param)
                            values.append(json.dumps(value))
                        elif isinstance(value, type(None)):
                            pass
                    db.update( 'tickers', 'keySymbol', symbol, params, tuple(values) )
                tickerCount += responseData['count']
                db.commit()
                log.info('Stocks found so far: %s' % tickerCount)
                nextRequestArgs = None
                if 'next_url' in responseData:
                    nextRequestArgs = {'url': responseData['next_url']}
            else:
                nextRequestArgs = None
        
        db.createTable('status_db', ["'timestamp' TIMESTAMP"])
        db.insertOrIgnore('status_db', ['rowid', 'timestamp'], (1, int(datetime.now().timestamp()),))
        db.update( 'status_db', 'rowid', 1, ['timestamp'], (int(datetime.now().timestamp()),) )

    def decipherSymbolName(self, symbol):
        if ':' in symbol:
            items = symbol.split(':')
            if items[0] == 'I':
                return ('^'+items[1])
            if items[0] == 'C':
                return (items[1]+'=X')
            if items[0] == 'X':
                return items[1]
            return None
        return symbol

    def getTickers(self, market=None, exchangeCountry=None):
        db = database.Database(self.dbName)
        if market == None:
            tvalues, tparams = db.getRows('tickers', columns=['keySymbol', 'primary_exchange'])
        else:
            tvalues, tparams = db.getRows('tickers', columns=['keySymbol', 'primary_exchange'], whereColumns=['market'], areValues=[market])
        
        keySymbols = []
        if exchangeCountry != None:
            dbSaved = database.Database('saved')
            mvalues, mparams = dbSaved.getRows('ISO10383_MIC', columns=['MIC', 'ISO COUNTRY CODE (ISO 3166)'])
            mics = {}
            for value in mvalues:
                mics[value[0]] = value[1]
            for value in tvalues:
                if value[1] in mics and mics[value[1]] == exchangeCountry:
                    keySymbols.append(value[0])
        else:
            keySymbols = [x[0] for x in tvalues]

        symbols = []
        for symbol in keySymbols:
            symbol = self.decipherSymbolName(symbol)
            if symbol != None:
                symbols.append(symbol)
        
        return symbols
    
    def getUSExchangeStockSymbols(self):
        dbStocklist = database.Database(self.dbName)
        dbSaved = database.Database('saved')
        values, params = dbSaved.getRows('ISO10383_MIC', columns=['MIC', 'ISO COUNTRY CODE (ISO 3166)'])
        mics = {}
        for value in values:
            mics[value[0]] = value[1]
        values, params = dbStocklist.getRows('tickers', columns=['keySymbol','primary_exchange'])
        symbols = []
        for value in values:
            if value[1] in mics and mics[value[1]] == 'US':
                if ':' in value[0]:
                    print(value[0])
                symbol = self.decipherSymbolName(value[0])
                if symbol != None:
                    symbols.append(value[0])
        return symbols
