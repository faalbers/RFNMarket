from .base import Base
from ...utils import log, database
from pprint import pp
from datetime import datetime
import json

# https://polygon.io/
class Tickers(Base):
    dbName = 'polygon_tickers'
    
    @staticmethod
    def getTableNames(tableName):
        if tableName == 'all':
            return ['tickers', 'types']
        return [tableName]

    def __init__(self, symbols=[], tables=[], forceUpdate=False):
        super().__init__()
        self.db = database.Database(self.dbName)

        # check if we need to update stocklist, maybe once every half a year
        mult = 1
        if forceUpdate: mult = 0
        updateTime = int(datetime.now().timestamp() - (60*60*24*31*6*mult))
        lastUpdateTime = self.db.getMaxColumnValue('status_db', 'timestamp')

        if lastUpdateTime != None and lastUpdateTime > updateTime: return
   
        log.info('Polygon Tickers updating')
        log.info('Last time updated: %s' % datetime.fromtimestamp(lastUpdateTime))

        nextRequestArgs = {
            'url': 'https://api.polygon.io/v3/reference/tickers/types',
        }
        response = self.requestCallLimited(nextRequestArgs)
        if response.headers.get('content-type').startswith('application/json'):
            responseData = response.json()
            types = responseData['results']
            self.db.createTable('types', ["'code' TEXT PRIMARY KEY", "'asset_class' TEXT", "'description' TEXT", "'locale' TEXT"])
            values = []
            for type in types:
                value = (type['code'], type['asset_class'], type['description'], type['locale'] )
                values.append(value)
            self.db.insertOrIgnore('types', ['code', 'asset_class', 'description', 'locale'], values)

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
                if 'results' in responseData:
                    tickersList = responseData['results']
                else:
                    # happened once , dunno what to do here yet
                    print('response data has nor results key')
                    pp(responseData)
                    exit(0)
                self.db.createTable('tickers', ["'keySymbol' TEXT PRIMARY KEY", "'timestamp' TIMESTAMP"])
                for ticker in tickersList:
                    symbol = ticker.pop('ticker').upper()
                    timestamp = int(datetime.now().timestamp())
                    if 'last_updated_utc' in ticker:
                        lastUpdateUtc = ticker.pop('last_updated_utc')
                        timeformat = '%Y-%m-%dT%H:%M:%SZ'
                        if '.' in lastUpdateUtc: timeformat = '%Y-%m-%dT%H:%M:%S.%fZ'
                        timestamp = int(datetime.strptime(lastUpdateUtc, timeformat) .timestamp())
                    self.db.insertOrIgnore('tickers', ['keySymbol'], (symbol,))
                    params = ['timestamp']
                    values = [timestamp]
                    for param, value in ticker.items():
                        if isinstance(value, int):
                            self.db.addColumn('tickers', param, 'INTEGER')
                            params.append(param)
                            values.append(value)
                        elif isinstance(value, float):
                            self.db.addColumn('tickers', param, 'FLOAT')
                            params.append(param)
                            values.append(value)
                        elif isinstance(value, str):
                            self.db.addColumn('tickers', param, 'TEXT')
                            params.append(param)
                            values.append(value)
                        elif isinstance(value, bool):
                            self.db.addColumn('tickers', param, 'BOOLEAN')
                            params.append(param)
                            values.append(value)
                        elif isinstance(value, list):
                            self.db.addColumn('tickers', param, 'JSON')
                            params.append(param)
                            values.append(json.dumps(value))
                        elif isinstance(value, dict):
                            self.db.addColumn('tickers', param, 'JSON')
                            params.append(param)
                            values.append(json.dumps(value))
                        elif isinstance(value, type(None)):
                            pass
                    self.db.update( 'tickers', 'keySymbol', symbol, params, tuple(values) )
                tickerCount += responseData['count']
                self.db.commit()
                log.info('Stocks found so far: %s' % tickerCount)
                nextRequestArgs = None
                if 'next_url' in responseData:
                    nextRequestArgs = {'url': responseData['next_url']}
            else:
                nextRequestArgs = None
        
        self.db.createTable('status_db', ["'timestamp' TIMESTAMP"])
        self.db.insertOrIgnore('status_db', ['rowid', 'timestamp'], (1, int(datetime.now().timestamp()),))
        self.db.update( 'status_db', 'rowid', 1, ['timestamp'], (int(datetime.now().timestamp()),) )

