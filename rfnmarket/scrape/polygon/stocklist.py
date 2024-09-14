from .base import Base
from ...utils import log, database
from pprint import pp
from datetime import datetime
import json

class StockList(Base):
    def __init__(self):
        super().__init__()
        self.dbName = 'polygon_stocklist'
        
        # check if we need to update stocklist, maybe once every half a year
        updateTime = int(datetime.now().timestamp() - (60*60*24*31*6))
        db = database.Database(self.dbName)
        lastUpdateTime = db.getMaxValue('status_db', 'timestamp')

        if lastUpdateTime != None and lastUpdateTime > updateTime: return
   
        log.info('Polygon StockList updating')
        log.info('Last time updated: %s' % datetime.fromtimestamp(lastUpdateTime))

        db = database.Database(self.dbName)
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
                # db.addTable('stocklist', ["'keySymbol' TEXT PRIMARY KEY", "'timestamp' TIMESTAMP", "'name' TEXT", "'market' TEXT",
                #     "'locale' TEXT", "'primary_exchange' TEXT", "'type' TEXT", "'active' BOOLEAN",
                #     "'currency_name' TEXT", "'currency_symbol' TEXT", "'base_currency_symbol' TEXT", "'base_currency_name' TEXT",
                #     "'cik' TEXT", "'composite_figi' TEXT", "'share_class_figi' TEXT"])
                db.addTable('stocklist', ["'keySymbol' TEXT PRIMARY KEY", "'timestamp' TIMESTAMP"])
                for ticker in tickersList:
                    symbol = ticker.pop('ticker').upper()
                    timestamp = int(datetime.now().timestamp())
                    if 'last_updated_utc' in ticker:
                        lastUpdateUtc = ticker.pop('last_updated_utc')
                        timeformat = '%Y-%m-%dT%H:%M:%SZ'
                        if '.' in lastUpdateUtc: timeformat = '%Y-%m-%dT%H:%M:%S.%fZ'
                        timestamp = int(datetime.strptime(lastUpdateUtc, timeformat) .timestamp())
                    db.insertOrIgnore('stocklist', ['keySymbol'], (symbol,))
                    params = ['timestamp']
                    values = [timestamp]
                    for param, value in ticker.items():
                        if isinstance(value, int):
                            db.addColumnIfNotExists('stocklist', param, 'INTEGER')
                            params.append(param)
                            values.append(value)
                        elif isinstance(value, float):
                            db.addColumnIfNotExists('stocklist', param, 'FLOAT')
                            params.append(param)
                            values.append(value)
                        elif isinstance(value, str):
                            db.addColumnIfNotExists('stocklist', param, 'TEXT')
                            params.append(param)
                            values.append(value)
                        elif isinstance(value, bool):
                            db.addColumnIfNotExists('stocklist', param, 'BOOLEAN')
                            params.append(param)
                            values.append(value)
                        elif isinstance(value, list):
                            db.addColumnIfNotExists('stocklist', param, 'JSON')
                            params.append(param)
                            values.append(json.dumps(value))
                        elif isinstance(value, dict):
                            db.addColumnIfNotExists('stocklist', param, 'JSON')
                            params.append(param)
                            values.append(json.dumps(value))
                        elif isinstance(value, type(None)):
                            pass
                    db.update( 'stocklist', 'keySymbol', symbol, params, tuple(values) )
                tickerCount += responseData['count']
                db.commit()
                log.info('Stocks found so far: %s' % tickerCount)
                nextRequestArgs = None
                if 'next_url' in responseData:
                    nextRequestArgs = {'url': responseData['next_url']}
            else:
                nextRequestArgs = None
        db.addTable('status_db', ["'timestamp' TIMESTAMP"])
        db.insertOrIgnore('status_db', ['rowid', 'timestamp'], (1, int(datetime.now().timestamp()),))
        db.update( 'status_db', 'rowid', 1, ['timestamp'], (int(datetime.now().timestamp()),) )

