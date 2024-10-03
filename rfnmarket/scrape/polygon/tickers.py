from .base import Base
from ...utils import log, database
from pprint import pp
from datetime import datetime
import json
import pandas as pd

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
        statusDb = self.db.idxTableReadData('status_db')
        updateTime = int(datetime.now().timestamp() - (60*60*24*31*6))
      
        lastUpdateTime = updateTime
        if len(statusDb) > 0: lastUpdateTime = statusDb.loc['all', 'types']
        if forceUpdate or lastUpdateTime <= updateTime:
            log.info('Polygon types updating')
            log.info('Last time updated: %s' % datetime.fromtimestamp(lastUpdateTime))
            nextRequestArgs = {
                'url': 'https://api.polygon.io/v3/reference/tickers/types',
            }
            response = self.requestCallLimited(nextRequestArgs)
            if response.headers.get('content-type').startswith('application/json'):
                responseData = response.json()
                responseData = responseData['results']
                pdData = pd.DataFrame(responseData)
                pdData.set_index('code', inplace=True)
                
                dtype = {'code': 'TEXT PRIMARY KEY'}
                pdData.to_sql('types', self.db.getConnection(), index=True, if_exists='replace', dtype=dtype)
                
                status = {'types': int(datetime.now().timestamp())}
                self.db.idxTableWriteRow(status, 'status_db', 'timestamps', 'all', 'update')

        lastUpdateTime = updateTime
        if len(statusDb) > 0: lastUpdateTime = statusDb.loc['all', 'tickers']
        if forceUpdate or lastUpdateTime <= updateTime:
            log.info('Polygon tickers updating')
            log.info('Last time updated: %s' % datetime.fromtimestamp(lastUpdateTime))
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
                    for ticker in tickersList:
                        symbol = ticker.pop('ticker').upper()
                        timestamp = int(datetime.now().timestamp())
                        if 'last_updated_utc' in ticker:
                            lastUpdateUtc = ticker.pop('last_updated_utc')
                            timeformat = '%Y-%m-%dT%H:%M:%SZ'
                            if '.' in lastUpdateUtc: timeformat = '%Y-%m-%dT%H:%M:%S.%fZ'
                            timestamp = int(datetime.strptime(lastUpdateUtc, timeformat) .timestamp())
                        ticker['timestamp'] = timestamp
                        self.db.idxTableWriteRow(ticker, 'tickers', 'keySymbol', symbol, 'update')
                    tickerCount += responseData['count']
                    self.db.commit()
                    log.info('Stocks found so far: %s' % tickerCount)
                    nextRequestArgs = None
                    if 'next_url' in responseData:
                        nextRequestArgs = {'url': responseData['next_url']}
                else:
                    nextRequestArgs = None
            
            status = {'tickers': int(datetime.now().timestamp())}
            self.db.idxTableWriteRow(status, 'status_db', 'timestamps', 'all', 'update')

