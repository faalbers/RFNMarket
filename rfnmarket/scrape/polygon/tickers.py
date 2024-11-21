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
        dataStatus = self.db.tableRead('status_db', keyValues=['ALLSYMBOLS'], columns=['tickers', 'types'])
        lastUpdateTypes = None
        lastUpdateTickers = None
        updateTime = int(datetime.now().timestamp() - (60*60*24*31*6))
        if not forceUpdate:
            if 'ALLSYMBOLS' in dataStatus:
                status = dataStatus['ALLSYMBOLS']
                if 'types' in status: lastUpdateTypes = status['types']
                if 'tickers' in status: lastUpdateTickers = status['tickers']

        # lets backup the database first
        if (lastUpdateTypes == None or lastUpdateTypes <= updateTime) or \
            (lastUpdateTickers == None or lastUpdateTickers <= updateTime):
            self.db.backup()
        
        if lastUpdateTypes == None or lastUpdateTypes <= updateTime:
            log.info('Polygon types updating')
            if lastUpdateTypes == None:
                log.info('First time update')
            else:
                log.info('Last time updated: %s' % datetime.fromtimestamp(lastUpdateTypes))
            
            nextRequestArgs = {
                'url': 'https://api.polygon.io/v3/reference/tickers/types',
            }
            response = self.requestCallLimited(nextRequestArgs)
            if response.headers.get('content-type').startswith('application/json'):
                responseData = response.json()
                responseData = responseData['results']
                writeData = {}
                for row in responseData:
                    code = row.pop('code')
                    writeData['code'] = row
                self.db.tableWrite('types', writeData, 'code', method='replace')

            self.db.tableWrite('status_db', {'ALLSYMBOLS': {'types': int(datetime.now().timestamp())}}, 'keySymbol', method='update')

        if lastUpdateTickers == None or lastUpdateTickers <= updateTime:
            log.info('Polygon tickers updating')
            if lastUpdateTickers == None:
                log.info('First time update')
            else:
                log.info('Last time updated: %s' % datetime.fromtimestamp(lastUpdateTickers))
            
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
                    writeData = {}
                    responseData = response.json()
                    if 'results' in responseData:
                        tickersList = responseData['results']
                    else:
                        # happened once , dunno what to do here yet
                        print('response data has no results key')
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
                        writeData[symbol] = ticker
                    self.db.tableWrite('tickers', writeData, 'keySymbol', method='update')
                    tickerCount += responseData['count']
                    self.db.commit()
                    log.info('Stocks found so far: %s' % tickerCount)
                    nextRequestArgs = None
                    if 'next_url' in responseData:
                        nextRequestArgs = {'url': responseData['next_url']}
                else:
                    nextRequestArgs = None
            
            self.db.tableWrite('status_db', {'ALLSYMBOLS': {'tickers': int(datetime.now().timestamp())}}, 'keySymbol', method='update')

