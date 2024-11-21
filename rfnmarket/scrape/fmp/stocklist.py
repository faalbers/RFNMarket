from .base import Base
from ...utils import log, database
from pprint import pp
from datetime import datetime
import pandas as pd

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
        dataStatus = self.db.tableRead('status_db', keyValues=['ALLSYMBOLS'], columns=['stocklist'])
        lastUpdateTime = None
        if not forceUpdate and len(dataStatus) > 0:
            lastUpdateTime = dataStatus['ALLSYMBOLS']['stocklist']
            updateTime = int(datetime.now().timestamp() - (60*60*24*31*6))
            if lastUpdateTime > updateTime: return

        # lets backup the database first
        self.db.backup()

        log.info('FMP StockList updating')
        if lastUpdateTime != None:
            log.info('Last time updated: %s' % datetime.fromtimestamp(lastUpdateTime))

        requestArgs = {
            'url': 'https://financialmodelingprep.com/api/v3/stock/list',
            'params': {},
            'timeout': 30,
        }
        response = self.requestCallLimited(requestArgs)
      
        if response.headers.get('content-type').startswith('application/json'):
            responseData = response.json()
            writeData =  {}
            for entry in responseData:
                symbol = entry.pop('symbol')
                writeData[symbol] = entry
            self.db.tableWrite('stocklist', writeData, 'keySymbol', method='update')

            self.db.tableWrite('status_db', {'ALLSYMBOLS': {'stocklist': int(datetime.now().timestamp())}}, 'keySymbol', method='update')