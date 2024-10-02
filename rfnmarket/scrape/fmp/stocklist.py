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
        status = self.db.idxTableReadData('status_db')
        lastUpdateTime = None
        if len(status) != 0:
            lastUpdateTime = status.loc['all', 'stocklist']
        if not forceUpdate:
            updateTime = int(datetime.now().timestamp() - (60*60*24*31*6))
            if lastUpdateTime != None and lastUpdateTime > updateTime: return
        
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
            pdData = pd.DataFrame(responseData)
            pdData.set_index('symbol', inplace=True)
            pdData.rename_axis('keySymbol', inplace=True)
            # remove duplicate indices
            pdData = pdData.reset_index().drop_duplicates()
            pdData.set_index('keySymbol', inplace=True)
            
            dtype = {'keySymbol': 'TEXT PRIMARY KEY'}
            pdData.to_sql('stocklist', self.db.getConnection(), index=True, if_exists='replace', dtype=dtype)
            
            status = {'stocklist': int(datetime.now().timestamp())}
            self.db.idxTableWriteData(status, 'status_db', 'timestamps', 'all', 'update')
    
