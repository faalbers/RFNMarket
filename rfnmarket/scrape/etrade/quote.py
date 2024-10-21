from .base import Base
from ...utils import log, database
from pprint import pp
from datetime import datetime

class Quote(Base):
    dbName = 'etrade_quote'

    @staticmethod
    def getTableNames(tableName):
        if tableName == 'all':
            return ['equity', 'mutualFund']
        return [tableName]

    def update(self, keyValues, tables, forceUpdate):
        # do all if forced update
        if forceUpdate: return keyValues

        # check what symbols need to be updated
        now = int(datetime.now().timestamp())
        symbols = []
        updateTime = now - 60*60*24
        dataStatus = self.db.tableRead('status_db', keyValues=keyValues, columns=['quote'])
        for symbol in keyValues:
            if symbol in dataStatus:
                if dataStatus[symbol]['quote'] <= updateTime:
                    symbols.append(symbol)
            else:
                symbols.append(symbol)

        return symbols


    def __init__(self, keyValues=[], tables=[], forceUpdate=False):
        super().__init__()
        self.db = database.Database(self.dbName)   

        # find how many keyvalues (symbols), still to do
        keyValues = self.update(keyValues, tables, forceUpdate=forceUpdate)

        log.info('Etrade Quote update')
        log.info('symbols processing : %s' % len(keyValues))
        
        # requestArgsList = []
        blockSize=50
        keyValueIndices = range(len(keyValues))
        keyValuesIndicesCount = len(keyValueIndices)
        rangeCount = keyValuesIndicesCount / blockSize
        todoCount = len(keyValues)
        for x in range(int(rangeCount) + ((rangeCount) > int(rangeCount))):
            log.debug('Still %s symbols to do ...' % todoCount)
            blockKeyValueIndices = keyValueIndices[x*blockSize:(x+1)*blockSize]
            symbolsList = [keyValues[x] for x in blockKeyValueIndices]
            allData = self.pushData(symbolsList, 'ALL', ['All', 'MutualFund'])
            # with open('etrade_ALL.txt', 'w', encoding='utf-8') as f:
            #     pp(allData, f)
            if 'MutualFund' in allData:
                mfData = self.pushData(list(allData['MutualFund'].keys()), 'MF_DETAIL', ['MutualFund'])
                allData['MutualFund'] = mfData['MutualFund']
            # with open('etrade_ALL.txt', 'w', encoding='utf-8') as f:
            #     pp(allData, f)
            if 'All' in allData:
                self.db.tableWrite('equity', allData['All'], 'keySymbol', method='update')
            if 'MutualFund' in allData:
                self.db.tableWrite('mutualFund', allData['MutualFund'], 'keySymbol', method='update')
            now = int(datetime.now().timestamp())
            update = {}
            for symbol in symbolsList:
                update[symbol] = {'quote': now}
            self.db.tableWrite('status_db', update, 'keySymbol', method='update')
            self.db.commit()
            todoCount -= blockSize

    def pushData(self, symbols, detailFlag, detailTypes):
        symbolsString = ','.join(symbols)
        # log.info(symbolsString)
        requestArgs = {
            'url': 'https://api.etrade.com/v1/market/quote/%s.json' % symbolsString,
            'params': {
                'detailFlag': detailFlag,
                'overrideSymbolCount': 'true',
            },
        }
        response = self.requestCallLimited(requestArgs)
        pushData = {}
        if response.headers.get('content-type').startswith('application/json'):
            responseData = response.json()
            if 'QuoteResponse' in responseData:
                responseData = responseData['QuoteResponse']
                if 'QuoteData' in responseData:
                    for quoteData in responseData['QuoteData']:
                        product = quoteData.pop('Product')
                        symbol = product.pop('symbol')
                        detailData = None
                        detailType = None
                        for dType in detailTypes:
                            if dType in quoteData:
                                detailData = quoteData.pop(dType)
                                detailType = dType
                                break
                        if detailType == None:
                            log.info('detailTypes not found: %s found: %s' % (detailTypes, list(quoteData.keys())))
                            continue
                        if not detailType in pushData:
                            pushData[detailType] = {}
                        pushData[detailType][symbol] = {**product,**quoteData,**detailData}
                else:
                    log.info('response: no QuoteData:')
                    log.info(responseData)
            else:
                log.info('response: no QuoteResponse:')
                log.info(responseData)
        else:
            log.info('response content-type: %s' % response.headers.get('content-type'))
        
        return pushData
