from .base import Base
from ... import database
from ...utils import log
from . import const
from datetime import datetime
from pprint import pp

class QuoteProfile(Base):
    def __init__(self, symbols, updateMax=False):
        super().__init__()
        self.symbols = symbols
        self.databases = []
        # add databases that need to be updated with new data found
        self.databases.append(database.Profiles())

        # from symbols found, get longest updated one
        # get symbols that were not found
        startTimestamp = int(datetime.now().timestamp())
        notFoundSymbols = set()
        for db in self.databases:
            timestamp, foundSymbols = db.getLowestTimestamp(self.symbols)
            notFoundSymbols = notFoundSymbols.union(set(self.symbols).difference(set(foundSymbols)))
            if timestamp < startTimestamp:
                startTimestamp = timestamp
        timeExpired = datetime.now() - datetime.fromtimestamp(startTimestamp)
        notFoundSymbols = list(notFoundSymbols)
        # print(timeExpired)
        # print(notFoundSymbols)

        if timeExpired.days < (3*30):
            if len(notFoundSymbols) > 0:
                self.symbols = notFoundSymbols
            else:
                log.info('Quarterly QuoteProfile skipped')
                return

        modules = ['quoteType', 'summaryProfile',]
        # modules = ['defaultKeyStatistics']

        log.info('Running geQuoteProfile on %s symbols quarterly' % len(self.symbols))
        modules = list(set(modules).intersection(const.YAHOO_QUOTE_SUMMARY_MODULES))
        if len(modules) == 0:
            log.error('Yahoo.getQuoteSummary: No valid modules selected. Use Yahoo.getQuoteProfileModules for valid ones.')
            return
        modules = ','.join(modules)
        requestArgsList = []
        for symbol in self.symbols:
                    requestArgs = {
                        'url': 'https://query2.finance.yahoo.com/v10/finance/quoteSummary/'+symbol.upper(),
                        'params': {
                            'modules': modules,
                            'corsDomain': 'finance.yahoo.com',
                            'formatted': 'false',
                        },
                        'timeout': 30,
                    }
                    requestArgsList.append(requestArgs)
        self.multiRequest(requestArgsList, blockSize=100)

    def pushAPIData(self, symbolIndex, symbolData):
        symbol = self.symbols[symbolIndex]
        symbolData = symbolData['quoteSummary']
        if symbolData['result'] == None: return
        symbolData = symbolData['result'][0]
        pushData = symbolData
        pushData['timestamp'] = int(datetime.now().timestamp())
        for db in self.databases:
            db.updateYahooQuoteProfile(symbol, pushData)
