from .base import Base
from ... import database
from ...utils import log
from datetime import datetime

class Chart(Base):
    def __init__(self, symbols, updateMax=False):
        super().__init__()
        self.symbols = symbols
        self.databases = []
        # add databases that need to be updated with new data found
        self.databases.append(database.TimeSeries())

        # select period 1 based on lowset end time stamps
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

        # log.info('Running getCharts with range %s on intervals of %s with %s symbols' % (range, interval, len(symbols)))
        log.info('Running getCharts on %s symbols' % len(symbols))
        period2 = int(datetime.now().timestamp())
        interval = '1d'
        requestArgsList = []
        for symbol in symbols:
            if symbol in notFoundSymbols:
                now = datetime.now()
                period1 = int(datetime(year=now.year-10,month=now.month,day=now.day).timestamp())
            else:
                period1 = startTimestamp
            # print('%s: %s: %s' % (symbol, datetime.fromtimestamp(period1), period1))
            requestArgs = {
                'url': 'https://query2.finance.yahoo.com/v8/finance/chart/'+symbol.upper(),
                'params': {
                    'period1': period1,
                    'period2': period2,
                    'interval': interval,
                    'events': 'div,splits,capitalGains',
                },
                'timeout': 30,
            }                      
            requestArgsList.append(requestArgs)
        self.multiRequest(requestArgsList, blockSize=50)
        
    def pushAPIData(self, symbolIndex, symbolData):
        symbol = self.symbols[symbolIndex]
        symbolData = symbolData['chart']
        if symbolData['result'] == None: return
        symbolData = symbolData['result'][0]
        pushData = {}
        pushData['timestamp'] = int(datetime.now().timestamp())
        pushData['meta'] = symbolData.pop('meta')
        if 'timestamp' in symbolData:
            timestamps = symbolData.pop('timestamp')
            if 'indicators' in symbolData:
                indicators = symbolData.pop('indicators')
                pushData['indicators'] = {}
                for indicator, indicatorDataList in indicators.items():
                    pushData['indicators'][indicator] = {}
                    indexTimestamp = 0
                    for timestamp in timestamps:
                        pushData['indicators'][indicator][timestamp] = {}
                        for indicatorData in indicatorDataList:
                            for element, elementData in indicatorData.items():
                                pushData['indicators'][indicator][timestamp][element] = elementData[indexTimestamp]
                        indexTimestamp += 1
        if 'events' in symbolData:
            events = symbolData.pop('events')
            pushData['events'] = {}
            for event, eventData in events.items():
                pushData['events'][event] = {}
                for key, eventEntry in eventData.items():
                    pushData['events'][event][eventEntry['date']] = {}
                    for element, value in eventEntry.items():
                        if element == 'date': continue
                        pushData['events'][event][eventEntry['date']][element] = value
        
        for db in self.databases:
            db.updateYahooChart(symbol, pushData)
