from .base import Base
from ... import database
from ...utils import log
from datetime import datetime

class Chart(Base):
    def __init__(self, symbols, updateMax=False):
        super().__init__()
        self.symbols = symbols
        self.databases = []
        self.databases.append(database.TimeSeries())

        # select period 1 based on lowset end time stamps
        startTimestamp = int(datetime.now().timestamp())
        if not updateMax:
            for db in self.__databases:
                timestamp = db.getLastLowestTimestamp(symbols)
                if timestamp < startTimestamp:
                    startTimestamp = timestamp

        # select period 1 based on lowset end time stamps
        period1 = startTimestamp
        period2 = int(datetime.now().timestamp())
        interval = '1d'
        
        # log.info('Running getCharts with range %s on intervals of %s with %s symbols' % (range, interval, len(symbols)))
        log.info('Running getCharts on %s symbols' % len(symbols))
        requestArgsList = []
        if updateMax:
            log.info('range: 10y')
            requestArgsParams = {
                'range': '10y',
                'interval': interval,
                'events': 'div,splits,capitalGains',
            }
        else:
            log.info('period1: %s' % datetime.fromtimestamp(startTimestamp))
            requestArgsParams = {
                'period1': period1,
                'period2': period2,
                'interval': interval,
                'events': 'div,splits,capitalGains',
            }
        for symbol in symbols:
                    requestArgs = {
                        'url': 'https://query2.finance.yahoo.com/v8/finance/chart/'+symbol.upper(),
                        'params': requestArgsParams,
                        'timeout': 30,
                    }                      
                    requestArgsList.append(requestArgs)
        # responseDataList = self.multiRequest(requestArgsList, blockSize=50)
        self.multiRequest(requestArgsList, blockSize=50)
        
        # # create user data
        # data = {}
        # if responseDataList == None: return
        # indexSymbol = 0
        # for symbol in symbols:
        #     symbolData = responseDataList[indexSymbol]
        #     indexSymbol += 1
        #     if symbolData == None:
        #         continue
        #     symbolData = symbolData['chart']
        #     if symbolData['result'] == None: continue
        #     symbolData = symbolData['result'][0]
        #     data[symbol] = {}
        #     data[symbol]['timestamp'] = int(datetime.now().timestamp())
        #     data[symbol]['meta'] = symbolData.pop('meta')
        #     if 'timestamp' in symbolData:
        #         timestamps = symbolData.pop('timestamp')
        #         if 'indicators' in symbolData:
        #             indicators = symbolData.pop('indicators')
        #             data[symbol]['indicators'] = {}
        #             for indicator, indicatorDataList in indicators.items():
        #                 data[symbol]['indicators'][indicator] = {}
        #                 indexTimestamp = 0
        #                 for timestamp in timestamps:
        #                     data[symbol]['indicators'][indicator][timestamp] = {}
        #                     for indicatorData in indicatorDataList:
        #                         for element, elementData in indicatorData.items():
        #                             data[symbol]['indicators'][indicator][timestamp][element] = elementData[indexTimestamp]
        #                     indexTimestamp += 1
        #     if 'events' in symbolData:
        #         events = symbolData.pop('events')
        #         data[symbol]['events'] = {}
        #         for event, eventData in events.items():
        #             data[symbol]['events'][event] = {}
        #             for key, eventEntry in eventData.items():
        #                 data[symbol]['events'][event][eventEntry['date']] = {}
        #                 for element, value in eventEntry.items():
        #                     if element == 'date': continue
        #                     data[symbol]['events'][event][eventEntry['date']][element] = value

        # for db in self.databases:
        #     db.updateYahooChart(data)
        
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
            db.updateYahooAPI(symbol, pushData)
