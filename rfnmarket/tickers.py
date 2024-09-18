from . import scrape
from .utils import log
from pprint import pp

class Tickers():
    __dataTypes = {
        'profile': [
            scrape.yahoo.QuoteSummary,
        ],
        'statistics': [
            scrape.yahoo.QuoteSummary,
        ],
        'price': [
            scrape.yahoo.QuoteSummary,
            scrape.yahoo.Chart,
        ],
    }

    def __init__(self, logLevel=log.WARNING):
        log.initLogger(logLevel=logLevel)
        self.fmpStocklist =  scrape.fmp.StockList()
        self.polygonTickers = scrape.polygon.Tickers()
        self.savedSaved = scrape.saved.Saved()

    # @property
    # def test(self):
    #     self.scrapers.add((scrape.yahoo.TimeSeries, 'test'))
    
    # def updateStocks(self, symbols=None):
    #     scraperObjects = {}
    #     # gather types and create scraper objects
    #     scrapers = {}
    #     for scraper in self.scrapers:
    #         if not scraper[0] in scrapers:
    #             scrapers[scraper[0]] = []
    #         scrapers[scraper[0]].append(scraper[1])
    #     for scraperClass, types in scrapers.items():
    #         if not scraperClass in scraperObjects:
    #             scraperObjects[scraperClass] = scraperClass(symbols, types)
    def updateData(self, symbols, types=None):
        pass
    
    def getData(self, symbols, types, update=False):
        # find scrapers and types to update
        scrapers = {}
        for type in set(types).intersection(self.__dataTypes.keys()):
            for scraperClass in self.__dataTypes[type]:
                if not scraperClass in scrapers:
                    scrapers[scraperClass] = []
                scrapers[scraperClass].append(type)
        # update if requested and retrieve data
        data = {}
        for scraperClass, types in scrapers.items():
            if update:
                scraper = scraperClass(symbols, types)
            else:
                scraper = scraperClass()
            for symbol, scrapeData in scraper.getData(symbols, types).items():
                if not symbol in data:
                    data[symbol] = {}
                for param, paramData in scrapeData.items():
                    data[symbol][param] = paramData
        return data
        
    def getTickerSymbols(self, exchangeCountry=None, includeIndices= False):
        fmpSymbols = self.fmpStocklist.getStocks(exchangeCountry=exchangeCountry)
        
        polygonSymbols = self.polygonTickers.getTickers(exchangeCountry=exchangeCountry)
        if includeIndices:
            polygonSymbols += self.polygonTickers.getTickers(market='indices')
       
        return list(set(fmpSymbols + polygonSymbols))
    
    # def getQuoteTypeSymbols(self):
    #     qs = scrape.yahoo.QuoteSummary()
    #     return qs.getQuoteTypeSymbols()
    
    # def getChart(self, symbol, typeName):
    #     ch = scrape.yahoo.Chart()
    #     return ch.getChart(symbol, typeName)

    def getQuickenInvestments(self):
        investments = self.savedSaved.getQuickenInvestments()
        return investments

# class TickersOld():
#     def __init__(self, logLevel=log.WARNING):
#         log.initLogger(logLevel=logLevel)
#         self.dbEntries = set()
        
#     @property
#     def dividends(self):
#         self.dbEntries.add((
#             database.TimeSeries,
#             database.TimeSeries.getDividends,
#             'dividends'))

#     @property
#     def splits(self):
#         self.dbEntries.add((
#             database.TimeSeries,
#             database.TimeSeries.getSplits,
#             'splits'))
    
#     @property
#     def prices(self):
#         self.dbEntries.add((
#             database.TimeSeries, {},
#             database.TimeSeries.getPrices,
#             'prices'))
    
#     @property
#     def volumes(self):
#         self.dbEntries.add((
#             database.TimeSeries,
#             database.TimeSeries.getVolumes,
#             'volumes'))
    
#     @property
#     def splits(self):
#         self.dbEntries.add((
#             database.TimeSeries,
#             database.TimeSeries.getSplits,
#             'splits'))

#     @property
#     def profile(self):
#         self.dbEntries.add((
#             database.Summary,
#             database.Summary.getProfiles,
#             'profile'))
    
#     @property
#     def statistics(self):
#         self.dbEntries.add((
#             database.Summary,
#             database.Summary.getStatistics,
#             'statistics'))

#     def getData(self, symbols, update=False, updateMax=False):
#         dbObjects = {}
#         # creta database objects
#         for dbClass, dbCommand, entryName in self.dbEntries:
#             if not dbClass in dbObjects:
#                 dbObjects[dbClass] = dbClass()
#         # update databases
#         if update or updateMax:
#             for dbClass, dbObject in  dbObjects.items():
#                 dbObject.update(symbols, updateMax=updateMax)
#         # retieve data from databases
#         # do not return data when running an updateMax
#         if updateMax: return {}
#         data = {}
#         for dbClass, dbCommand, entryName in self.dbEntries:
#             data[entryName] = dbCommand(dbObjects, symbols)
#         return data


