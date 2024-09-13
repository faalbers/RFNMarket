from . import scrape
from .utils import log
from pprint import pp

class Tickers():
    def __init__(self, logLevel=log.WARNING):
        log.initLogger(logLevel=logLevel)
        self.scrapers = set()

    @property
    def profile(self):
        self.scrapers.add((scrape.yahoo.QuoteSummary, 'profile'))
    
    @property
    def statistics(self):
        self.scrapers.add((scrape.yahoo.QuoteSummary, 'statistics'))

    @property
    def price(self):
        self.scrapers.add((scrape.yahoo.QuoteSummary, 'price'))
        self.scrapers.add((scrape.yahoo.Chart, 'price'))

    @property
    def test(self):
        self.scrapers.add((scrape.yahoo.TimeSeries, 'test'))
    
    def update(self, symbols=None):
        scraperObjects = {}
        # gather types and create scraper objects
        scrapers = {}
        for scraper in self.scrapers:
            if not scraper[0] in scrapers:
                scrapers[scraper[0]] = []
            scrapers[scraper[0]].append(scraper[1])
        for scraperClass, types in scrapers.items():
            if not scraperClass in scraperObjects:
                scraperObjects[scraperClass] = scraperClass(symbols, types)


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


