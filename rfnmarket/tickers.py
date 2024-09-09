from . import database
from .utils import log

class Tickers():
    def __init__(self, logLevel=log.WARNING):
        log.initLogger(logLevel=logLevel)
        self.__dbEntries = set()
        
    @property
    def dividends(self):
        self.__dbEntries.add((database.TimeSeries, database.TimeSeries.getDividends, 'dividends'))

    @property
    def splits(self):
        self.__dbEntries.add((database.TimeSeries, database.TimeSeries.getSplits, 'splits'))
    
    @property
    def prices(self):
        self.__dbEntries.add((database.TimeSeries, database.TimeSeries.getPrices, 'prices'))
    
    @property
    def volumes(self):
        self.__dbEntries.add((database.TimeSeries, database.TimeSeries.getVolumes, 'volumes'))
    
    @property
    def splits(self):
        self.__dbEntries.add((database.TimeSeries, database.TimeSeries.getSplits, 'splits'))

    @property
    def profile(self):
        self.__dbEntries.add((database.Profiles, database.Profiles.getProfiles, 'profile'))

    def getData(self, symbols, update=False, updateMax=False):
        dbObjects = {}
        # creta database objects
        for dbClass, dbCommand, entryName in self.__dbEntries:
            if not dbClass in dbObjects:
                dbObjects[dbClass] = dbClass()
        # update databases
        if update or updateMax:
            for dbClass, dbObject in  dbObjects.items():
                dbObject.update(symbols, updateMax=updateMax)
        # retieve data from databases
        # do not trturn data when running an updateMax
        if updateMax: return {}
        data = {}
        for dbClass, dbCommand, entryName in self.__dbEntries:
            data[entryName] = dbCommand(dbObjects, symbols)
        return data


