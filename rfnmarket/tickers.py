from . import scrape
from .utils import log
from pprint import pp
from . import vault
from datetime import datetime

class Tickers():
    def __init__(self, logLevel=log.WARNING):
        log.initLogger(logLevel=logLevel)
        self.vdata = vault.Data()

    def updateData(self, catalogs, keyValues, forceUpdate=False):
        self.vdata.updateData(catalogs, keyValues, forceUpdate=forceUpdate)

    def getData(self, catalogs, keyValues=[], update=False, forceUpdate=False):
        data = self.vdata.getData(catalogs, keyValues=keyValues, update=update, forceUpdate=forceUpdate)
        return data
   
    def getDataCatalog(self):
        return self.vdata.getCatalog()

    def getQuickenInvestments(self):
        return self.vdata.getQuickenInvestments()
