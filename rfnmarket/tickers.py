from . import scrape, vault, report
from .utils import log
from pprint import pp
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
    
    def makeQuickenReport(self):
        quickenData = self.vdata.getQuickenInvestments()
        profileData = self.vdata.getData(['profile'], keyValues=list(quickenData.keys()))['profile']
        # with open('profileData.txt', 'w', encoding='utf-8') as f:
        #     pp(profileData, f)
        chartData = self.vdata.getData(['timeSeries'], keyValues=list(quickenData.keys()), update=True)['timeSeries']['chart']
        # with open('chartData.txt', 'w', encoding='utf-8') as f:
        #     pp(chartData, f)

        qReport = report.Report()
        qReport.makeQuickenReport(quickenData, profileData, chartData)




    
