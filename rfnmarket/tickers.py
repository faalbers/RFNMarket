from . import scrape, vault, report
from .utils import log, utils, GICS
from pprint import pp
from datetime import datetime
import pandas as pd

class Tickers():
    def __init__(self, logLevel=log.WARNING):
        log.initLogger(logLevel=logLevel)
        self.vdata = vault.Data()

    def getSymbolsGICS(self, sectors=[], industryGroups=[], industries=[], subIndustries=[]):
        data = self.vdata.getData(['GICS'])['GICS']
        df = pd.DataFrame(data).T
        gics = GICS()

        sectors = gics.getNames('sector')
        industryGroups = gics.getNames('industryGroup')
        industries = gics.getNames('industry')
        subIndustries = gics.getNames('subIndustry')

        symbolsGICS = {}
        for symbol, values in df.dropna(subset = ['sector']).iterrows():
            sector = values['sector']
            if sector == '': continue
            industry = values['industry']

            symbolGICS = {}
            if gics.renameSubIndustry(industry) in subIndustries:
                industry = gics.renameSubIndustry(industry)
                symbolGICS['sector'] = subIndustries[industry][0]
                symbolGICS['industryGroup'] = subIndustries[industry][1]
                symbolGICS['industry'] = subIndustries[industry][2]
                symbolGICS['subIndustry'] = industry
            elif gics.renameIndustry(industry) in industries:
                industry = gics.renameIndustry(industry)
                symbolGICS['sector'] = industries[industry][0]
                symbolGICS['industryGroup'] = industries[industry][1]
                symbolGICS['industry'] = industry
            elif gics.renameIndustryGroup(industry) in industryGroups:
                industry = gics.renameIndustryGroup(industry)
                symbolGICS['sector'] = industryGroups[industry][0]
                symbolGICS['industryGroup'] = industry
            if len(symbolGICS) > 0:
                symbolsGICS[symbol] = symbolGICS
        
        for symbol, values in df.dropna(subset = ['SP500sector']).iterrows():
            sector = values['SP500sector']
            if not symbol in symbolsGICS:
                symbolsGICS[symbol] = {'sector': sector}
        
        for symbol, values in df.dropna(subset = ['NASDAQindustry']).iterrows():
            industry = values['NASDAQindustry']
            if not symbol in symbolsGICS:
                symbolGICS = {}
                if gics.renameSubIndustry(industry) in subIndustries:
                    industry = gics.renameSubIndustry(industry)
                    symbolGICS['sector'] = subIndustries[industry][0]
                    symbolGICS['industryGroup'] = subIndustries[industry][1]
                    symbolGICS['industry'] = subIndustries[industry][2]
                    symbolGICS['subIndustry'] = industry
                elif gics.renameIndustry(industry) in industries:
                    industry = gics.renameIndustry(industry)
                    symbolGICS['sector'] = industries[industry][0]
                    symbolGICS['industryGroup'] = industries[industry][1]
                    symbolGICS['industry'] = industry
                elif gics.renameIndustryGroup(industry) in industryGroups:
                    industry = gics.renameIndustryGroup(industry)
                    symbolGICS['sector'] = industryGroups[industry][0]
                    symbolGICS['industryGroup'] = industry
                elif gics.renameSector(industry) in sectors:
                    industry = gics.renameSector(industry)
                    symbolGICS['sector'] = industry
                if len(symbolGICS) > 0:
                    symbolsGICS[symbol] = symbolGICS
        return symbolsGICS

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

    def createDataOverview(self, fileName):
        symbols = self.getData(['ussymbols'])['ussymbols']
        data = self.getData(['all'], symbols[:1000])
        allData = {}
        utils.dataStructure(data, allData, set(symbols))
        with open(fileName, 'w', encoding="utf-8") as f:
            utils.printHierachy(allData, f, 0)


    
