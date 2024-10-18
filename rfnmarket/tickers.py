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
        unknownSubIndustries = set(['Shell Companies', 'Scientific & Technical Instruments'])
        thaList = set()
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
            elif not industry in unknownSubIndustries:
                thaList.add(industry)
            if len(symbolGICS) > 0:
                symbolsGICS[symbol] = symbolGICS
        
        for symbol, values in df.dropna(subset = ['SP500sector']).iterrows():
            sector = values['SP500sector']
            if not symbol in symbolsGICS:
                symbolsGICS[symbol] = {'sector': sector}
        
        thaList = set()
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
                elif not industry in unknownSubIndustries:
                    thaList.add(industry)
                if len(symbolGICS) > 0:
                    print(symbolGICS)
                    # symbolsGICS[symbol] = symbolGICS

        with open('torename.txt', 'w') as f:
            pp(thaList, f)
        with open('gics.txt', 'w') as f:
            pp(gics.getNames('subIndustry'), f)

            # symbolGICS = {}
            # if sector in sectors:
            #     symbolGICS['sector'] = sector
            # elif sector in industryGroups:
            #     symbolGICS['sector'] = industryGroups[sector][0]
            #     symbolGICS['industryGroup'] = sector
            # else:
            #     sector = gics.renameSector(sector)
            #     symbolGICS['sector'] = sector
            
            # if industry in industries:
            #     symbolGICS['industry'] = industry
            #     if not 'sector' in symbolGICS:
            #         symbolGICS['sector'] = industries[industry][0]
            #     elif industries[industry][0] != symbolGICS['sector']:
            #         raise ValueError ('industry: "%s" does fit under sector: "%s"' % (industry, symbolGICS['sector']))
            #     if not 'industryGroup' in symbolGICS:
            #         symbolGICS['industryGroup'] = industries[industry][1]
            #     elif industries[industry][1] != symbolGICS['industryGroup']:
            #         raise ValueError ('industry: "%s" does fit under industryGroup: "%s"' % (industry, symbolGICS['industryGroup']))
            # elif industry in subIndustries:
            #     symbolGICS['subIndustry'] = industry
            #     if not 'sector' in symbolGICS:
            #         symbolGICS['sector'] = subIndustries[industry][0]
            #     elif subIndustries[industry][0] != symbolGICS['sector']:
            #         # raise ValueError ('subIndustry: "%s" does fit under sector: "%s"' % (industry, symbolGICS['sector']))
            #         print('subIndustry: "%s" does fit under sector: "%s"' % (industry, symbolGICS['sector']))
            #         print('it should be: %s' % subIndustries[industry])
            #     if not 'industryGroup' in symbolGICS:
            #         symbolGICS['industryGroup'] = subIndustries[industry][1]
            #     elif subIndustries[industry][1] != symbolGICS['industryGroup']:
            #         # raise ValueError ('subIndustry: "%s" does fit under industryGroup: "%s"' % (industry, symbolGICS['industryGroup']))
            #         print('subIndustry: "%s" does fit under industryGroup: "%s"' % (industry, symbolGICS['industryGroup']))
            #         print('it should be: %s' % subIndustries[industry])
            #     if not 'industry' in symbolGICS:
            #         symbolGICS['industry'] = subIndustries[industry][2]
            #     elif subIndustries[industry][2] != symbolGICS['industry']:
            #         raise ValueError ('subIndustry: "%s" does fit under industry: "%s"' % (industry, symbolGICS['industry']))

            #     # if 'sector' in symbolGICS and industries[industry][0] != symbolGICS['sector']:
            #     #     print()
            #     #     print(industry)
            #     #     print(industries[industry])
            #     #     print(symbolGICS)
            #     # if 'industryGroup' in symbolGICS and industries[industry][1] != symbolGICS['industryGroup']:
            #     #     print()
            #     #     print(industry)
            #     #     print(industries[industry])
            #     #     print(symbolGICS)
            #     industryTypesFound.add('subIndustry')
            # else:
            #     industriesNotFound.add(industry)

            # symbolsGICS[symbol] = symbolGICS
        # pp(symbolsGICS)
        # print('\nsectorTypesFound:')
        # pp(sectorTypesFound)
        # print('\nsectorsNotFound:')
        # pp(sectorsNotFound)
        # print('\nindustryTypesFound:')
        # pp(industryTypesFound)
        # print('\nindustriesNotFound:')
        # pp(industriesNotFound)

        # sectors = gics.getSectors()
        # for sector in sectors:
        #     print('%s' % sector)
        #     industryGroups = gics.getIndustryGroups(sector=sector)
        #     for industryGroup in industryGroups:
        #         print('\t%s' % industryGroup)
        #         industries = gics.getIndustries(sector=sector, industryGroup=industryGroup)
        #         for industry in industries:
        #             print('\t\t%s' % industry)
        
        # print('\nall industryGroups:')
        # for industryGroup in gics.getIndustryGroups():
        #     print(industryGroup)
        
        
        
        # for symbol, values in df.dropna(subset = ['sector']).iterrows():
        #     sector = values['sector']
        #     if sector == '': continue
        #     industry = values['industry']
        #     names = gics.getNames(sector=sector, industry=industry)
        #     if names == None:
        #         return
        
        # for symbol, values in df.dropna(subset = ['SP500sector']).iterrows():
        #     sector = values['SP500sector']
        #     names = gics.getNames(sector=sector)
        #     if names == None:
        #         return
        
        # for symbol, values in df.dropna(subset = ['NASDAQsector']).iterrows():
        #     sector = values['NASDAQsector']
        #     if sector == 'Miscellaneous':
        #         # not sure what to do yet
        #         continue

        #     names = gics.getNames(sector=sector)
        #     if names == None:
        #         return
        
        # sectorRename = {
        #     '': 'Unknown',
        #     'Basic Materials': 'Materials',
        #     'Consumer Defensive': 'Consumer Staples',
        #     'Consumer Staples': 'Consumer Staples',
        #     'Consumer Cyclical': 'Consumer Discretionary',
        #     'Consumer Discretionary': 'Consumer Discretionary',
        #     'Financial Services': 'Financials',
        #     'Finance': 'Financials',
        #     'Technology': 'Information Technology',
        #     'Communication Services': 'Communication Services',
        #     'Telecommunications': 'Communication Services',
        #     'Energy': 'Energy',
        #     'Healthcare': 'Healthcare',
        #     'Health Care': 'Healthcare',
        #     'Industrials': 'Industrials',
        #     'Real Estate': 'Real Estate',
        #     'Utilities': 'Utilities',
        #     'Miscellaneous': 'Miscellaneous',
        # }
        # data = self.vdata.getData(['sectorIndustry'])['sectorIndustry']
        # df = pd.DataFrame(data).T
        # data = {}
        # # find 'sector' 'industry' data
        # for symbol, values in df.dropna(subset = ['sector']).iterrows():
        #     sector = sectorRename[values['sector']]
        #     if sector == 'Unknown': continue
        #     industry = values['industry']
        #     if not symbol in data:
        #         data[symbol] = {}
        #     data[symbol]['sector'] = sector
        #     data[symbol]['industry'] = industry
        
        
        # # find 'SP500sector' data
        # for symbol, values in df.dropna(subset = ['SP500sector']).iterrows():
        #     sector = values['SP500sector']
        #     if not sector in dataA:
        #         dataA[sector] = {}
        #     inIndustry = False
        #     for industry, symbols in dataA[sector].items():
        #         if symbol in symbols:
        #             inIndustry = True
        #             break
        #     if not inIndustry:
        #         if not 'Unknown' in dataA[sector]:
        #             dataA[sector]['Unknown'] = set()
        #         dataA[sector]['Unknown'].add(symbol)
        
        # # find 'NASDAQsector' 'NASDAQindustry' data
        # checkSet = set()
        # for symbol, values in df.dropna(subset = ['NASDAQsector']).iterrows():
        #     sector = sectorRename[values['NASDAQsector']]
        #     if sector == 'Miscellaneous':
        #         checkSet.add(symbol)
        # print(len(checkSet))
        
        # checkDict = {}
        # for sector, industryData in dataA.items():
        #     # print('%s:' % sector)
        #     for industry, symbols in industryData.items():
        #         same = symbols.intersection(checkSet)
        #         if len(same) > 0:
        #             if not sector in checkDict:
        #                 checkDict[sector] = same
        #             else:
        #                 checkDict[sector].union(same)
        # pp(checkDict)
        # # for sector, industryData in dataA.items():
        # #     print('%s:' % sector)
        # #     for industry, symbols in industryData.items():
        # #         print('%s = %s' % (industry, len(symbols)))

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


    
