from . import scrape, vault, report
from .utils import log, utils, GICS
from pprint import pp
from datetime import datetime
import pandas as pd

class Tickers():
    def __init__(self, logLevel=log.WARNING):
        log.initLogger(logLevel=logLevel)
        self.vdata = vault.Data()

    def getProfile(self, symbols=[]):
        data = self.vdata.getData(['profile'], symbols)['profile']

        # get all mics and acronyms that have US country code and update profile
        mics = data['mic']['ISO10383_MIC']
        usacronyms = set()
        usmics = set()
        for micRow in mics:
            if 'cc' in micRow and micRow['cc'] == 'US':
                usmics.add(micRow['mic'])
                if 'acronym' in micRow:
                    usacronyms.add(micRow['acronym'])

        for keyValue, keyData in data['profile'].items():
            if 'mic' in keyData and keyData['mic'] in usmics:
                keyData['exchangeCountry'] = 'United States'
            if 'acronym' in keyData and keyData['acronym'] in usacronyms:
                keyData['exchangeCountry'] = 'United States'

        data = data['profile']

        # GICS names
        gics = GICS()
        sectors = gics.getNames('sector')
        industryGroups = gics.getNames('industryGroup')
        industries = gics.getNames('industry')
        subIndustries = gics.getNames('subIndustry')

        for symbol, sData in data.items():
            # if symbol == 'SAND':
            #     print(sData)
            # set GICS data
            industry = None
            if 'industry' in sData:
                industry = sData.pop('industry')
            industryNASDAQ = None
            if 'industryNASDAQ' in sData:
                industryNASDAQ = sData.pop('industryNASDAQ')
            sectorSP500 = None
            if 'sectorSP500' in sData:
                sectorSP500 = sData.pop('sectorSP500')
            
            if industry == None: industry = industryNASDAQ

            symbolGICS = {}
            if industry != None and industry != '':
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
            elif sectorSP500 != None:
                symbolGICS['sector'] = sectorSP500
            
            # set market cap category
            if 'marketCap' in sData:
                if sData['marketCap'] < 50000000:
                    sData['marketCapCategory'] = 'Nano Cap'
                elif sData['marketCap'] < 300000000:
                    sData['marketCapCategory'] = 'Micro Cap'
                elif sData['marketCap'] < 2000000000:
                    sData['marketCapCategory'] = 'Small Cap'
                elif sData['marketCap'] < 10000000000:
                    sData['marketCapCategory'] = 'Mid Cap'
                elif sData['marketCap'] < 200000000000:
                    sData['marketCapCategory'] = 'Large Cap'
                else:
                    sData['marketCapCategory'] = 'Mega Cap'
            
            if len(symbolGICS) > 0:
                data[symbol] = {**sData,**symbolGICS}

        return data

    def getStatistics(self, symbols=[]):
        data = self.vdata.getData(['statistics'], symbols)['statistics']
        return data
    
    def getQuickenInvestments(self):
        quickenData = self.vdata.getData(['quicken'])['quicken']

        # set in and out transaction
        symbolTransactions = {}
        for entry in quickenData:
            rowData = {}
            if not 'shares' in entry: continue
            symbol = entry.pop('symbol')
            rowData['date'] = pd.Timestamp(datetime.fromtimestamp(entry.pop('timestamp'))).to_datetime64()
            if entry['transaction'] in ['Buy', 'ShrsIn', 'ReinvLg', 'ReinvSh', 'ReinvDiv', 'ReinvInt']:
                rowData['shares'] = entry['shares']
            else:
                rowData['shares'] = -entry['shares']
            rowData['transaction'] = entry['transaction']
            if 'price' in entry: rowData['price'] = entry['price']
            if 'costBasis' in entry: rowData['costBasis'] = entry['costBasis']
            if not symbol in symbolTransactions:
                symbolTransactions[symbol] = []
            symbolTransactions[symbol].append(rowData)

        # get symbol DataFrames that still have shares
        symbolDfs = {}
        columns = ['date', 'transaction', 'shares', 'price', 'costBasis']
        for symbol, trs in symbolTransactions.items():
            df = pd.DataFrame(trs, columns=columns)
            sharesOwned = df['shares'].sum()
            if sharesOwned < 0.001: continue
            symbolDfs[symbol] = df
        
        return symbolDfs

    def makeQuickenReport(self):
        quickenData = self.getQuickenInvestments()
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


    
