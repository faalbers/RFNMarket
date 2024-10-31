from . import scrape, vault, report
from .utils import log, utils, GICS, QIF
from pprint import pp
from datetime import datetime, time, timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class Tickers():
    def __init__(self, logLevel=log.WARNING):
        log.initLogger(logLevel=logLevel)
        self.vdata = vault.Data()

    def getProfile(self, symbols=[], update = False):
        data = self.vdata.getData(['profile'], symbols, update=update)['profile']

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
        qif = QIF('database/2020.QIF')
        qAccounts = qif.getAcounts(accType='Invst')
        qSecurities = qif.getSecurities()

        securities = pd.DataFrame(qSecurities)
        securities.set_index('name', verify_integrity=True, inplace=True)

        investments = {}
        for account in qAccounts:
            transactions = pd.DataFrame(account['transactions'])
            transactions.sort_values(by='date', inplace=True)
            def applyValue(sec, column):
                if sec == sec: return securities.loc[sec,column]
            transactions['secSymbol'] = transactions['security'].apply(applyValue,args=('symbol',))
            transactions['secType'] = transactions['security'].apply(applyValue,args=('type',))
            investments[account['name']] = transactions
        
        return investments
    
    def makePortfolioReport(self):
        # Set display options to show all columns and rows
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', None)
        
        portfolioData = self.getQuickenInvestments()

        for accountName , qTrs in portfolioData.items():
            if accountName != 'FIDELITY_ Frank Roth': continue
            print(accountName)
            # ['date', 'amount', 'action', 'transferAccount', 'security', 'price', 'shares', 'commission', 'description', 'memo', 'secSymbol', 'secType']
            trs = qTrs[['date', 'action', 'secSymbol', 'shares', 'price', 'amount']]
            print(trs)
            # break

    def getQuickenStockTransactions(self, update=False):
        quickenData = self.vdata.getData(['quicken'], update=update)['quicken']

        # set in and out transaction
        symbolTransactions = {}
        for entry in quickenData:
            rowData = {}
            if not 'shares' in entry: continue
            symbol = entry.pop('symbol')
            rowData['date'] = pd.Timestamp(datetime.fromtimestamp(entry.pop('timestamp'))).date()
            rowData['shares'] = entry['shares']
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
            dfIn = df[df['transaction'].isin(['Buy', 'ShrsIn', 'ReinvLg', 'ReinvSh', 'ReinvDiv', 'ReinvInt'])]
            dfOut = df[~df['transaction'].isin(['Buy', 'ShrsIn', 'ReinvLg', 'ReinvSh', 'ReinvDiv', 'ReinvInt'])]
            sharesOwned = dfIn['shares'].sum() - dfOut['shares'].sum()
            if sharesOwned < 0.001: continue
            df.sort_values(by='date', inplace=True)
            symbolDfs[symbol] = df
        
        return symbolDfs
    
    def getTimeSeries(self, symbols, update=False):
        timeSeriesData = self.vdata.getData(['timeSeries'], keyValues=symbols, update=update)['timeSeries']
        if not 'chart' in timeSeriesData: return {}
        timeSeriesData = timeSeriesData['chart']

        timeseries = {}
        for symbol, tsData in timeSeriesData.items():
            df = pd.DataFrame(tsData).T
            df.sort_index(inplace=True)
            df.index = pd.to_datetime(df.index, unit='s').date
            timeseries[symbol] = df
        return timeseries

    def getClose(self, symbols, startDate, endDate, update=False):
        # keep for now because it has reusable date functionality to be implemented later in getTimeSeries
        startTS = int(datetime.combine(startDate, time()).timestamp())
        endTS = int((datetime.combine(endDate, time())+timedelta(days=1)).timestamp())
        data = self.vdata.getData(['timeSeries'], keyValues=symbols, update=update)['timeSeries']['chart']

        # setup close DataFrame
        dateRange = pd.date_range(startDate, endDate)
        dfClose = pd.DataFrame(index=dateRange)

        # join all symbol close prices
        for symbol, tsData in data.items():
            timestamps = set([x for x in tsData.keys() if startTS < x < endTS])
            subset = {k: tsData[k] for k in timestamps}
            df = pd.DataFrame(subset).T
            df.sort_index(inplace=True)
            df.index = pd.to_datetime(df.index, unit='s').date

            if not df.index.is_unique:
                raise ValueError('Tickers.getClose: timeseries index is not unique')
            dfClose = dfClose.join(df['close'])
            dfClose.rename(columns={'close': symbol}, inplace=True)
        
        # drop all rows that have all values nan
        dfClose.dropna(axis=0, how='all', inplace=True)

        # fill values forward and backwards
        dfClose.infer_objects(copy=False).ffill(inplace=True)
        dfClose.infer_objects(copy=False).bfill(inplace=True)
        
        return dfClose

    def makeStocksReport(self, reportName, update=False):
        # get quicken data for all symbols
        quickenData = self.getQuickenStockTransactions(update=update)
        symbols = list(quickenData.keys())
        symbols.sort()

        # time series data
        tsData = self.vdata.getData(['timeSeries'], keyValues=symbols, update=update)['timeSeries']['chart']
        closeType = 'adjclose'

        # profile data
        profileData = self.getProfile(symbols)

        # collect start and end dates for portfolio resume
        startDate = pd.Timestamp(datetime.now()).date()
        endDate = pd.Timestamp(datetime.now()).date()

        # start a report
        qReport = report.Report(reportName)
        # qReport.printStyles()

        # handle each asset
        for symbol in symbols:
            # no point if no price history
            if not symbol in tsData: continue
            # if symbol != 'VITAX': continue

            # make time series data frame
            dfTS = pd.DataFrame(tsData[symbol]).T
            
            # profile data
            name = profileData[symbol]['name']
            columns = ['type', 'fundFamily', 'exchange', 'sector', 'industry', 'country', 'city', 'state']
            row = {}
            for info in columns:
                if info in profileData[symbol]:
                    row[info] = profileData[symbol][info]
            profile = pd.DataFrame([row], columns=columns).dropna(axis=1)
            symbolLine = '%s: %s' % (symbol, name)
            qReport.addParagraph(symbolLine, qReport.getStyle('Heading1'))
            qReport.addTable(profile)
            if 'info' in profileData[symbol]:
                qReport.addSpace(0.1)
                qReport.addParagraph(profileData[symbol]['info'], qReport.getStyle('BodyText'))

            # get symbol quicken Data Frame
            dfQuicken = quickenData[symbol]
            sharesCount = 0

            # get total incoming transactions
            sharesInRows = []
            dfIn = dfQuicken[dfQuicken['transaction'].isin(['Buy', 'ShrsIn', 'ReinvLg', 'ReinvSh', 'ReinvDiv', 'ReinvInt'])]
            sharesCount += dfIn['shares'].sum()
            firstDate = dfIn.iloc[0]['date']
            inTransactions = list(set(dfIn['transaction'].to_list()))
            inTransactions.sort()
            for transaction in inTransactions:
                dfTrs = dfIn[dfIn['transaction'] == transaction]
                shares = dfTrs['shares'].dropna().sum()
                cost = dfTrs['costBasis'].dropna().sum()
                price = cost / shares
                sharesInRows.append({'transaction': transaction, 'shares': shares, 'cost': cost, 'average price': price})
            sharesIn = pd.DataFrame(sharesInRows, columns=['transaction', 'shares', 'average price', 'cost'])
            if len(sharesIn) > 0:
                qReport.addParagraph('Incoming Shares: start date: %s' % firstDate.strftime('%Y-%m-%d'), qReport.getStyle('Heading2'))
                qReport.addTable(sharesIn.round(4))
            
            # get total outgoing transactions
            sharesOutRows = []
            dfOut = dfQuicken[~dfQuicken['transaction'].isin(['Buy', 'ShrsIn', 'ReinvLg', 'ReinvSh', 'ReinvDiv', 'ReinvInt'])]
            sharesCount -= dfOut['shares'].sum()
            outTransactions = list(set(dfOut['transaction'].to_list()))
            outTransactions.sort()
            for transaction in outTransactions:
                dfTrs = dfOut[dfOut['transaction'] == transaction]
                shares = dfTrs['shares'].dropna().sum()
                gain = dfTrs['costBasis'].dropna().sum()
                price = gain / shares
                sharesOutRows.append({'transaction': transaction, 'shares': shares, 'gain': gain, 'average price': price})
            sharesOut = pd.DataFrame(sharesOutRows, columns=['transaction', 'shares', 'average price', 'gain'])
            if len(sharesOut) > 0:
                qReport.addParagraph('Outgoing Shares:', qReport.getStyle('Heading2'))
                qReport.addTable(sharesOut.round(4))

            # shares in portfolio
            qReport.addParagraph('Shares in Portfolio:', qReport.getStyle('Heading2'))
            tsLast = dfTS.iloc[-1]
            date = str(datetime.fromtimestamp(int(tsLast.name)).date())
            price = float(tsLast[closeType])
            value = sharesCount * price
            columns = ['last close date', 'shares', 'price', 'value']
            row = {'last close date': date, 'shares': sharesCount, 'price': price, 'value': value}
            portfolio = pd.DataFrame([row], columns=['last close date', 'shares', 'price', 'value'])
            qReport.addTable(portfolio.round(4))

            # start chart from first investment date
            fromTS = int(datetime.combine(firstDate, time()).timestamp())
            chartFirst = dfTS.loc[fromTS:]

            # plot price
            dfPrice = chartFirst[[closeType]].copy()
            dfPrice[closeType] = dfPrice[closeType].div(dfPrice.iloc[0][closeType])
            dfPrice.index = pd.to_datetime(dfPrice.index, unit='s').date
            dfPrice.index = dfPrice.index.astype(str) # need to do this for plotting to work correctly
            qReport.plotLineDF(dfPrice, ylabel='price', yline=1.0)
            
            qReport.addPageBreak()

            # get dividends payed per time series data
            divRows = []
            if 'dividend' in chartFirst.columns:
                dfDividends = chartFirst.dropna(subset = ['dividend'])
                if len(dfDividends) > 0:
                    #  we got dividends in the time series
                    for index , values in dfDividends.iterrows():
                        # find from quicken how many shares we have collected by that date
                        date = datetime.fromtimestamp(index).date()
                        dfInToDate = dfIn[dfIn['date'] <= date]
                        dfOutToDate = dfOut[dfOut['date'] <= date]
                        sharesToDate = dfInToDate['shares'].sum() - dfOutToDate['shares'].sum()
                        if sharesToDate < 0.01: continue
                        dividendPayed = sharesToDate * values['dividend']
                        # divShares = dividendPayed / values[closeType]
                        row = {'date': date, 'div/share': values['dividend'], 'shares': sharesToDate, 'div payed': dividendPayed}
                        divRows.append(row)
            divPortfolio = pd.DataFrame(divRows)
            if len(divPortfolio) > 0:
                qReport.addParagraph('Dividends per data:', qReport.getStyle('Heading2'))
                qReport.addTable(divPortfolio.round(4))
            
            # get quicken dividend reinvestments
            divReinvest = dfIn[dfIn['transaction'].isin(['ReinvDiv'])]
            if len(divReinvest) > 0:
                qReport.addParagraph('Dividends reinvested:', qReport.getStyle('Heading2'))
                qReport.addTable(divReinvest.round(4))
            elif len(divPortfolio) > 0:
                style = qReport.getStyle('Heading2')
                style.textColor = qReport.colors.red
                qReport.addParagraph('Dividends reinvested:', style)

            # get dividends history for 5 years
            fromTS = int(datetime.now().timestamp() - (60*60*24*356*5))
            chartFiveYear = dfTS.loc[fromTS:]
            if 'dividend' in chartFiveYear.columns:
                # drop all rows with no dividend info
                chartFiveYear = chartFiveYear.dropna(subset = ['dividend'])
                # check if we still have data
                if len(chartFiveYear) > 0:
                    # find dividends distributed since we owned stock
                    fromTS = int(datetime.combine(firstDate, time()).timestamp())
                    chartFiveYear['owned'] = chartFiveYear.index >= fromTS
                    # chartFiveYear['divpercent'] = (chartFiveYear['dividend'] / chartFiveYear[closeType]) * 100.0
                    chartFiveYear.index = pd.to_datetime(chartFiveYear.index, unit='s').date
                    chartFiveYear.index = chartFiveYear.index.astype(str)
                    divColors = [("red" if x else "blue") for x in chartFiveYear['owned']]
                    qReport.addParagraph('Dividend ($) per share:', qReport.getStyle('Heading2'))
                    qReport.plotBarDF(chartFiveYear[['dividend']], ylabel='$ / share', colors=divColors)
            
            qReport.addPageBreak()

        qReport.buildDoc()
    
    def createDataOverview(self, fileName):
        symbols = self.getData(['ussymbols'])['ussymbols']
        data = self.getData(['all'], symbols[:1000])
        allData = {}
        utils.dataStructure(data, allData, set(symbols))
        with open(fileName, 'w', encoding="utf-8") as f:
            utils.printHierachy(allData, f, 0)


    
