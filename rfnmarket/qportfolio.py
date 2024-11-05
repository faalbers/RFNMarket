from . import Tickers
from .report import Report
from .utils import QIF, log, utils
import pandas as pd
from pprint import pp
from datetime import datetime
import matplotlib.pyplot as plt

# portfolio based on Quicken QIF file
class QPortfolio():
    __sharesIn = ['Buy', 'ShrsIn', 'ReinvDiv', 'ReinvLg', 'ReinvSh']
    __sharesOut = ['Sell', 'ShrsOut']
    __receiveDividend = ['Div']
    __reinvDividend = ['ReinvDiv']
    __reinvCapitalGain = ['ReinvLg', 'ReinvSh']
    __allActions = __sharesIn+__sharesOut+__receiveDividend

    def __init__(self, qifFile):
        self.__tickers = Tickers(logLevel=log.DEBUG)
        qif = QIF(qifFile)
        qAccounts = qif.getAcounts(accType='Invst')
        qSecurities = qif.getSecurities()

        securities = pd.DataFrame(qSecurities)
        self.__securities = securities.copy()
        self.__securities.drop_duplicates(subset='symbol', keep='first', inplace=True)
        # self.__securities.reset_index().drop_duplicates(subset='symbol', keep='first')
        self.__securities.set_index('symbol', verify_integrity=True, inplace=True)
        # self.__securities = self.__securities.reset_index().drop_duplicates(subset='index', keep='first').set_index('index')

        securities.set_index('name', verify_integrity=True, inplace=True)
        self.__accounts = {}
        for account in qAccounts:
            transactions = pd.DataFrame(account['transactions'])
            transactions.sort_values(by='date', inplace=True)
            def applyValue(sec, column):
                if sec == sec: return securities.loc[sec,column]
            transactions['secSymbol'] = transactions['security'].apply(applyValue,args=('symbol',))
            transactions['secType'] = transactions['security'].apply(applyValue,args=('type',))
            self.__accounts[account['name']] = transactions

    def getAccountNames(self):
        accNames = list(self.__accounts.keys())
        accNames.sort()
        return accNames
    
    def getAllShareActions(self):
        allActions = set()
        for account, transactions in self.__accounts.items():
            dfShares = transactions.dropna(subset = ['shares'])
            actions = set(dfShares['action'].to_list())
            allActions = allActions.union(actions)
        allActions = list(allActions)
        allActions.sort()
        return allActions
    
    def getShareActions(self, account, symbol=None):
        if not account in self.__accounts: return []
        dfShares = self.__accounts[account].dropna(subset = ['shares'])
        if symbol == None:
            actions = list(set(dfShares['action'].to_list()))
        else:
            actions = list(set(dfShares[dfShares['secSymbol'] == symbol]['action'].to_list()))
        actions.sort()
        return actions
    
    def getActions(self, account, symbol=None):
        if not account in self.__accounts: return []
        dfShares = self.__accounts[account]
        if symbol == None:
            actions = list(set(dfShares['action'].to_list()))
        else:
            actions = list(set(dfShares[dfShares['secSymbol'] == symbol]['action'].to_list()))
        actions.sort()
        return actions
    
    def getAccountSecurityShares(self, account, withShares=True):
        accData = self.__accounts[account]
        sharesIn = accData[accData['action'].isin(self.__sharesIn)]
        sharesOut = accData[accData['action'].isin(self.__sharesOut)]
        symbols = set(sharesIn['secSymbol'].to_list() + sharesOut['secSymbol'].to_list())
        secShares = {}
        for symbol in symbols:
            shIn = sharesIn[sharesIn['secSymbol'] == symbol]['shares'].sum()
            shOut = sharesOut[sharesOut['secSymbol'] == symbol]['shares'].sum()
            shLeft = shIn-shOut
            if withShares:
                if shLeft > 0.01: secShares[symbol] = shLeft
            else:
                secShares[symbol] = shLeft
        return secShares
    
    def getSecurityShares(self):
        symbols = set()
        for account in self.getAccountNames():
            sShares = self.getAccountSecurityShares(account)
            symbols = symbols.union(sShares.keys())
        symbols = list(symbols)
        symbols.sort()
        return symbols
    
    def getInvestmentHistory(self, account, update=False):
        nowDate = datetime.now().date()
        accData = self.__accounts[account]
        firstDate = accData.iloc[0]['date']
        symbols = set(accData['secSymbol'].dropna().to_list()).difference({'Cash', 'CD'})
        timeSeries = self.__tickers.getTimeSeries(symbols, update=update)

        investmentHistory = {}
        for symbol in symbols:
            sAccData = accData[accData['secSymbol'] == symbol]
            sFirstDate = sAccData.iloc[0]['date']

            # close price
            if symbol in timeSeries:
                dateRange = pd.date_range(sFirstDate, nowDate).date
                ts = pd.DataFrame(index=dateRange)
                ts = ts.join(timeSeries[symbol]['close'])
                ts = ts.infer_objects(copy=False).ffill()
                ts = ts.infer_objects(copy=False).bfill()

            # get in shares
            sharesIn = sAccData[sAccData['action'].isin(self.__sharesIn)][['date', 'shares', 'amount']]
            sharesIn.set_index('date', inplace=True)
            sharesIn = sharesIn.dropna(subset = ['shares'])
            if sharesIn['amount'].isna().any() and symbol in timeSeries:
                sharesIn = sharesIn.join(ts)
                sharesIn['amount'] = sharesIn['amount'].fillna(sharesIn['shares']*sharesIn['close'])
                sharesIn.drop('close', axis=1, inplace=True)
            
            # get out shares and negate them
            sharesOut = sAccData[sAccData['action'].isin(self.__sharesOut)][['date', 'shares', 'amount']]
            sharesOut.set_index('date', inplace=True)
            sharesOut = sharesOut.dropna(subset = ['shares'])
            if sharesOut['amount'].isna().any():
                if symbol in timeSeries:
                    sharesOut = sharesOut.join(ts)
                    sharesOut['amount'] = sharesOut['amount'].fillna(sharesOut['shares']*sharesOut['close'])
                    sharesOut.drop('close', axis=1, inplace=True)
                elif symbol == '^IRX' or symbol.startswith('91279'):
                    # this is a hack for now
                    sharesOut['amount'] = sharesOut['amount'].fillna(sharesOut['shares'])
            sharesOut.loc[:,'shares'] = -sharesOut['shares']
            sharesOut.loc[:,'amount'] = -sharesOut['amount']
            
            # concat them together and sum duplicate dates
            shares = pd.concat([sharesIn, sharesOut])[['shares', 'amount']]
            shares.sort_index(inplace=True)
            shares = shares.groupby(shares.index).sum() 

            # add dividend payments
            divAmount = sAccData[sAccData['action'].isin(self.__receiveDividend)][['date', 'amount']]
            divAmount.rename(columns={'amount': 'divAmount'}, inplace=True)
            divAmount.set_index('date', inplace=True)
            divAmount = divAmount.groupby(divAmount.index).sum()
            shares = shares.join(divAmount, how='outer')
            
            # add dividend reinvestments
            divReinv = sAccData[sAccData['action'].isin(self.__reinvDividend)][['date', 'shares', 'amount']]
            divReinv.rename(columns={'shares': 'divReinvShares', 'amount': 'divReinvAmount'}, inplace=True)
            divReinv.set_index('date', inplace=True)
            divReinv = divReinv.groupby(divReinv.index).sum()
            shares = shares.join(divReinv, how='outer')
            
            # add cap gain reinvestments
            cgReinv = sAccData[sAccData['action'].isin(self.__reinvCapitalGain)][['date', 'shares', 'amount']]
            cgReinv.rename(columns={'shares': 'cgReinvShares', 'amount': 'cgReinvAmount'}, inplace=True)
            cgReinv.set_index('date', inplace=True)
            cgReinv = cgReinv.groupby(cgReinv.index).sum()
            shares = shares.join(cgReinv, how='outer')
            

            # drop all columns with only nan values
            shares.dropna(axis=1, how='all', inplace=True)
            
            investmentHistory[symbol] = shares
        
        return investmentHistory

    def test(self, accountNames=[], update=False):
        if False:
            # just temporary for testing
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('display.width', None)

        if len(accountNames) == 0:
            accountNames = self.getAccountNames()
        else:
            accountNames = set(accountNames).intersection((self.getAccountNames()))

        for accountName in accountNames:
            # ehData = self.getEventReinvestHistory(accountName, update=update)
            result = self.getInvestmentHistory(accountName, update=update)
            # pp(result)
    
    def makeReports(self, accountNames=[], update=False):
        if len(accountNames) == 0:
            accountNames = self.getAccountNames()
        else:
            accountNames = set(accountNames).intersection((self.getAccountNames()))

        for accountName in accountNames:
            ihData = self.getInvestmentHistory(accountName, update=update)
            
            # find symbols with shares left and the very first investment date
            nowDate = datetime.now().date()
            firstDate = nowDate
            symbols = set()
            for symbol, sData in ihData.items():
                if sData['shares'].sum() > 0:
                    symbols.add(symbol)
                    if sData.index[0] < firstDate: firstDate = sData.index[0]
            
            # utils.printDataFrame(sData, 'dataframe.txt', append=True)

            # no shares left, no need to continue
            if len(symbols) == 0: continue
            symbols = list(symbols)
            symbols.sort()

            # get timeseries for symbols and spread them to full date range
            timeSeries = self.__tickers.getTimeSeries(symbols, update=update)

            # setup account data
            dateRangeAll = pd.date_range(firstDate, datetime.now().date()).date
            dfAmountsAll = pd.DataFrame(index=dateRangeAll)
            dfValuesAll = pd.DataFrame(index=dateRangeAll)
            sInv = {}
            for symbol in symbols:
                sDateRange = pd.date_range(ihData[symbol].index[0], nowDate).date
                
                # get timeseries for symbol investment data range
                if symbol in timeSeries:
                    ts = pd.DataFrame(index=sDateRange)
                    ts = ts.join(timeSeries[symbol]['close'])
                    ts = ts.infer_objects(copy=False).ffill()
                    ts = ts.infer_objects(copy=False).bfill()

                # fill investment data for each entry
                sIhData = ihData[symbol][['shares', 'amount']].dropna(subset = ['shares'])
                # sum all investments
                dfSharesSymbol = pd.DataFrame(index=sDateRange)
                dfAmountsSymbol = pd.DataFrame(index=sDateRange)
                invCount = 0
                for date, row in sIhData.iterrows():
                    dateRange = pd.date_range(date, nowDate).date
                    invName = 'INV%04d' % invCount
                    
                    # gather shares
                    dfShares = pd.DataFrame(index=dateRange)
                    dfShares.at[date, invName] = row['shares']
                    dfShares = dfShares.infer_objects(copy=False).ffill()
                    dfSharesSymbol = dfSharesSymbol.join(dfShares)

                    # gather amounts
                    dfAmounts = pd.DataFrame(index=dateRange)
                    dfAmounts.at[date, invName] = row['amount']
                    dfAmounts = dfAmounts.infer_objects(copy=False).ffill()
                    dfAmountsSymbol = dfAmountsSymbol.join(dfAmounts)

                    invCount += 1
                
                # gather all data frames
                symbolInv = pd.DataFrame(index=sDateRange)
                symbolInv['shares'] = dfSharesSymbol.sum(axis=1)
                symbolInv['amount'] = dfAmountsSymbol.sum(axis=1)

                # add more info columns to symbolInv
                symbolInv['priceBuy'] = symbolInv['amount']/symbolInv['shares']
                if symbol in timeSeries:
                    symbolInv = symbolInv.join(ts)
                    symbolInv.rename(columns={'close': 'priceClose'}, inplace=True)
                    symbolInv['value'] = symbolInv['shares'] * symbolInv['priceClose']
                else:
                    symbolInv['priceClose'] = symbolInv['priceBuy']
                    symbolInv['value'] = symbolInv['amount']
                symbolInv['gain$'] = symbolInv['value'] - symbolInv['amount']
                symbolInv['gain%'] = ((symbolInv['value']/symbolInv['amount'])* 100.0)-100.0
                symbolInv['priceClose%'] = ((symbolInv['priceClose']/symbolInv.iloc[0]['priceClose'])*100.0)-100.0
                
                # utils.printDataFrame(symbolInv, 'dataframe.txt', append=True)
                
                # add symbol investments
                sInv[symbol] = symbolInv

                # now we can add to totals for summing
                dfAmountsAll = dfAmountsAll.join(symbolInv['amount'])
                dfAmountsAll.rename(columns={'amount': symbol}, inplace=True)
                dfValuesAll = dfValuesAll.join(symbolInv['value'])
                dfValuesAll.rename(columns={'value': symbol}, inplace=True)
            
            # sum all symbol values
            allInv = pd.DataFrame(index=dateRangeAll)
            allInv['amount'] = dfAmountsAll.sum(axis=1)
            allInv['value'] = dfValuesAll.sum(axis=1)
            allInv['gain$'] = allInv['value'] - allInv['amount']
            allInv['gain%'] = ((allInv['value']/allInv['amount'])* 100.0)-100.0

            lastDate = allInv.index[-1]
            totalPayed = allInv.at[lastDate, 'amount']
            
            # utils.printDataFrame(allInv, 'dataframe.txt', append=True)
            
            # report account data
            reportName = accountName.replace(' ','_')
            qReport = Report(reportName)

            qReport.addParagraph(accountName+':', qReport.getStyle('Heading1'))

            # report holdings
            rows = []
            for symbol in symbols:
                sData = sInv[symbol]
                payed = sData.at[lastDate, 'amount']
                rowData = {'symbol': symbol,
                    'Dist %': (payed/totalPayed)*100.0,
                    'payed': payed,
                    'shares': sData.at[lastDate, 'shares'],
                    'value': sData.at[lastDate, 'value'],
                    'gain $': sData.at[lastDate, 'gain$'],
                    'gain %': sData.at[lastDate, 'gain%']}
                rows.append(rowData)
            if len(rows) == 0:
                qReport.addPageBreak()
                continue
            dfHoldings = pd.DataFrame(rows)
            qReport.addParagraph('Holdings as per %s:' % lastDate, qReport.getStyle('Heading2'))
            dfHoldings.sort_values(by='Dist %', inplace=True, ascending=False)
            totalRow = {'symbol': 'Total:',
                    'Dist %': dfHoldings['Dist %'].sum(),
                    'payed': allInv.at[lastDate, 'amount'],
                    'shares': '',
                    'value': allInv.at[lastDate, 'value'],
                    'gain $': allInv.at[lastDate, 'gain$'],
                    'gain %': allInv.at[lastDate, 'gain%']}
            dfHoldings.loc[len(dfHoldings)] = totalRow
            qReport.addTable(dfHoldings.round(4))

            qReport.plotLineDF(allInv, y = ['amount', 'value'], labels= ['payed', 'value'], colors=['green', 'blue'], height=2.5)
            qReport.plotLineDF(allInv, y = ['gain$'], labels= ['gain $'], divLine=allInv.iloc[0]['gain$'], colors=['blue'], height=2.5)
            qReport.plotLineDF(allInv, y = ['gain%'], labels= ['gain %'], divLine=0.0, colors=['blue'], height=2.5)
            
            qReport.addPageBreak()

            # report symbols
            profileData = self.__tickers.getProfile(ihData.keys(), update=update)
            for symbol in symbols:
                sData = sInv[symbol]
                # make symbol intro page
                if symbol in profileData:
                    name = profileData[symbol]['name']
                    columns = ['type', 'acronym', 'sector', 'industry', 'country', 'city', 'state', 'fundFamily', 'categoryName']
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
                else:
                    symbolLine = '%s: No profile data found' % symbol
                    qReport.addParagraph(symbolLine, qReport.getStyle('Heading1'))
                
                qReport.plotLineDF(sData, y = ['amount', 'value'], labels= ['payed', 'value'], colors=['green', 'blue'], height=5)

                qReport.addPageBreak()

                # make Gain and Price Page
                qReport.addParagraph(symbolLine, qReport.getStyle('Heading1'))

                qReport.addParagraph('%s: Gain and Price:' % symbol, qReport.getStyle('Heading2'))

                qReport.plotLineDF(sData, y = ['gain$'], labels= ['gain $'], colors=['blue'], height=5)
                qReport.plotLineDF(sData, y = ['gain%', 'priceClose%'], labels= ['gain %', 'price %'], divLine=0.0, colors=['blue', 'green'], height=5)
                
                qReport.addPageBreak()

                # make dividends page
                firstDate = ihData[symbol].index[0]
                divDateRange = pd.date_range(firstDate, nowDate).date
                sDivData = pd.DataFrame(index=divDateRange)
                if symbol in timeSeries and 'dividend' in timeSeries[symbol]:
                    sDivData = sDivData.join(timeSeries[symbol]['dividend'])
                # divColumns = ['divReinvShares', 'divReinvAmount', 'divAmount']
                divColumns = ['divReinvAmount', 'divAmount']
                divColumns = list(set(divColumns).intersection(set(ihData[symbol].columns)))
                divColumns.sort()
                if len(divColumns) > 0:
                    sDivData = sDivData.join(ihData[symbol][divColumns])
                sDivData.dropna(how='all', inplace=True)
                sDivData = sDivData.join(sData['shares'])
                # utils.printDataFrame(sDivData, 'dataframediv.txt', title='%s: %s' % (accountName,symbol), append=True)
                if sDivData.shape[0] > 0:
                    # utils.printDataFrame(sDivData, 'dataframediv.txt', title='%s: %s' % (accountName,symbol), append=True)
                    # print()
                    # print(sDivData.columns)
                    sDivData.rename(columns={'divAmount': 'divCashed', 'divReinvAmount': 'divReinv'}, inplace=True)
                    if 'dividend' in sDivData:
                        sDivData['divPosted'] = sDivData['dividend'] * sDivData['shares']
                    # utils.printDataFrame(sDivData, 'dataframediv.txt', title='%s: %s' % (accountName,symbol), append=True)
                    values = ['divPosted', 'divCashed','divReinv']
                    availValues = []
                    for value in values:
                        if value in sDivData.columns:
                            availValues.append(value)
                    if len(availValues):
                        barData = sDivData[availValues].dropna(how='all')
                        barColumns = barData.columns.to_list()
                        colColors = {
                            'divPosted': 'black',
                            'divReinv': 'blue',
                            'divCashed': 'green',
                        }
                        barColors = [colColors[x] for x in barColumns]
                        barData = barData.join(sDivData['shares'])
                        # utils.printDataFrame(barData, 'dataframediv.txt', title='%s: %s' % (accountName,symbol), append=True)
                        qReport.addParagraph('%s: Dividends:' % symbol, qReport.getStyle('Heading2'))
                        qReport.plotBarsLineDF(barData, ybars=barColumns, yline='shares',
                            yBarsLabel='amount $', barColors=barColors,
                            yLineLabel='shares', lineColor='orange', plotHeight = 5)
                
                        qReport.addPageBreak()

            qReport.buildDoc()

    def makeReportsOld(self, accountNames=[], update=False):
        if len(accountNames) == 0:
            accountNames = self.getAccountNames()
        else:
            accountNames = set(accountNames).intersection((self.getAccountNames()))

        for accountName in accountNames:
            # if accountName != 'ETRADE_Trust': continue
            ihData = self.getInvestmentHistory(accountName, update=update)

            # find symbols with shares left and the very first investment date
            nowDate = datetime.now().date()
            firstDate = nowDate
            symbols = set()
            for symbol, sData in ihData.items():
                if sData.iloc[-1]['shares'] < 0.5: continue
                symbols.add(symbol)
                if sData.index[0] < firstDate: firstDate = sData.index[0]

            # no shares left, no need to continue
            if len(symbols) == 0: continue

            # get timeseries for symbols and spread them to full date range
            timeSeries = self.__tickers.getTimeSeries(symbols, update=update)
            
            # setup account data
            dateRange = pd.date_range(firstDate, datetime.now().date()).date
            dfAmounts = pd.DataFrame(index=dateRange)
            dfValues = pd.DataFrame(index=dateRange)
            # add additional data to ihData and collect sums for account data
            for symbol in symbols:
                # get timeseries for symbol investment data range
                if symbol in timeSeries:
                    sDateRange = pd.date_range(ihData[symbol].index[0], nowDate).date
                    ts = pd.DataFrame(index=sDateRange)
                    ts = ts.join(timeSeries[symbol]['close'])
                    ts = ts.infer_objects(copy=False).ffill()
                    ts = ts.infer_objects(copy=False).bfill()

                # add more info columns to ihData
                ihData[symbol]['priceBuy'] = ihData[symbol]['amount']/ihData[symbol]['shares']
                if symbol in timeSeries:
                    ihData[symbol] = ihData[symbol].join(ts)
                    ihData[symbol].rename(columns={'close': 'priceClose'}, inplace=True)
                    ihData[symbol]['value'] = ihData[symbol]['shares'] * ihData[symbol]['priceClose']
                else:
                    ihData[symbol]['priceClose'] = ihData[symbol]['priceBuy']
                    ihData[symbol]['value'] = ihData[symbol]['amount']
                ihData[symbol]['gain$'] = ihData[symbol]['value'] - ihData[symbol]['amount']
                ihData[symbol]['gain%'] = ((ihData[symbol]['value']/ihData[symbol]['amount'])* 100.0)-100.0
                ihData[symbol]['priceClose%'] = ((ihData[symbol]['priceClose']/ihData[symbol].iloc[0]['priceClose'])*100.0)-100.0

                # now we can add to totals for summing
                dfAmounts = dfAmounts.join(ihData[symbol]['amount'])
                dfAmounts.rename(columns={'amount': symbol}, inplace=True)
                dfValues = dfValues.join(ihData[symbol]['value'])
                dfValues.rename(columns={'value': symbol}, inplace=True)

            # sum all symbol values
            allInv = pd.DataFrame(index=dateRange)
            allInv['amount'] = dfAmounts.sum(axis=1)
            allInv['value'] = dfValues.sum(axis=1)
            allInv['gain$'] = allInv['value'] - allInv['amount']
            allInv['gain%'] = ((allInv['value']/allInv['amount'])* 100.0)-100.0

            lastDate = allInv.index[-1]
            totalPayed = allInv.at[lastDate, 'amount']
            
            reportName = accountName.replace(' ','_')
            qReport = Report(reportName)

            qReport.addParagraph(accountName+':', qReport.getStyle('Heading1'))

            # report holdings
            rows = []
            for symbol in symbols:
                sData = ihData[symbol]
                payed = sData.at[lastDate, 'amount']
                rowData = {'symbol': symbol,
                    'Dist %': (payed/totalPayed)*100.0,
                    'payed': payed,
                    'value': sData.at[lastDate, 'value'],
                    'gain $': sData.at[lastDate, 'gain$'],
                    'gain %': sData.at[lastDate, 'gain%']}
                rows.append(rowData)
            if len(rows) == 0:
                pp(rows)
                qReport.addPageBreak()
                continue
            dfHoldings = pd.DataFrame(rows)
            dfHoldings.sort_values(by='Dist %', inplace=True, ascending=False)
            totalRow = {'symbol': 'Total:',
                    'Dist %': dfHoldings['Dist %'].sum(),
                    'payed': allInv.at[lastDate, 'amount'],
                    'value': allInv.at[lastDate, 'value'],
                    'gain $': allInv.at[lastDate, 'gain$'],
                    'gain %': allInv.at[lastDate, 'gain%']}
            dfHoldings.loc[len(dfHoldings)] = totalRow
            qReport.addParagraph('Holdings as per %s:' % lastDate, qReport.getStyle('Heading2'))
            qReport.addTable(dfHoldings.round(4))

            qReport.plotLineDF(allInv, y = ['amount', 'value'], labels= ['payed', 'value'], colors=['green', 'blue'], height=2.5)
            qReport.plotLineDF(allInv, y = ['gain$'], labels= ['gain $'], divLine=allInv.iloc[0]['gain$'], colors=['blue'], height=2.5)
            qReport.plotLineDF(allInv, y = ['gain%'], labels= ['gain %'], divLine=0.0, colors=['blue'], height=2.5)
            
            qReport.addPageBreak()

            # report symbols
            profileData = self.__tickers.getProfile(ihData.keys(), update=update)
            ehData = self.getEventHistory(accountName, update=update)
            for symbol in symbols:
                if symbol != 'VMFXX': continue

                sData = ihData[symbol]
                # make symbol intro page
                if symbol in profileData:
                    name = profileData[symbol]['name']
                    columns = ['type', 'acronym', 'sector', 'industry', 'country', 'city', 'state', 'fundFamily', 'categoryName']
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
                else:
                    symbolLine = '%s: No profile data found' % symbol
                    qReport.addParagraph(symbolLine, qReport.getStyle('Heading1'))
                
                qReport.plotLineDF(sData, y = ['amount', 'value'], labels= ['payed', 'value'], colors=['green', 'blue'], height=5)

                qReport.addPageBreak()

                # make Gain and Price Page
                qReport.addParagraph(symbolLine, qReport.getStyle('Heading1'))

                qReport.addParagraph('Gain and Price:', qReport.getStyle('Heading2'))

                qReport.plotLineDF(sData, y = ['gain$'], labels= ['gain $'], colors=['blue'], height=5)
                qReport.plotLineDF(sData, y = ['gain%', 'priceClose%'], labels= ['gain %', 'price %'], divLine=0.0, colors=['blue', 'green'], height=5)
                
                qReport.addPageBreak()

                # make dividend and capital gain page
                # print(accountName, symbol)
                # print(self.getShareActions(accountName, symbol))


            qReport.buildDoc()


        



        
