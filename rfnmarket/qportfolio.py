from . import Tickers
from .utils import QIF
import pandas as pd
from pprint import pp
from datetime import datetime
import matplotlib.pyplot as plt

# portfolio based on Quicken QIF file
class QPortfolio():
    __sharesIn = ['Buy', 'ShrsIn', 'ReinvDiv', 'ReinvLg', 'ReinvSh']
    __sharesOut = ['Sell', 'ShrsOut']

    def __init__(self, qifFile):
        self.__tickers = Tickers()
        qif = QIF(qifFile)
        qAccounts = qif.getAcounts(accType='Invst')
        qSecurities = qif.getSecurities()

        securities = pd.DataFrame(qSecurities)
        self.__securities = securities.copy()
        self.__securities.set_index('symbol', verify_integrity=True, inplace=True)

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
    
    def getShareActions(self, account):
        if not account in self.__accounts: return []
        dfShares = self.__accounts[account].dropna(subset = ['shares'])
        actions = list(set(dfShares['action'].to_list()))
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
    
    def getInvestmentHistory(self, account):
        nowDate = datetime.now().date()
        # gather actions and make share out actions negative
        accData = self.__accounts[account]
        sharesIn = accData[accData['action'].isin(self.__sharesIn)]
        sharesOut = accData[accData['action'].isin(self.__sharesOut)]
        sharesOut.loc[:,'shares'] = -sharesOut['shares']
        sharesOut.loc[:,'amount'] = -sharesOut['amount']
        shares = pd.concat([sharesIn, sharesOut], verify_integrity=True)[['date', 'secSymbol', 'shares', 'amount']]
        shares.sort_index(inplace=True)
        shares.set_index('date', inplace=True)

        # find symbols and iterate them:
        symbols = set(shares['secSymbol'].to_list())

        # get timeseries for symbols
        timeSeries = self.__tickers.getTimeSeries(symbols, update=True)

        # gather data per symbol we have timeseries for
        investmentHistory = {'symbols': {}}
        firstDay = nowDate
        for symbol, tsData in timeSeries.items():
            # if symbol != 'ANET': continue
            # get symbols shares data
            sShares = shares[shares['secSymbol'] == symbol][['shares', 'amount']]

            # only handle if shares are left
            if sShares['shares'].sum() < 0.5: continue

            # create date range for data creation
            if sShares.index[0] < firstDay:
                firstDay = sShares.index[0]
            fullDateRange = pd.date_range(sShares.index[0], nowDate).date

            # get close prices
            ts = pd.DataFrame(index=fullDateRange)
            ts = ts.join(tsData['close'])

            ts = ts.infer_objects(copy=False).ffill()
            ts = ts.infer_objects(copy=False).bfill()

            # fill amount values in nan spots
            if sShares['amount'].isna().any():
                sShares = sShares.join(ts)
                sShares['amount'] = sShares['amount'].fillna(sShares['shares']*sShares['close'])
                sShares.drop('close', axis=1, inplace=True)

            # group and sum dates so we dont have duplicates
            sShares = sShares.groupby(sShares.index).sum()

            # create collection dataframes
            dfSharesSymbol = pd.DataFrame(index=fullDateRange)
            dfAmountsSymbol = pd.DataFrame(index=fullDateRange)

            invCount = 0
            for date, row in sShares.iterrows():
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
            symbolInv = pd.DataFrame(index=fullDateRange)
            symbolInv['shares'] = dfSharesSymbol.sum(axis=1)
            symbolInv = symbolInv.join(ts)
            symbolInv.rename(columns={'close': 'price'}, inplace=True)
            symbolInv['value'] = symbolInv.prod(axis=1)
            symbolInv['amount'] = dfAmountsSymbol.sum(axis=1)
            symbolInv['gain$'] = symbolInv['value'] - symbolInv['amount']
            symbolInv['gain%'] = ((symbolInv['value']/symbolInv['amount'])* 100.0)-100.0
            symbolInv['price%'] = ((symbolInv['price']/symbolInv.iloc[0]['price'])*100.0)-100.0

            investmentHistory['symbols'][symbol] = symbolInv
        dateRange = pd.date_range(firstDay, nowDate).date
        dfAmounts = pd.DataFrame(index=dateRange)
        dfValues = pd.DataFrame(index=dateRange)
        for amount, symbolInv in investmentHistory['symbols'].items():
            dfAmounts = dfAmounts.join(symbolInv['amount'])
            dfAmounts.rename(columns={'amount': amount}, inplace=True)
            dfValues = dfValues.join(symbolInv['value'])
            dfValues.rename(columns={'value': amount}, inplace=True)
        allInv = pd.DataFrame(index=dateRange)
        allInv['amount'] = dfAmounts.sum(axis=1)
        allInv['value'] = dfValues.sum(axis=1)
        allInv['gain$'] = allInv['value'] - allInv['amount']
        allInv['gain%'] = ((allInv['value']/allInv['amount'])* 100.0)-100.0
        investmentHistory['all'] = allInv

        return investmentHistory

    def test(self):
        if False:
            # just temporary for testing
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            pd.set_option('display.width', None)

        # accName = 'Fidelity_Rollover_Amy'
        # accName = 'FIDELITY_ Frank Roth'
        # accName = 'ETRADE_Frank Rollover'
        accName = 'ETRADE_Trust'
        # accData = self.__accounts[accName]
        # pp(accData[['action', 'shares']].dropna(subset = ['shares']))

        # symbols = self.getAccountSecuritySymbols(account)
        # pp(symbols)
        # timeSeries = self.__tickers.getTimeSeries(symbols, update=True)

        # for accName in self.getAccountNames():
        #     pp(self.getInvestmentHistory(accName))
        #     for symbol, incData in self.getInvestmentHistory(accName).items():
        #         incData.plot(y=['gain%', 'price%'], grid=True, title='%s: %s' % (symbol, accName))
        
        incData = self.getInvestmentHistory(accName)
        for symbol, sData in incData['symbols'].items():
            # pp(incData)
            # incData.plot(y=['amount', 'value'], grid=True, title='%s: %s' % ('ANET', accName))
            sData.plot(y=['gain%', 'price%'], grid=True, title='%s: %s' % (symbol, accName))
            # incData.plot(y=['gain%'], grid=True, title='%s: %s' % ('ANET', accName))
            # incData.plot(y=['price%'], grid=True, title='%s: %s' % ('ANET', accName))
        incData['all'].plot(y=['gain$'], grid=True, title='%s' % accName)

        plt.show()

        



        
