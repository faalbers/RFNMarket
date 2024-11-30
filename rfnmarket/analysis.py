from . import Market
import pandas as pd
from .utils import GICS, utils
from pprint import pp


class Analysis():
    def __init__(self):
        self.tickers = Market()

    def test(self):
        marketCaps = ['Small Cap', 'Mid Cap']
        sectors = ['Information Technology']
        profile = self.tickers.getProfile()
        dfProfile = pd.DataFrame(profile).T

        # get us esxchange symbols only for now
        dfProfile = dfProfile[dfProfile['exchangeCountry'] == 'United States']

        # get the ones in cap range
        dfProfile = dfProfile[dfProfile['marketCapCategory'].isin(marketCaps)]

        # get the needed sectors
        # dfProfile.dropna(subset = ['sector'], inplace=True)
        dfProfile = dfProfile[dfProfile['sector'].isin(sectors)]

        # dfTrailingPE = df.dropna(subset = ['trailingPE'])
        # dfTrailingPE = dfTrailingPE[dfTrailingPE['trailingPE'] != 'Infinity']
        # print(dfTrailingPE['trailingPE'].min())
        # print(dfTrailingPE['trailingPE'].max())
        
        # dfForwardPE = df.dropna(subset = ['forwardPE'])
        # print(dfForwardPE['forwardPE'].min())
        # print(dfForwardPE['forwardPE'].max())
        
        # gics = GICS()
        # pp(gics.getNames('industryGroup'))
        
        statistics = self.tickers.getStatistics()
        dfStatistics = pd.DataFrame(statistics).T
        dfStatistics = dfStatistics[dfStatistics.index.isin(dfProfile.index.to_list())]
        print(dfStatistics)
        # allData = {}
        # utils.dataStructure(statistics, allData, set(symbols))
        # with open('alldata.txt', 'w', encoding="utf-8") as f:
        #     utils.printHierachy(allData, 'statistics.txt')




