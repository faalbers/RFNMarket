from ...utils import log, database
from ratelimit import limits, sleep_and_retry
from finvizfinance.quote import finvizfinance
import pandas as pd
from pprint import pp
import math

class Ticker_News():
    dbName = 'finviz_ticker_news'

    @staticmethod
    def getTableNames(tableName):
        if tableName == 'all':
            return ['table_reference']
        return [tableName]

    @sleep_and_retry
    @limits(calls=3, period=1)
    def __get_news(self, symbol):
        try:
            ticker = finvizfinance(symbol)
            news = ticker.ticker_news()
        except Exception as e:
            # log.info(f"An error occurred: {e}")
            return pd.DataFrame()
        return news

    def __init__(self, symbols=[], tables=[], forceUpdate=False):
        super().__init__()

        # setup database
        self.db = database.Database(self.dbName)

        # lets backup the database first
        self.db.backup()

        log.info('Ticker News update')
        log.info('symbols processing: %s' % len(symbols))

        symbols_done = 0
        symbols_found = 0
        news_found = 0
        symbols_not_found = set()
        for symbol in symbols:
            # get news and make sure timestamps are unique
            if (symbols_done % 100) == 0:
                # if len(symbols_not_found) > 0:
                #     log.info('symbols not found: %s' % list(symbols_not_found))
                symbols_not_found = set()
                log.info('symbols found so far: %s' % symbols_found)
                log.info('symbols to do       : %s' % (len(symbols) - symbols_done))
            
            news_df = self.__get_news(symbol)
            if len(news_df) == 0:
                symbols_not_found.add(symbol)
                symbols_done += 1
                continue
            news_df.sort_values(by='Date', inplace=True)
            news_df.reset_index(drop=True, inplace=True)
            news_df['Date'] = news_df['Date'].apply(lambda x: int(x.timestamp()))
            if not news_df['Date'].is_unique:
                grouped_df = news_df.groupby('Date')
                for timestamp, group in grouped_df.groups.items():
                    if group.shape[0] > 1:
                        for index in group[1:]:
                            timestamp += 1
                            news_df.loc[index, 'Date'] = timestamp
            news_df.set_index('Date', inplace=True)

            if len(news_df) > 0:
                symbols_found += 1
                news_found += len(news_df)
                # make unique table name
                tableName = 'news_'
                for c in symbol:
                    if c.isalnum():
                        tableName += c
                    else:
                        tableName += '_'
                
                # write ticker news table and update table reference
                self.db.tableWrite(tableName, news_df.T.to_dict(), 'timestamp', method='append')
                self.db.tableWrite('table_reference', {symbol: {'news': tableName}}, 'keySymbol', method='append')
            
            symbols_done += 1

        # if len(symbols_not_found) > 0:
        #     log.info('symbols not found: %s' % list(symbols_not_found))
        log.info('total symbols found      : %s' % symbols_found)
        log.info('total news articles found: %s' % news_found)


