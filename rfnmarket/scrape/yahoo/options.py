from ...utils import log, database
from .base import Base
from pprint import pp
from datetime import datetime
import json, copy
from . import const
import pandas as pd
import numpy as np

# https://yahooquery.dpguthrie.com/guide/ticker/modules/

class Options(Base):
    dbName = 'yahoo_options'

    @staticmethod
    def getTableNames(tableName):
        if tableName == 'all':
            [tableName]
        return [tableName]    

    def __init__(self, symbols=[], tables=[], forceUpdate=False):
        super().__init__()
        self.db = database.Database(self.dbName)

        # not needed yet
        
        # update procs need these
        self.symbols = [] # accessed by index
        requestArgsList = []
        for symbol in symbols:
            requestArgs = {
                'url': 'https://query2.finance.yahoo.com/v7/finance/options/'+symbol.upper(),
                'timeout': 30,
            }
            requestArgsList.append(requestArgs)
            self.symbols.append(symbol)
        self.multiRequest(requestArgsList, blockSize=100)
    
    def pushAPIData(self, symbolIndex, response):
        symbol = self.symbols[symbolIndex]
        if response.headers.get('content-type').startswith('application/json'):
            symbolData = response.json()
            if 'optionChain' in symbolData:
                # handle API response
                symbolData = symbolData['optionChain']
                if symbolData['error'] != None:
                    # handle error response
                    symbolData = symbolData['error']
                elif symbolData['result'] != None:
                    # handle data return response
                    symbolData = symbolData['result'][0]
                    with open('dataout.txt' , 'w') as f:
                        pp(symbolData, f)
                    # print(len(symbolData['options'][0]))
                    # print(len(symbolData['expirationDates']))
                    # print(len(symbolData['strikes']))
                    oCount = 0
                    for option in symbolData['options']:
                        print('Option: 0')
                        print('expirationDate: %s' % datetime.fromtimestamp(option['expirationDate']))
                        for call in option['calls']:
                            print('strike call: %s' % call['strike'])
                        for put in option['puts']:
                            print('strike put : %s' % put['strike'])
                        oCount += 1

        # # update status
        # status = {}
        # for module in self.symbolModules[symbol]:
        #     status[module] = int(datetime.now().timestamp())
        # self.db.tableWrite('status_db', {symbol: status}, 'keySymbol', method='update')
    
    def dbCommit(self):
        # call from base to commit
        self.db.commit()
        
