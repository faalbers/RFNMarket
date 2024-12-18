import pandas as pd
from pprint import pp
from ..utils import database, log
from .. import scrape
from datetime import datetime
import copy
from .catalog import Catalog

class Data():
    def __init__(self, log_level=None):
        if log_level != None:
            log.initLogger(logLevel=log_level)
        self.catalog = Catalog()
        self.databases = {}
    
    def getScrapeDB(self, scrapeClass):
        if not scrapeClass in self.databases:
            self.databases[scrapeClass] = database.Database(scrapeClass.dbName)
        return self.databases[scrapeClass]

    def closeScrapeDB(self, scrapeClass):
        if scrapeClass in self.databases:
            self.databases.pop(scrapeClass)
    
    def closeAllScrapeDB(self):
        scrapeClasses = list(self.databases.keys())
        for scrapeClass in scrapeClasses:
            self.closeScrapeDB(scrapeClass)

    def update(self, catalogs=[], keyValues=[], forceUpdate=False):
        # gather scrape classes and needed tables
        scraperClasses = []
        for catalog in catalogs:
            catData =  self.catalog.getCatalog(catalog)
            if len(catData) > 0:
                for setName, dfData in catData['sets'].items():
                    for scraperClass, scraperData in dfData['scrapes'].items():
                        scraperClasses.append((scraperClass, list(scraperData.keys())))

        updateScrapers = {}
        for ssData in scraperClasses:
            scraperClass = ssData[0]
            if not scraperClass in updateScrapers:
                updateScrapers[scraperClass] = []
            for tableName in ssData[1]:
                updateScrapers[scraperClass] += scraperClass.getTableNames(tableName)
        
        # update scrapers with tables
        for scraperClass, tableNames in updateScrapers.items():
            # print(scraperClass)
            # print(tableNames)
            scraperClass(keyValues, tables=list(set(tableNames)), forceUpdate=forceUpdate)
    
    def getData(self, catalogs=[], keyValues=[], update=False, forceUpdate=False, catalogDB=None):
        # make sure all keyValues are capitalized
        keyValues = [x.upper() for x in keyValues]
        
        if update or forceUpdate: self.update(catalogs, keyValues, forceUpdate=forceUpdate)

        mainData = {}
        for catalog in catalogs:
            if catalogDB != None:
                catData =  catalogDB[catalog]
            else:
                catData =  self.catalog.getCatalog(catalog)
            
            # get data sets
            setsData = {}
            for setName , setData in catData['sets'].items():
                tablesData = {}
                for scrapeClass, scrapeData in setData['scrapes'].items():
                    # access scrape database
                    db = self.getScrapeDB(scrapeClass)
                    for tableName, tableData in scrapeData.items():
                        scrapeTableNames = scrapeClass.getTableNames(tableName)
                        handleKeyValues = tableData['keyValues']
                        for tableName in scrapeTableNames:
                            columns = {}
                            for columnSet in tableData['columnSettings']:
                                searchColumn = columnSet[0]
                                makeColumn = columnSet[1]
                                if searchColumn == 'all':
                                    for columnName in db.getTableColumnNames(tableName):
                                        if makeColumn != '':
                                            newColumnName = makeColumn + columnName.capitalize()
                                        else:
                                            newColumnName = columnName
                                        if not columnName in columns:
                                            columns[columnName] = {}
                                        columns[columnName]['newName'] = newColumnName
                                        columns[columnName]['settings'] = columnSet[2:]
                                else:
                                    if not searchColumn in columns:
                                        columns[searchColumn] = {}
                                    columns[searchColumn]['newName'] = makeColumn
                                    columns[searchColumn]['settings'] = columnSet[2:]
                            
                            # get table data
                            foundData = db.tableRead(tableName, keyValues, list(columns.keys()), handleKeyValues=handleKeyValues)
                            # skip if no data found
                            if len(foundData) == 0: continue

                            # make data
                            if handleKeyValues:
                                makeData = {}
                                for keyValue, keyData in foundData.items():
                                    newKeyData = {}
                                    for searchColumn, columnSettings in columns.items():
                                        if not searchColumn in keyData: continue
                                        newKeyData[columnSettings['newName']] = keyData[searchColumn]
                                    if len(newKeyData) > 0:
                                        makeData[keyValue] = newKeyData
                            else:
                                makeData = []
                                for rowData in foundData:
                                    newRowData = {}
                                    for searchColumn, columnSettings in columns.items():
                                        if not searchColumn in rowData: continue
                                        newRowData[columnSettings['newName']] = rowData[searchColumn]
                                    if len(newRowData) > 0:
                                        makeData.append(newRowData)
                            
                            if len(makeData) > 0:
                                tablesData[tableName] = makeData
                # run sets post procs
                if 'postProcs' in setData:
                    for procEntry in setData['postProcs']:
                        proc = procEntry[0]
                        procParams = procEntry[1]
                        setsData[setName] = proc(self, tablesData, **procParams)
                else:
                    setsData[setName] = tablesData
            
            # run setcatalogs post procs
            if 'postProcs' in catData:
                for procEntry in catData['postProcs']:
                    proc = procEntry[0]
                    procParams = procEntry[1]
                    mainData[catalog] = proc(self, setsData, **procParams)
            else:
                mainData[catalog] = setsData
                    
        self.closeAllScrapeDB()
        return mainData
    
    # def getCatalog(self):
    #     catalog = {}
    #     for cat, data in self.__catalog.items():
    #         catalog[cat] = data['info']
    #     return catalog
    

