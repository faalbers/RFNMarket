import glob, os
import pandas as pd
import sqlite3
from pathlib import Path
from ...utils import log, database
from datetime import datetime
from pprint import pp

class Saved():
    def __init__(self):
        self.dataPath = 'database/'
        self.getCSV()

    def getCSV(self):
        cvsFiles = glob.glob(self.dataPath+'*.csv')
        db = database.Database('saved')
        # get status first
        fileDates , dataNames = db.findRows('status_db', ['rowid'], [1])
        statusDates = {}
        index = 0
        for dataName in dataNames:
            statusDates[dataName] = fileDates[0][index]
            index += 1
        # now lets retrieve the data if needed
        connection = db.getConnection()
        filesRead = 0
        for cvsFile in cvsFiles:
            fileDate = int(os.path.getmtime(cvsFile))
            dataName = Path(cvsFile).stem
            
            # only read data from newer files
            if dataName in statusDates and fileDate >= statusDates[dataName]: continue
            
            # read file into database
            data = pd.read_csv(cvsFile)
            data.to_sql(dataName, con=connection, index=False, if_exists='replace')
            filesRead += 1

            # update status
            db.addTable('status_db', ["'%s' TIMESTAMP" % dataName])
            db.addColumnIfNotExists('status_db', dataName, 'TIMESTAMP')
            db.insertOrIgnore('status_db', ['rowid'], (1,))
            db.update( 'status_db', 'rowid', 1, [dataName], (fileDate,) )

        if filesRead > 0:
            log.info('CSV files read and updated: %s' % filesRead)
