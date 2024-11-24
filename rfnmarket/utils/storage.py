import os, pickle
import shutil
from datetime import datetime
from . import log

def get(name):
    fileName = name+'.pickle'
    if not os.path.exists(fileName):
        # log.error('storage.get: File not found: %s' % fileName)
        return None
    with open(fileName, 'rb') as f:
        data = pickle.load(f)
    f.close()
    # log.info('storage retrieved: %s' % fileName)
    return data

def save(name, data):
    fileName = name+'.pickle'
    with open(fileName, 'wb') as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
    f.close()
    # log.info('storage saved: %s' % fileName)

def backup(name):
    fileName = name+'.pickle'
    now = datetime.fromtimestamp(int(datetime.now().timestamp()))
    backupFileName = '%s_%s_%s.pickle' % (name, now.date(), str(now.time()).replace(':', '-'))
    if not os.path.exists(fileName):
        log.error('storage.backup: File not found: %s' % fileName)
        return
    shutil.copyfile(fileName, backupFileName)
    # log.info('storage backup: %s -> %s' % (fileName, backupFileName))
