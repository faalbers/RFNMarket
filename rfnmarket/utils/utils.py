# return data structure
def dataStructure(readData, writeData, keyValues):
    if isinstance(readData, dict):
        for key, keyData in readData.items():
            if key in keyValues:
                key = 'SYMBOL'
            elif isinstance(key, int):
                key = 'TIMESTAMP'
            if isinstance(keyData, dict):
                if not key in writeData:
                    writeData[key] = {}
                dataStructure(keyData, writeData[key], keyValues)
            elif isinstance(keyData, list):
                if not key in writeData:
                    writeData[key] = []
                dataStructure(keyData, writeData[key], keyValues)
            else:
                writeData[key] = keyData
    if isinstance(readData, list):
        for listData in readData:
            if len(writeData) == 0:
                if isinstance(listData, dict):
                    entity = {}
                    writeData.append(entity)
                    dataStructure(listData, entity, keyValues)
                elif isinstance(listData, list):
                    entity = []
                    writeData.append(entity)
                    dataStructure(listData, entity, keyValues)
                else:
                    writeData.append(listData)
            break

# print data hierachy
def printHierachy(data, f, level):
    tab = '    '
    if isinstance(data, dict):
        f.write('%s{\n' % (tab*level))
        for key, keyData in data.items():
            f.write("%s'%s':\n" % (tab*(level+1), key))
            printHierachy(keyData, f, level+1)
        f.write('%s}\n' % (tab*level))
    elif isinstance(data, list):
        f.write('%s[\n' % (tab*level))
        for listData in data:
            printHierachy(listData, f, level+1)
        f.write('%s]\n' % (tab*level))
    else:
        if isinstance(data, str):
            f.write("%s'%s'\n" % (tab*(level+1), data))
        else:
            f.write("%s%s\n" % (tab*(level+1), data))

