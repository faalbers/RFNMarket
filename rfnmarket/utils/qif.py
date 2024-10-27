from pprint import pp
from datetime import datetime

class QIF():
    @staticmethod
    def __getEntries(entries, entryType):
        dataTypesLeft = set()
        transactions = []
        for entry in entries:
            dataTypes = set(entry.keys())
            transaction = {}
            # get date
            date = entry['D'][0].split('/')
            month = int(date[0].strip())
            date = date[1].split("'")
            day = int(date[0].strip())
            year = int(date[1].strip())+2000
            transaction['date'] = datetime(day=day, month=month, year=year).date()
            dataTypes.remove('D')

            # get values
            if 'U' in entry:
                transaction['amount'] = float(entry['U'][0].replace(',',''))
                dataTypes.remove('U')
            if 'N' in entry:
                if entryType == 'Invst':
                    transaction['action'] = entry['N'][0]
                dataTypes.remove('N')
            if 'P' in entry:
                if entryType == 'Invst':
                    transaction['description'] = entry['P'][0]
                dataTypes.remove('P')
            if 'L' in entry:
                if entryType == 'Invst':
                    transaction['transferAccount'] = entry['L'][0]
                dataTypes.remove('L')
            if 'M' in entry:
                if entryType == 'Invst':
                    transaction['memo'] = entry['M'][0]
                dataTypes.remove('M')
            if 'Y' in entry:
                if entryType == 'Invst':
                    transaction['security'] = entry['Y'][0]
                dataTypes.remove('Y')
            if 'I' in entry:
                if entryType == 'Invst':
                    transaction['price'] = float(entry['I'][0].replace(',',''))
                dataTypes.remove('I')
            if 'Q' in entry:
                if entryType == 'Invst':
                    transaction['shares'] = float(entry['Q'][0].replace(',',''))
                dataTypes.remove('Q')
            if 'O' in entry:
                if entryType == 'Invst':
                    transaction['commission'] = float(entry['O'][0].replace(',',''))
                dataTypes.remove('O')
            dataTypesLeft = dataTypesLeft.union(dataTypes)
            transactions.append(transaction)
        # print(dataTypesLeft)
        return transactions
    
    def __init__(self, fileName):
        line = '*'
        sections = []
        section = {}
        entry = {}
        with open(fileName, 'r') as f:
            while line:
                if line.startswith('!'):
                    if len(section) > 0:
                        sections.append(section)
                    section = {'header': line.strip('\n')[1:].strip(), 'entries': []}
                    entry = {}
                elif line == '^\n':
                    if len(entry) > 0:
                        section['entries'].append(entry)
                    entry = {}
                elif line != '*':
                    cleanLine = line.strip('\n').strip()
                    key = cleanLine[0]
                    value = cleanLine[1:]
                    if not key in entry:
                        entry[key] = []
                    entry[key].append(value)

                line = f.readline()
        # with open('qifSections.txt', 'w') as f:
        #     pp(sections, f)
        # return
        headers = {}
        autoSwitch = False
        autoSwitchCount = 0
        self.accounts = []
        account = {}
        self.securities = []
        for section in sections:
            # keep track of headers for now
            if not section['header'] in headers:
                headers[section['header']] = 0
            headers[section['header']] += 1

            if section['header'] == 'Option:AutoSwitch':
                autoSwitch = True
                autoSwitchCount += 1
            elif section['header'] == 'Clear:AutoSwitch':
                autoSwitch = False
            elif section['header'] == 'Type:Security':
                entry = section['entries'][0]
                security = {}
                security['name'] = entry['N'][0]
                security['symbol'] = entry['S'][0]
                security['type'] = entry['T'][0]
                self.securities.append(security)
            elif section['header'] == 'Account':
                if autoSwitchCount == 1:
                    pass
                if autoSwitchCount == 2:
                    entry = section['entries'][0]
                    account['name'] = entry['N'][0]
                    account['type'] = entry['T'][0]
                    if 'D' in entry:
                        account['description'] = entry['D'][0]
            elif section['header'] == 'Type:Bank':
                accType = 'Bank'
                if account['type'] == accType:
                    self.accounts.append(account)
                    account = {}
            elif section['header'] == 'Type:CCard':
                accType = 'CCard'
                if account['type'] == accType:
                    self.accounts.append(account)
                    account = {}
            elif section['header'] == 'Type:Cash':
                accType = 'Cash'
                if account['type'] == accType:
                    self.accounts.append(account)
                    account = {}
            elif section['header'] == 'Type:Oth A':
                accType = 'Oth A'
                if account['type'] == accType:
                    self.accounts.append(account)
                    account = {}
            elif section['header'] == 'Type:Oth L':
                accType = 'Oth L'
                if account['type'] == accType:
                    self.accounts.append(account)
                    account = {}
            elif section['header'] == 'Type:Invst':
                accType = 'Invst'
                if account['type'] == accType:
                    # print('transactions: Invst: %s' % account['name'])
                    account['transactions'] = QIF.__getEntries(section['entries'], accType)
                    self.accounts.append(account)
                    account = {}
        
        # for account in self.accounts:
        #     if account['name'] == 'FIDELITY_ Frank Roth':
        #         pp(account)
        # pp(self.securities)

    def getAcounts(self, accType=None):
        if accType == None:
            return self.accounts
        else:
            accounts = []
            for account in self.accounts:
                if account['type'] == accType:
                    accounts.append(account)
            return accounts

    def getSecurities(self):
        return self.securities

