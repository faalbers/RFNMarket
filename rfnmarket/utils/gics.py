from gics import GICS as GICSP
from pprint import pp

# https://www.msci.com/our-solutions/indexes/gics
# https://www.spglobal.com/marketintelligence/en/documents/112727-gics-mapbook_2018_v3_letter_digitalspreads.pdf

class SubIndustry():
    def __init__(self, name, data):
        self.name = name
        self.__data = data

class Industry():
    def __init__(self, name, data):
        self.name = name
        self.__data = data
    
    def getSubIndustry(self, subIndustry):
        if subIndustry in self.__data['subIndustries']:
            return SubIndustry(subIndustry, self.__data['subIndustries'][subIndustry])
        return None

    def getSubIndustryNames(self):
        subIndustryNames =  list(self.__data['subIndustries'].keys())
        subIndustryNames.sort()
        return subIndustryNames

class IndustryGroup():
    def __init__(self, name, data):
        self.name = name
        self.__data = data
    
    def getIndustry(self, industry):
        if industry in self.__data['industries']:
            return Industry(industry, self.__data['industries'][industry])
        return None

    def getIndustryNames(self):
        industryNames =  list(self.__data['industries'].keys())
        industryNames.sort()
        return industryNames

class Sector():
    def __init__(self, name, data):
        self.name = name
        self.__data = data

    def getIndustryGroup(self, industryGroup):
        if industryGroup in self.__data['industryGroups']:
            return IndustryGroup(industryGroup, self.__data['industryGroups'][industryGroup])
        return None

    def getIndustryGroupNames(self):
        industryGroupNames =  list(self.__data['industryGroups'].keys())
        industryGroupNames.sort()
        return industryGroupNames
    
class GICS():
    @staticmethod
    def renameSector(name):
        rename = {
            'Basic Materials': 'Materials',
            'Consumer Defensive': 'Consumer Staples',
            'Consumer Cyclical': 'Consumer Discretionary',
            'Technology': 'Information Technology',
            'Healthcare': 'Health Care',
        }
        if name in rename:
            return rename[name]
        return name
    
    @staticmethod
    def renameIndustryGroup(name):
        rename = {
            'Telecom Services': 'Telecommunication Services',
            'Power Generation': 'Utilities',
            'Real Estate Investment Trusts': 'Equity Real Estate Investment Trusts (REITs)',
            'Plastic Products': 'Materials',
        }
        if name in rename:
            return rename[name]
        return name
    
    @staticmethod
    def renameIndustry(name):
        rename = {
            'Packaging & Containers': 'Containers & Packaging',
            'Infrastructure Operations': 'IT Services',
            'Major Chemicals': 'Chemicals',
            'Major Banks': 'Banks',
            'Finance Companies': 'Financial Services',
        }
        if name in rename:
            return rename[name]
        return name
    
    @staticmethod
    def renameSubIndustry(name):
        rename = {
            'Agricultural Inputs': 'Agricultural Products & Services',
            'Airports & Air Services': 'Airport Services',
            
            'Auto Manufacturers': 'Automobile Manufacturers',
            'Auto Manufacturing': 'Automobile Manufacturers',
            'Motor Vehicles': 'Automobile Manufacturers',
            'Auto & Truck Dealerships': 'Automotive Retail',
            'Automotive Aftermarket': 'Automotive Retail',
            'Auto Parts': 'Automotive Parts & Equipment ',
            'Auto Parts:O.E.M.': 'Automotive Parts & Equipment ',

            'Banks - Regional': 'Regional Banks',
            'Commercial Banks': 'Regional Banks',
            'Banks - Diversified': 'Diversified Banks',

            'Beverages - Brewers': 'Brewers',
            'Beverages - Non-Alcoholic': 'Soft Drinks & Non-alcoholic Beverages',
            'Beverages - Wineries & Distilleries': 'Distillers & Vintners',
            'Beverages (Production/Distribution)': 'Soft Drinks & Non-alcoholic Beverages',
            
            'Building Materials': 'Building Products',
            'Business Equipment & Supplies': 'Office Services & Supplies',
            'Computer Hardware': 'Technology Hardware, Storage & Peripherals',
            'Confectioners': 'Food Retail',
            'Consulting Services': 'Research & Consulting Services',
            'Credit Services': 'Consumer Finance',
            'Department Stores': 'Consumer Staples Merchandise Retail ',
            'Department/Specialty Retail Stores': 'Consumer Staples Merchandise Retail ',
            'Discount Stores': 'Other Specialty Retail',
            'Other Specialty Stores': 'Other Specialty Retail',
            'Electronic Gaming & Multimedia': 'Consumer Electronics',
            'Consumer Electronics/Appliances': 'Consumer Electronics',
            'Electronics & Computer Distribution': 'Technology Distributors',
            'Farm & Heavy Construction Machinery': 'Agricultural & Farm Machinery',
            'Grocery Stores': 'Food Retail',
            'Food Distribution': 'Food Distributors',
            'Healthcare Plans': 'Health Care  Services',
            'Health Information Services': 'Health Care  Services',
            'Lodging': 'Hotels, Resorts & Cruise Lines',
            'Leisure': 'Leisure Facilities',
            'Financial Data & Stock Exchanges': 'Financial Exchanges & Data',
            'Footwear & Accessories': 'Footwear',
            'Furnishings, Fixtures & Appliances': 'Home Furnishings',
            'Household & Personal Products': 'Household Products',
            'Independent Oil & Gas': 'Independent Power Producers & Energy Traders',
            'Industrial Distribution': 'Trading Companies & Distributors',
            'Uranium': 'Diversified Metals & Mining',
            'Tools & Accessories': 'Household Appliances',
            'Staffing & Employment Services': 'Human Resource & Employment Services',
            'Solar': 'Renewable Electricity ',
            'Resorts & Casinos': 'Hotels, Resorts & Cruise Lines',
            'Railroads': 'Rail Transportation',
            'Pharmaceutical Retailers': 'Drug Retail',
            'Paper & Paper Products': 'Paper Products',
            'Other Precious Metals & Mining': 'Precious Metals & Minerals',
            'Precious Metals': 'Precious Metals & Minerals',
            'Residential Construction': 'Construction & Engineering',
            'Security & Protection Services': 'Security & Alarm Services',
            'Semiconductor Equipment & Materials': 'Semiconductor Materials & Equipment',
            'Marine Shipping': 'Marine Transportation',
            'Internet Content & Information': 'Internet Services & Infrastructure',
            'Internet Retail': 'Computer &Electronics Retail',
            'Metal Fabrication': 'Steel',
            'Metal Fabrications': 'Steel',
            'Software - Infrastructure': 'Internet Services & Infrastructure',
            'Lumber & Wood Production': 'Forest Products',
            'Luxury Goods': 'Apparel, Accessories & Luxury Goods',
            'Integrated Freight & Logistics': 'Air Freight & Logistics',
            'Trucking': 'Automobile Manufacturers',
            'Recreational Vehicles': 'Automobile Manufacturers',
            'Personal Services': 'Specialized Consumer Services',
            'Consumer Specialties': 'Housewares & Specialties',
            'Finance: Consumer Services': 'Consumer Finance',
            'Finance/Investors Services': 'Investment Banking & Brokerage',
            'Mining & Quarrying of Nonmetallic Minerals (No Fuels)': 'Precious Metals & Minerals',
            'Telecommunications Equipment': 'Communications Equipment',
            'Specialty Insurers': 'Multi-line Insurance',
            'Other Consumer Services': 'Specialized Consumer Services',
            'Misc Health and Biotechnology Services': 'Life Sciences Tools & Services',
            'Medical/Nursing Services': 'Health Care Facilities',
            'Medical/Dental Instruments': 'Health Care Equipment',
            'Life Insurance': 'Life & Health Insurance',
            'Investment Bankers/Brokers/Service': 'Investment Banking & Brokerage',
            'Investment Managers': 'Asset Management & Custody Banks',
            'Industrial Machinery/Components': 'Industrial Machinery & Supplies & Components',
            'Property-Casualty Insurers': 'Property & Casualty Insurance',
            'EDP Services': 'Data Processing & Outsourced Services ',
            'Computer Communications Equipment': 'Communications Equipment',
            'Cable & Other Pay Television Services': 'Cable &Satellite',
            'Catalog/Specialty Distribution': 'Other Specialty Retail',
            'Newspapers/Magazines': 'Publishing',
            'Electrical Products': 'Electrical Components & Equipment',
            'Specialty Foods': 'Food Distributors',
            'Blank Checks': 'Paper Products',

            'Other Industrial Metals & Mining': 'Diversified Metals & Mining',
            'Asset Management': 'Asset Management & Custody Banks',
            'Conglomerates': 'Industrial Conglomerates',
            'Diagnostics & Research': 'Health Care Technology',
            'Engineering & Construction': 'Construction & Engineering',
            'Information Technology Services': 'IT Consulting & Other Services',
            'Education & Training Services': 'Education Services',
            'Farm Products': 'Agricultural Products & Services',
            'Airlines': 'Passenger Airlines',
            'Mortgage Finance': 'Commercial & Residential Mortgage Finance ',
            'Rental & Leasing Services': 'Diversified Support Services',
            'Communication Equipment': 'Communications Equipment',
            'Building Products & Equipment': 'Building Products',
            'Electrical Equipment & Parts': 'Electrical Components & Equipment',
            'Textile Manufacturing': 'Textiles',
            'Specialty Industrial Machinery': 'Industrial Machinery & Supplies & Components',
            'Financial Conglomerates': 'Multi-Sector Holdings',
            'Apparel Manufacturing': 'Apparel, Accessories & Luxury Goods',
            'Advertising Agencies': 'Advertising',
            'Specialty Business Services': 'Diversified Support Services',
            'Travel Services': 'Hotels, Resorts & Cruise Lines',
            'Packaged Foods': 'Packaged Foods & Meats',
            'Hotels/Resorts': 'Hotels, Resorts & Cruise Lines',
            'Steel/Iron Ore': 'Steel',
            'Metal Mining': 'Steel',
            'Radio And Television Broadcasting And Communications Equipment': 'Communications Equipment',
            'Movies/Entertainment': 'Movies & Entertainment',
            'Accident &Health Insurance': 'Life & Health Insurance',

            'Pollution & Treatment Controls': 'Environmental & Facilities Services',
            'Waste Management': 'Environmental & Facilities Services',

            'Gambling': 'Casinos & Gaming',
            'Gambling': 'Casinos & Gaming',

            'Coking Coal': 'Coal & Consumable Fuels',
            'Thermal Coal': 'Coal & Consumable Fuels',
            'Coal Mining': 'Coal & Consumable Fuels',

            'Insurance - Life': 'Life & Health Insurance',
            'Insurance - Diversified': 'Multi-line Insurance',
            'Insurance - Property & Casualty': 'Property & Casualty Insurance',
            'Insurance - Reinsurance': 'Reinsurance',
            'Insurance - Specialty': 'Multi-line Insurance',

            'Drug Manufacturers - Specialty & Generic': 'Pharmaceuticals',
            'Drug Manufacturers - General': 'Pharmaceuticals',

            'Medical Devices': 'Health Care Equipment',
            'Medical Care Facilities': 'Health Care Facilities',
            'Medical Distribution': 'Health Care Distributors',
            'Medical Instruments & Supplies': 'Health Care Supplies',

            'Oil & Gas Integrated': 'Integrated Oil & Gas',
            'Oil & Gas E&P': 'Oil & Gas Exploration & Production',
            'Oil & Gas Midstream': 'Oil & Gas Storage & Transportation',
            'Oil & Gas Production': 'Oil & Gas Exploration & Production',
            
            'Real Estate - Development': 'Real Estate Development',
            'Real Estate - Diversified': 'Diversified Real Estate Activities',

            'Software - Application': 'Application Software',
            'Computer Software: Prepackaged Software': 'Application Software',

            'REIT - Diversified': 'Diversified REITs',
            'REIT - Mortgage': 'Mortgage REITs',
            'REIT - Healthcare Facilities': 'Health Care REITs',
            'REIT - Hotel & Motel': 'Hotel & Resort REITs',
            'REIT - Industrial': 'Industrial REITs',
            'REIT - Office': 'Office REITs',
            'REIT - Residential': 'Multi-Family Residential REITs',
            'REIT - Retail': 'Retail REITs',
            'REIT - Specialty': 'Other Specialized REITs',

            'Utilities - Diversified': 'Multi-Utilities',
            'Utilities - Regulated Electric': 'Electric Utilities',
            'Electric Utilities: Central': 'Electric Utilities',
            'Utilities - Regulated Gas': 'Gas Utilities',
            'Utilities - Regulated Water': 'Water Utilities',
            'Utilities - Renewable': 'Renewable Electricity ',
            'Utilities - Independent Power Producers': 'Independent Power Producers & Energy Traders',
            'Natural Gas Distribution': 'Gas Utilities',

            'Biotechnology: Biological Products (No Diagnostic Substances)': 'Biotechnology',
            'Biotechnology: Commercial Physical & Biological Resarch': 'Biotechnology',
            'Biotechnology: In Vitro & In Vivo Diagnostic Substances': 'Life Sciences Tools & Services',
            'Biotechnology: Pharmaceutical Preparations': 'Pharmaceuticals',
        }
        if name in rename:
            return rename[name]
        return name

    def __init__(self):
        self.definition = GICSP().definition
        codes = list(self.definition.keys())
        codes.sort()
        self.__gicsData = {}
        levelChildren = {0: 'industryGroups', 1: 'industries', 2: 'subIndustries'}
        for code in codes:
            levelCodes = []
            while len(code) > 0:
                levelCode = code[:2]
                if len(levelCode) > 0:
                    levelCodes.append(levelCode)
                code = code[2:]
            currentCode = ''
            gicsCurrent = self.__gicsData
            level = 0
            for levelCode in levelCodes:
                currentCode += levelCode
                codeData = self.definition[currentCode]
                levelName = codeData['name']
                if not levelName in gicsCurrent:
                    gicsCurrent[levelName] = {}
                gicsCurrent[levelName]['code'] = currentCode
                if 'description' in codeData:
                    gicsCurrent[levelName]['description'] = codeData['description']
                
                if level in levelChildren:
                    childrenName = levelChildren[level]
                    if not childrenName in gicsCurrent[levelName]:
                        gicsCurrent[levelName][childrenName] = {}
                    gicsCurrent = gicsCurrent[levelName][childrenName]
                
                level += 1
    
    def getSector(self, sector):
        if sector in self.__gicsData:
            return Sector(sector, self.__gicsData[sector])
        return None
    
    def getSectorNames(self):
        sectors = list(self.__gicsData.keys())
        sectors.sort()
        return sectors

    def getNames(self, type, sectors=[], industryGroups=[], industries=[], subIndustries=[]):
        names = {}
        for sector in self.getSectorNames():
            if len(sectors) > 0 and not sector in sectors: continue
            if type == 'sector':
                names[sector] = []
            else:
                sector = self.getSector(sector)
                for industryGroup in sector.getIndustryGroupNames():
                    if len(industryGroups) > 0 and not industryGroup in industryGroups: continue
                    if type == 'industryGroup':
                        names[industryGroup] = [sector.name]
                    else:
                        industryGroup = sector.getIndustryGroup(industryGroup)
                        for industry in industryGroup.getIndustryNames():
                            if len(industries) > 0 and not industry in industries: continue
                            if type == 'industry':
                                names[industry] = [sector.name, industryGroup.name]
                            else:
                                industry = industryGroup.getIndustry(industry)
                                for subIndustry in industry.getSubIndustryNames():
                                    if len(subIndustries) > 0 and not subIndustry in subIndustries: continue
                                    if type == 'subIndustry':
                                        names[subIndustry] = [sector.name, industryGroup.name, industry.name]
        return names

    def getCodeInfo(self, code):
        return self.definition[str(code)]
    # def getIndustryGroups(self, sector=None):
    #     industryGroups = set()
    #     if sector == None:
    #         for sector, sectorData in self.__gicsData.items():
    #             industryGroups = industryGroups.union(sectorData['industryGroups'].keys())
    #     elif sector in self.__gicsData:
    #         industryGroups = set(self.__gicsData[sector]['industryGroups'].keys())
    #     else:
    #         raise ValueError('unknown sector: %s' % sector)
    #     industryGroups = list(industryGroups)
    #     industryGroups.sort()
    #     return industryGroups
    
    # def getNames(self, sector, industryGroup=None, industry=None):
    #     return None
    #     if not sector in self.__gicsData:
    #         sector = self.__renameSector(sector)
    #     if industry == None:
    #         return {'sector': sector, 'industry': None}
    #     for industryGroup in self.getIndustryGroups(sector):
    #         for industry in self.getIndustryGroups(sector, industryGroup):
    #             pass
    #         print(self.getIndustryGroups(sector))
    #         print(industry)
    #         return None
    #     return {'sector': sector, 'industry': None}