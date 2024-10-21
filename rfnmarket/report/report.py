from reportlab.platypus import BaseDocTemplate, Frame, PageTemplate, Table, Paragraph, PageBreak, Image, Spacer
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
import io, copy
from pprint import pp
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from ..utils import storage

class Report():
    __padding = dict(
        leftPadding=72, 
        rightPadding=72,
        topPadding=10,
        bottomPadding=18)
    
    __styles = getSampleStyleSheet()
    
    @staticmethod
    def __onPage(canvas, doc, pagesize=A4):
        pageNum = canvas.getPageNumber()
        canvas.drawCentredString(pagesize[0]/2, 50, str(pageNum))
    
    @staticmethod
    def __onPageLandscape(canvas, doc):
        Report.__onPage(canvas, doc, pagesize=landscape(A4))
        return None
    
    __portraitTemplate = PageTemplate(
        id = 'portrait', 
        frames = Frame(0, 0, *A4, **__padding),
        onPage = __onPage, 
        pagesize = A4)

    __landscapeTemplate = PageTemplate(
        id = 'landscape', 
        frames = Frame(0, 0, *landscape(A4), **__padding), 
        onPage=__onPageLandscape, 
        pagesize = landscape(A4))

    @staticmethod
    def __fig2image(f):
        buf = io.BytesIO()
        f.savefig(buf, format='png', dpi=300)
        buf.seek(0)
        x, y = f.get_size_inches()
        return Image(buf, x * inch, y * inch)

    @staticmethod
    def __df2table(df):
        tableData = [[col for col in df.columns]] + df.values.tolist()
        return Table(
            tableData,
            style=[
            ('FONT', (0,0), (-1,0), 'Helvetica-Bold', 8),
            ('FONT', (0,1), (-1,-1), 'Helvetica', 8),
            ('LINEBELOW',(0,0), (-1,0), 1, colors.black),
            ('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
            ('BOX', (0,0), (-1,-1), 1, colors.black),
            # ('ROWBACKGROUNDS', (0,0), (-1,-1), [colors.lightgrey, colors.white]),
            ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
        ],
        hAlign = 'LEFT')
    @staticmethod
    def __s2table(s):
        tableData = [[x,y] for x,y in s.items()]
        return Table(
            [[x,y] for x,y in s.items()],
            # rowHeights=[15] * len(tableData),
            style=[
            ('FONT', (0,0), (0,-1), 'Helvetica-Bold', 10),
            ('FONT', (0,1), (-1,-1), 'Helvetica', 10),
            # ('LINEBELOW',(0,0), (-1,0), 1, colors.black),
            ('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
            ('BOX', (0,0), (-1,-1), 1, colors.black),
            # ('ROWBACKGROUNDS', (0,0), (-1,-1), [colors.lightgrey, colors.white]),
            ('BACKGROUND', (0,0), (0,-1), colors.lightgrey),
        ],
        hAlign = 'LEFT')
    
    def __init__(self):
        pass

    def makeQuickenReport(self, quickenData, profileData, chartData):
        # create doc pages
        doc = BaseDocTemplate(
            'reports/quickenReport.pdf',
            pageTemplates=[
                self.__portraitTemplate,
            ]
        )
        closeType = 'adjclose'
        story = []
        for symbol, dfQuicken in quickenData.items():
            if not symbol in profileData: continue
            # if symbol != 'MMM': continue

            # profile data
            name = profileData[symbol]['name']
            columns = ['type', 'exchange', 'sector', 'fundFamily', 'industry', 'country', 'city', 'state']
            row = {}
            for info in columns:
                if info in profileData[symbol]:
                    row[info] = profileData[symbol][info]
            profile = pd.DataFrame([row], columns=columns).dropna(axis=1)

            # get total incoming transactions
            sharesInRows = []
            firstDate = datetime.now()
            for transaction in ['ShrsIn', 'Buy', 'ReinvDiv', 'ReinvSh', 'ReinvLg', 'ReinvInt']:
                transactions = dfQuicken[dfQuicken['transaction'] == transaction]
                if len(transactions) == 0: continue
                date = transactions.iloc[0]['date']
                if date < firstDate: firstDate = date
                shares = transactions['shares'].dropna().sum()
                amount = transactions['costBasis'].dropna().sum()
                price = amount / shares
                sharesInRows.append({'transaction': transaction, 'shares': shares, 'amount': amount, 'average price': price})
            sharesIn = pd.DataFrame(sharesInRows, columns=['transaction', 'shares', 'amount', 'average price'])
            
            # get total outgoing transactions
            sharesInRows = []
            for transaction in ['Sell', 'ShrsOut']:
                transactions = dfQuicken[dfQuicken['transaction'] == transaction]
                if len(transactions) == 0: continue
                shares = -transactions['shares'].dropna().sum()
                amount = transactions['costBasis'].dropna().sum()
                price = amount / shares
                sharesInRows.append({'transaction': transaction, 'shares': shares, 'amount': amount, 'average price': price})
            sharesOut = pd.DataFrame(sharesInRows, columns=['transaction', 'shares', 'amount', 'average price'])

            # portfolio data
            sharesLeft = float(sharesIn['shares'].sum() - sharesOut['shares'].sum())
            chart = pd.DataFrame(chartData[symbol]).T.iloc[-1]
            date = str(datetime.fromtimestamp(int(chart.name)))
            price = float(chart[closeType])
            worth = sharesLeft * price
            columns = ['last close date', 'shares', 'price', 'worth']
            row = {'last close date': date, 'shares': sharesLeft, 'price': price, 'worth': worth}
            portfolio = pd.DataFrame([row], columns=['last close date', 'shares', 'price', 'worth'])

            # start chart from first investment
            fromTS = int(firstDate.timestamp())
            chartAll = pd.DataFrame(chartData[symbol]).T
            chartFirst = chartAll.loc[fromTS:]

            # get price chart and normalize
            dfPrice = chartFirst[[closeType]].copy()
            dfPrice[closeType] = dfPrice[closeType].div(dfPrice.iloc[0][closeType])
            dfPrice.index = pd.to_datetime(dfPrice.index, unit='s').date
            dfPrice.index = dfPrice.index.astype(str) # need to do this for plotting to work correctly

            # get dividends
            divRows = []
            dfTransactions = dfQuicken.sort_values(by=['date'])
            if 'dividend' in chartFirst.columns:
                dfDividends = chartFirst.dropna(subset = ['dividend'])
                if len(dfDividends) > 0:
                    for index , values in dfDividends.iterrows():
                        date = datetime.fromtimestamp(index)
                        qTrDate = dfTransactions[dfTransactions['date'] <= date]
                        if len(qTrDate) == 0: continue
                        sharesAtTime = qTrDate['shares'].sum()
                        divValue = sharesAtTime * values['dividend']
                        divShares = divValue / values[closeType]
                        row = {'date': date, 'div / share': values['dividend'], 'shares': sharesAtTime,
                            'div shares': divShares, 'price': values[closeType], 'div value': divValue}
                        divRows.append(row)
            dfDivQuicken = dfTransactions[dfTransactions['transaction'].isin(['ReinvDiv'])]
            dfDivPortfolio = pd.DataFrame(divRows)

            # get dividends history for 5 years
            dfDiviAll = pd.DataFrame()
            if 'dividend' in chartAll.columns:
                fromTS = datetime.now().timestamp() - (60*60*24*356*5)
                dfDiviAll = chartAll.loc[fromTS:].dropna(subset = ['dividend'])
                fromTS = int(firstDate.timestamp())
                dfDiviAll['owned'] = dfDiviAll.index >= fromTS
                dfDiviAll['divpercent'] = (dfDiviAll['dividend'] / dfDiviAll[closeType]) * 100.0
                dfDiviAll.index = pd.to_datetime(dfDiviAll.index, unit='s').date
                dfDiviAll.index = dfDiviAll.index.astype(str)

            # create pages 
            # profile
            symbolLine = '%s: %s' % (symbol, name)
            story.append( Paragraph(symbolLine, self.__styles['Heading1'], ) )
            story.append( self.__df2table(profile) )
            if 'info' in profileData[symbol]:
                story.append( Spacer(1,10) )
                story.append( Paragraph(profileData[symbol]['info']) )

            # shares
            if len(sharesIn) > 0:
                story.append( Paragraph('Incoming Shares: start date: %s' % firstDate.strftime('%Y-%m-%d'), self.__styles['Heading2'] ) )
                story.append( self.__df2table(sharesIn.round(4)) )
            if len(sharesOut) > 0:
                story.append( Paragraph('Outgoing Shares:', self.__styles['Heading2'] ) )
                story.append( self.__df2table(sharesOut.round(4)) )
            story.append( Paragraph('Shares in Portfolio:', self.__styles['Heading2'], ) )
            story.append( self.__df2table(portfolio.round(4)) )

            # plot price
            chartFig, ax = plt.subplots(dpi=300, figsize=(8, 4))
            dfPrice.plot(y=closeType, kind='line', ax=ax, color='blue', linewidth=1, label='_hidden')
            ax.set_ylabel('price')
            ax.axhline(y=1.0, color='green', linestyle='--')
            # plt.xticks(rotation=45, fontsize=6)
            plt.xticks(fontsize=6)
            plt.grid(True)

            story.append( self.__fig2image(chartFig) )
            plt.close(chartFig)
            story.append( PageBreak() )

            if len(dfDivPortfolio) > 0:
                story.append( Paragraph('Dividends per data:', self.__styles['Heading2'] ) )
                story.append( self.__df2table(dfDivPortfolio.round(4)) )
            
            if len(dfDivQuicken) > 0:
                story.append( Paragraph('Dividends reinvest:', self.__styles['Heading2'] ) )
                story.append( self.__df2table(dfDivQuicken.round(4)) )
            elif len(dfDivPortfolio) > 0:
                style = copy.deepcopy(self.__styles['Heading2'])
                style.textColor = colors.red
                story.append( Paragraph('No Dividends reinvested', style ) )
            
            if len(dfDiviAll) > 0:
                divColors = [("red" if x else "blue") for x in dfDiviAll['owned']]
                divFig, ax = plt.subplots(dpi=300, figsize=(8, 2))
                dfDiviAll.plot(y='dividend', kind='bar', ax=ax, color=divColors, label='_hidden', grid=True)
                ax.set_ylabel('$ / share')
                ax.set_axisbelow(True)
                plt.xticks(rotation=20, fontsize=6)
                story.append( Paragraph('Dividend ($) per share', self.__styles['Heading2'] ) )
                story.append( self.__fig2image(divFig) )
                plt.close(divFig)

                # dividends percent
                divFig, ax = plt.subplots(dpi=300, figsize=(8, 2))
                dfDiviAll.plot(y='divpercent', kind='bar', ax=ax, color=divColors, label='_hidden', grid=True)
                ax.set_ylabel('% / share')
                ax.set_axisbelow(True)
                plt.xticks(rotation=20, fontsize=6)
                story.append( Paragraph('Dividend percent per share', self.__styles['Heading2'] ) )
                story.append( self.__fig2image(divFig) )
                plt.close(divFig)

            
            story.append( PageBreak() )

        doc.build(story)
