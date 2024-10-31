from reportlab.platypus import BaseDocTemplate, Frame, PageTemplate, Table, Paragraph, PageBreak, Image, Spacer
from reportlab.lib import colors
import matplotlib.colors as mcolors
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
        bottomPadding=10)
    
    __styles = getSampleStyleSheet()
    
    @staticmethod
    def __onPage(canvas, doc, pagesize=A4):
        pageNum = canvas.getPageNumber()
        canvas.drawCentredString(pagesize[0]/2, 20, str(pageNum))
    
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
    
    def __init__(self, name):
        self.doc = BaseDocTemplate(
            'reports/%s.pdf' % name,
            pageTemplates=[
                self.__portraitTemplate,
            ]
        )
        self.story = []
    
    @property
    def colors(self):
        return colors

    def printStyles(self):
        self.__styles.list()
    
    def printColors(self):
        colors = list(mcolors.CSS4_COLORS.keys())
        colors.sort()
        pp(colors)

    def getStyle(self, name):
        return copy.deepcopy(self.__styles[name])

    def addParagraph(self, text, style=__styles['Normal']):
        self.story.append(Paragraph(text, style))

    def addTable(self, df):
        self.story.append( self.__df2table(df) )

    def addSpace(self, inches):
        self.story.append( Spacer(1,inches * inch) )
    
    def plotLineDF(self, dataFrame, y=[], labels=None, ylabel=None, yline=None, colors=None, grid=True, height=3):
        chartFig, ax = plt.subplots(dpi=300, figsize=(7, height))
        # for some reason it needs the y seting for things to work properly
        if len(y) == 0:
            y = dataFrame.columns.as_list()
        dataFrame.plot(y=y, ax=ax, kind='line', linewidth=1, label=labels, color=colors)
        if ylabel != None:
            ax.set_ylabel(ylabel)
        if yline != None:
            ax.axhline(y=yline, color='green', linestyle='--')
        # plt.xticks(rotation=45, fontsize=6)
        plt.xticks(fontsize=6)
        plt.grid(grid)
        self.story.append( self.__fig2image(chartFig) )
        plt.close(chartFig)

    def plotBarDF(self, dataFrame, ylabel=None, colors=[], grid=True):
        divFig, ax = plt.subplots(dpi=300, figsize=(8, 2))
        # for some reason it needs the y seting for things to work properly
        dataFrame.plot(y=dataFrame.columns[0], kind='bar', ax=ax, color=colors, label='_hidden', grid=grid)
        if ylabel != None:
            ax.set_ylabel(ylabel)
        ax.set_axisbelow(True)
        plt.xticks(rotation=20, fontsize=6)
        self.story.append( self.__fig2image(divFig) )
        plt.close(divFig)

    def addPageBreak(self):
        self.story.append(PageBreak())
    
    def buildDoc(self):
        self.doc.build(self.story)

