from reportlab.platypus import Table, Paragraph, Image
from reportlab.lib import colors
from reportlab.lib.units import inch
import io

class Report():
    @staticmethod
    def __fig2image(f):
        buf = io.BytesIO()
        f.savefig(buf, format='png', dpi=300)
        buf.seek(0)
        x, y = f.get_size_inches()
        return Image(buf, x * inch, y * inch)

    @staticmethod
    def __df2table(df):
        return Table(
            [[Paragraph(col) for col in df.columns]] + df.values.tolist(), 
            style=[
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('LINEBELOW',(0,0), (-1,0), 1, colors.black),
            ('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
            ('BOX', (0,0), (-1,-1), 1, colors.black),
            # ('ROWBACKGROUNDS', (0,0), (-1,-1), [colors.lightgrey, colors.white]),
            ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
        ],
        hAlign = 'LEFT')
    
    def __init__(self, docName):
        self.docName = docName

    
