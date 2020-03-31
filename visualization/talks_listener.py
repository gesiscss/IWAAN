#import matplotlib.pyplot as plt
import pandas as pd
import plotly
#import plotly.plotly as py
from plotly import graph_objs
#from wordcloud import WordCloud


class TalksListener():

    def __init__(self, df):
        self.df = df
        self.df_plotted = None

    def listen(self, begin, end, granularity):
        df = self.df
        
        #begin = pd.to_datetime(begin).tz_localize(None)
        #end = pd.to_datetime(end).tz_localize(None)

#         if begin < end or begin == end:
#             variable = 0

#         elif begin > end:
#             variable = end
#             end = begin
#             begin = variable
#         else:
#             variable = 1
#             print('Can not be the case!')

        filtered_df = df[(df.year_month.dt.date >= begin) & (df.year_month.dt.date <= end)]
        groupped_df = filtered_df.groupby(pd.Grouper(key='year_month', freq=granularity[0]+'S')).count().reset_index()

        # Plot Graph

        data = [
            graph_objs.Scatter(
                x=groupped_df['year_month'], y=groupped_df["comment"],
                marker=dict(color='rgba(0, 0, 0, 1)'))
        ]

        layout = graph_objs.Layout(hovermode='closest',
                                   xaxis=dict(title=granularity, ticklen=5,
                                              zeroline=True, gridwidth=2),
                                   yaxis=dict(title='Comments',
                                              ticklen=5, gridwidth=2),
                                   legend=dict(x=0.5, y=1.2),
                                   showlegend=False, barmode='group')




        plotly.offline.init_notebook_mode(connected=True)
        plotly.offline.iplot({"data": data, "layout": layout})

        self.df_plotted = groupped_df
