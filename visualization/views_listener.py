#import matplotlib.pyplot as plt
import pandas as pd
import plotly
#import plotly.plotly as py
from plotly import graph_objs
#from wordcloud import WordCloud


class ViewsListener():

    def __init__(self, df):
        self.df = df
        self.df_plotted = None

    def listen(self, begin, end, granularity):
        df = self.df

        if begin < end or begin == end:
            variable = 0

        elif begin > end:
            variable = end
            end = begin
            begin = variable
        else:
            variable = 1
            print('Can not be the case!')

        filtered_df = df[(df.timestamp >= begin) & (df.timestamp <= end)]
        groupped_df = filtered_df.groupby(pd.Grouper(
            key='timestamp', freq=granularity[0])).sum().reset_index()

        # Plot Graph
        views = list(groupped_df.views)
        month = list(groupped_df.timestamp)

        trace1 = graph_objs.Scatter(
            x=month, y=views,
            mode='lines+markers', name='Views',
            marker=dict(color='rgba(0, 128, 43, .8)')
        )

        layout = graph_objs.Layout(hovermode='closest',
                                   xaxis=dict(title=granularity, ticklen=5,
                                              zeroline=True, gridwidth=2),
                                   yaxis=dict(title='Views',
                                              ticklen=5, gridwidth=2),
                                   legend=dict(x=0.5, y=1.2),
                                   showlegend=False)

        data = [trace1]

        plotly.offline.init_notebook_mode(connected=True)
        plotly.offline.iplot({"data": data, "layout": layout})

        self.df_plotted = groupped_df
