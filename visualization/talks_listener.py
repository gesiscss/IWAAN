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
        

        filtered_df = df[(df.year_month.dt.date >= begin) & (df.year_month.dt.date <= end)]
        groupped_df = filtered_df.groupby(pd.Grouper(key='year_month', freq=granularity[0])).count().reset_index()

        # Plot Graph

        data = [
            graph_objs.Scatter(
                x=groupped_df['year_month'].dt.date, y=groupped_df["comment"],
                marker=dict(color='rgba(0, 0, 0, 1)'), hovertemplate ='%{x}<extra>%{y}</extra>')
        ]
        if granularity[0] == "D":
            tickformat = "%Y-%m-%d"
        else:
            tickformat = "%b %Y"

        layout = graph_objs.Layout(hovermode='closest',
                           xaxis=dict(title="Daily", ticklen=5,
                                      zeroline=True, gridwidth=2, tickformat = tickformat),
                           yaxis=dict(title='Comments',
                                      ticklen=5, gridwidth=2),
                           legend=dict(x=0.5, y=1.2),
                           showlegend=False, barmode='group')




        plotly.offline.init_notebook_mode(connected=True)
        plotly.offline.iplot({"data": data, "layout": layout})

        self.df_plotted = groupped_df
