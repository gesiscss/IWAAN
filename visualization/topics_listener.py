#import matplotlib.pyplot as plt
import pandas as pd
import plotly
#import plotly.plotly as py
from plotly import graph_objs
#from wordcloud import WordCloud


class TopicsListener():

    def __init__(self, df):
        self.df = df
        self.df_plotted = None

    def listen(self, begin, end, granularity):
        df = self.df
        
        filtered_df = df[(df.year_month.dt.date >= begin) & (df.year_month.dt.date <= end)]
        groupped_df = filtered_df.groupby([pd.Grouper(key='year_month', freq=granularity[0]), pd.Grouper(key='topics')]).count().reset_index()

        # Plot Graph
        

        data = []
        topic_count = filtered_df.groupby(by="topics").count().sort_values('user', ascending=False)
        topic_count = topic_count.loc[topic_count["user"]>5].iloc[0:10].reset_index()
        for topic in topic_count.topics:
            if topic != "":
                data.append(
                        graph_objs.Bar(
                            x=groupped_df[groupped_df['topics'] == topic]['year_month'], 
                            y=groupped_df[groupped_df['topics'] == topic]['comment'], name=topic
                            )
                )
        layout = graph_objs.Layout(hovermode='closest',
                                       xaxis=dict(title=granularity, ticklen=5,
                                                  zeroline=True, gridwidth=2),
                                       yaxis=dict(title='Comments',
                                                  ticklen=5, gridwidth=2),
                                       legend=dict(x=0, y=1.2),
                                       showlegend=True, barmode='group')



        plotly.offline.init_notebook_mode(connected=True)
        plotly.offline.iplot({"data": data, "layout": layout})

        self.df_plotted = groupped_df
