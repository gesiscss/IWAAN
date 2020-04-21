#import matplotlib.pyplot as plt
import pandas as pd
import plotly
#import plotly.plotly as py
from plotly import graph_objs
import re
from external.wikipedia import WikipediaDV, WikipediaAPI
#from wordcloud import WordCloud


class TopicsListener():

    def __init__(self, df):
        self.df = df
        self.df_plotted = None
    
    def find_topic(self,comment):
        if re.search('\/\*\s(.+?)\s\*\/', comment) and 'Signing' not in comment:
            if not re.search('\/\*\s(.+?@.+?)\s\*\/', comment):
                return (re.search('\/\*(.+?)\*\/', comment).group(1))
        else:
            pass
    
    #extracting a type of comment (new section added, revision deleted, section edited etc.)    
    def get_action_type(self):
        self.talk_content = self.talk_content[::-1].reset_index()
        self.talk_content['action_type'] = None
        for i, row in self.talk_content.iterrows():
            #checking if the rows before have this topic
            if row['topics'] in list(self.talk_content.topics[:i]):
                self.talk_content.loc[i, ['action_type']] = 'edit'
            #no rows found, means it's new
            else:
                self.talk_content.loc[i, ['action_type']] = 'new'
        self.talk_content = self.talk_content[::-1].reset_index()
        

    #extract topics and action_types
    def extract_topics(self, wikipediadv_ins):
        self.talk_content = self.df
        self.talk_content['topics'] = self.talk_content.comment.apply(lambda x: self.find_topic(x) if not pd.isna(x) else None)
        self.extract_null_content(wikipediadv_ins)
        self.get_action_type()
        
        #removing those topics that were untouched
        topics = self.talk_content.topics.value_counts()
        self.talk_content = self.talk_content[self.talk_content['topics'].isin(topics.index[topics>1])]
        self.df = self.talk_content


        #keeping only necessary columns for display
        topic_df = self.talk_content.drop(self.talk_content.columns.difference(['revid','user', 'year_month', 'topics', 'action_type']), axis=1)
        topic_df = topic_df[topic_df['topics'].notnull()].set_index('topics')
        
        return topic_df
    
    #for those revisions that have null comment, we extract diff using wikipedia api
    def extract_null_content(self, wikipediadv_ins):
        # setting empty dataframe
        self.talk_diff = pd.DataFrame(columns=['fromid', 'fromrevid', 'fromns', 'fromtitle', 'toid', 'torevid', 'tons', 'totitle', 'content'])

        null_topic = self.talk_content[self.talk_content['comment']==""]
        #iterrating over empty revision content
        for i, row in null_topic.iterrows():
            torev = row['revid']
            if i == len(self.talk_content) - 1:
                pass
            else:
                fromrev = self.talk_content.iloc[i+1]['revid']
                #appending diff content
                self.talk_diff = self.talk_diff.append(wikipediadv_ins.get_talk_rev_diff(fromrev=fromrev, torev=torev), ignore_index=True)
        
        self.talk_diff = self.talk_diff.rename(columns={"*":"comment"})
        
        #iterrating over comments 
        for comment in self.talk_diff['comment']:
            if re.search('==(.+?)==', comment):
                rev_id = self.talk_diff.loc[self.talk_diff['comment']==comment,'torevid']
                self.talk_content.loc[self.talk_content['revid']==int(rev_id), 'topics'] = re.search('==(.+?)==', comment).group(0)[2:-2]

    def listen(self, begin, end, granularity):
        df = self.df
        
        filtered_df = df[(df.year_month.dt.date >= begin) & (df.year_month.dt.date <= end)]
        groupped_df = filtered_df.groupby([pd.Grouper(key='year_month', freq=granularity[0]), pd.Grouper(key='topics')]).count().reset_index()

        # Plot Graph
        

        data = []
        topic_count = filtered_df.groupby(by="topics").count().sort_values('user', ascending=False)
        #displaying top 10 topics that have more than 5 edits (or just top 10)
        topic_count_top = topic_count.iloc[0:10].reset_index()
        if granularity[0] == 'Y':
            groupped_df['year_month'] = groupped_df['year_month'].dt.year
        elif granularity[0] == 'M':
            groupped_df['year_month'] = groupped_df['year_month'].dt.strftime('%Y-%m')
        else:
            groupped_df['year_month'] = groupped_df['year_month'].dt.date
        for topic in topic_count_top.topics:
            if topic != "":
                data.append(
                        graph_objs.Bar(
                            x=groupped_df[groupped_df['topics'] == topic]['year_month'], 
                            y=groupped_df[groupped_df['topics'] == topic]['comment'], name=topic
                            )
                )
        layout = graph_objs.Layout(hovermode='closest',
                                       xaxis=dict(title=granularity, ticklen=5, zeroline=True, gridwidth=2,type="category",
                                                  categoryorder='category ascending', tickangle=30, tickmode='auto', nticks=15),
                                       yaxis=dict(title='Revisions',
                                                  ticklen=5, gridwidth=2),
                                       legend=dict(x=0, y=2),
                                       showlegend=True, barmode='group')



        plotly.offline.init_notebook_mode(connected=True)
        plotly.offline.iplot({"data": data, "layout": layout})

        self.df_plotted = groupped_df
