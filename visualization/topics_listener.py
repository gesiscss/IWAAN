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
            return (re.search('\/\*(.+?)\*\/', comment).group(1))
        else:
            pass
    
    #extracting a type of comment (new section added, revision deleted, section edited etc.)    
    def get_action_type(self,comment):
        if re.search('\/\*(.+?)\*\/', comment) and 'Signing' not in comment:   
            if "new section" in comment:
                action_type = "new"
            else:
                action_type = "edit"
        elif "Undid revision" in comment:
            action_type = "undid revision " + comment.split(" ")[2]
            self.talk_content.loc[self.talk_content['revid']==int(comment.split(" ")[2]), 'topics'] += ': deleted'
        else:
            action_type = ' '.join(comment.split(" ")[0:2])
        return action_type

    #extract topics and action_types
    def extract_topics(self, wikipediadv_ins):
        self.talk_content = self.df
        self.talk_content['topics'] = self.talk_content.comment.apply(lambda x: self.find_topic(x) if not pd.isna(x) else None)
        self.extract_null_content(wikipediadv_ins)
        self.talk_content['action_type'] = self.talk_content.comment.apply(lambda x: self.get_action_type(x) if not pd.isna(x) else None)
        #group by talk topics
        #topic_df = self.talk_content.groupby(by="topics").count().sort_values('user', ascending=False)

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
