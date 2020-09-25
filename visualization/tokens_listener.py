import copy
import qgrid
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from IPython.display import display, Markdown as md, clear_output, HTML
from ipywidgets import Output, fixed
from .wordclouder import WordClouder
from .editors_listener import remove_stopwords
from datetime import datetime, timedelta
import plotly
import plotly.graph_objects as go

from metrics.token import TokensManager
from metrics.conflict import ConflictManager


class TokensListener():

    def __init__(self, agg, sources, lng):
        self.editors = agg[["editor_str", "editor"]].drop_duplicates().rename({"editor_str": "editor_id",
                                                       "editor": "name"}, axis=1).reset_index(drop=True)
        self.sources = sources
        self.lng = lng
        self.page_title = sources["tokens_all"]["article_title"].unique()[0]
        
    def get_columns(self):
        #create columns 'time_diff' (Time in sec between this action and the last action on the token)
        # and 'reverted_editor' (editor's name who made a previous action on the token)
        self.token_source.sort_values(['token_id', 'rev_time'], ascending = True, inplace=True)
        self.token_source['time_diff'] = self.token_source['rev_time'] - self.token_source.shift(1)['rev_time']
        self.token_source['reverted_editor'] = self.token_source.shift(1)['name']
        to_delete = (
                 #First row of each token
                 (self.token_source['o_rev_id'] == self.token_source['rev_id']))

        # delete but keep the row
        self.token_source.loc[to_delete, 'time_diff'] = np.nan
        self.token_source.loc[to_delete, 'reverted_editor'] = np.nan

    def convert_oadd(self):
        #convert 'action' of first insertion to 'oadd'
        #self.token_source['action'] = self.token_source.apply(lambda x: 'oadd' if x['o_rev_id'] == x['rev_id'] else x['action'], axis=1)
        mask_add = self.token_source["o_rev_id"] == self.token_source["rev_id"]
        self.token_source.loc[mask_add, "action"] = "oadd"
        
    def get_editor_names(self):
        #get editor names by editor id
        self.token_source = self.token_source.rename(columns={"editor":'editor_id'})
        self.token_source['editor_id'] = self.token_source['editor_id'].astype(str)
        tokens_merged = self.editors[['editor_id', 'name']].merge(self.token_source, right_index=True, on='editor_id', how='outer')
        self.token_source = tokens_merged[tokens_merged['token'].notnull()].copy()
        
    def convert_time_diff(time_diff):
        #convert time_diff to display as time in days:hours:min:sec format
        try:
            s = time_diff.seconds
            hours, remainder = divmod(s, 3600)
            minutes, seconds = divmod(remainder, 60)
            return '{:02}:{:02}:{:02}:{}'.format(int(time_diff.days), int(hours), int(minutes), int(seconds))
        except ValueError:
            return None
        
        
    def on_selection_change(self, change):
        #show link to wikipedia diff when clicking on a row
        with self.out213:
            clear_output()

            # Extract the rev_id selected and convert it to string.
            diff = self.qgrid_selected_revision.get_selected_df().reset_index()['rev_id'].iloc[0]      
            
            # Print URL.
            url = f"https://{self.lng}.wikipedia.org/w/index.php?&title={self.page_title}&diff={diff}"
            print('Link to the wikipedia diff: ')
            print(url)
        
    def listen(self, revid, stopwords):
        # Get source data through ConflictManager. 
        if stopwords == 'Not included':
            link_token = remove_stopwords(self.sources["tokens_all"], self.lng)
            self.token_source = link_token
            del link_token
        else:
            link_token = self.sources["tokens_all"]
            self.token_source = link_token
            del link_token
        
        self.token_source = self.token_source.reset_index(drop=True)

        #selected revision id:
        #self.rev_id = int(rev_id)
        
        #extract editor name and timestamp to display before the table
        self.rev_id = revid
        self.filtered_df = self.token_source[self.token_source['rev_id']==self.rev_id]
        if len(self.filtered_df) != 0:
            editor_name = self.editors.loc[self.editors['editor_id'] == self.filtered_df['editor'].values[0], 'name'].values[0]
        else:
            return display(md("No tokens in this revision!"))
        timestamp = pd.DatetimeIndex(self.token_source[self.token_source['rev_id']==self.rev_id]['rev_time'])[0]
        display(md(f"***Selected revision: ID: {self.rev_id}, editor name: {str(editor_name)}, timestamp: {str(timestamp.date())} {str(timestamp.time())}***"))
                   
        # Print URL to wikipedia diff.
        url = f"https://{self.lng}.wikipedia.org/w/index.php?title={self.page_title}&diff={self.rev_id}"
        display(HTML(f'<a href="{url}" target="_blank">Click here to see the Wikipedia Text DIFF</a>'))
            
        if  self.rev_id != None:
            #add necessary columns and process the dataframe:
            self.convert_oadd()
            self.get_editor_names()
            self.get_columns()
            #self.token_source['time_diff'] = self.token_source['time_diff'].apply(lambda x: TokensListener.convert_time_diff(x))
            
            #sort the dataframe by timestamp and token_id:
            self.token_source.sort_values(['rev_time', 'token_id'], ascending = True, inplace=True)
                   
            #get tokens from the selected revision (from previous and future revisions as well):
            rev_tokens = self.token_source.loc[self.token_source['rev_id'] == self.rev_id, 'token_id'].values
            tokens_for_grid = self.token_source.loc[self.token_source['token_id'].isin(rev_tokens), ['token', 'token_id', 'action', 'rev_id', 'rev_time', 'name', 'o_rev_id', 'reverted_editor', 'time_diff' ]].rename(columns={'token': 'string', 'name': 'editor'})
            
            #convert the format of columns to display:
            tokens_for_grid['rev_id'] = tokens_for_grid['rev_id'].astype(int).astype(str)
            tokens_for_grid['time_diff'] = tokens_for_grid['time_diff'].apply(lambda x: TokensListener.convert_time_diff(x))
            tokens_for_grid['time_diff'] = tokens_for_grid['time_diff'].astype(str)
            tokens_for_grid['token_id'] = tokens_for_grid['token_id'].astype(int).astype(str)
            
            tokens_for_grid.sort_values(["token_id", "rev_time"], inplace=True)
            tokens_for_grid.set_index('token_id', inplace=True)
            self.tokens_for_grid = tokens_for_grid.copy()
                   
            #qgrid widget:
            columns_set = {"rev_time": {"width": 180}, "action": {"width": 65}, "string": {"width": 100}, "token_id": {"width": 94}}
            
            qgrid_selected_revision = qgrid.show_grid(self.tokens_for_grid, column_definitions=columns_set)
            self.qgrid_selected_revision = qgrid_selected_revision
            
            display(self.qgrid_selected_revision)
            self.out213 = Output()
            display(self.out213)
            self.qgrid_selected_revision.observe(self.on_selection_change, names=['_selected_rows'])
        else:
            display(md(f'**The selected revision does not exist for this page. Try another**'))
            
            
            
class TokensOwnedListener():

    def __init__(self, agg, sources, lng):
        self.editors = agg[["editor_str", "editor"]].drop_duplicates().rename({"editor_str": "editor_id",
                                                       "editor": "name"}, axis=1).reset_index(drop=True)
        self.sources = sources
        self.lng = lng
        self.page_title = sources["tokens_all"]["article_title"].unique()[0]
        
    
    
    def get_editor_names(self):
        #get editor names by editor id
#         self.token_source = self.token_source.rename(columns={"editor":'editor_id'})
        self.editors['o_editor'] = self.editors['editor_id'].astype(str)
        self.token_source['o_editor'] = self.token_source['o_editor'].astype(str)
        tokens_merged = self.editors[['o_editor', 'name']].merge(self.token_source, right_index=True, on='o_editor', how='outer')
        self.token_source = tokens_merged[tokens_merged['token'].notnull()].copy()
        
        
    def listen(self,_range1, _range2, stopwords, granularity):
        
        # Get source data through ConflictManager. 
        if stopwords == 'Not included':
            link_token = remove_stopwords(self.sources["tokens_all"], self.lng)
            self.token_source = link_token
            del link_token
        else:
            link_token = self.sources["tokens_all"]
            self.token_source = link_token
            del link_token
        
        self.token_source = self.token_source.reset_index(drop=True)
        if (len(str(_range1.year)) < 4) | (len(str(_range2.year)) < 4):
            return display(md("Please enter the correct date!"))
        if _range1 > _range2:
            return display(md("Please enter the correct date!"))
        else:
            self.token_source = self.token_source[(self.token_source.rev_time.dt.date >= _range1) & (self.token_source.rev_time.dt.date <= _range2)]
        
        self.token_source['rev_time'] = pd.to_datetime(self.token_source['rev_time']).dt.tz_localize(None)
        self.get_editor_names()

        days = self.token_source['rev_time'].dt.to_period(granularity[0]).unique() #getting unique days 
        today = pd.Period(datetime.today(), freq=granularity[0])
        days = pd.Series(np.append(days, today)).sort_values(ascending=False) #adding today

        if len(days) > 0:
            days = days.dt.to_timestamp(granularity[0]) + pd.DateOffset(1) #converting and adding one day for extracting previous dates from dataframe
            self.summ = pd.DataFrame(columns=['name', 'action', 'rev_time'])
            _abs = []
            df = self.token_source
            for rev_time in days:
                df = df[df['rev_time'] <= rev_time]
                last_action = df.groupby('token_id').last() #last of group values for each token id
                surv = last_action[last_action['action'] != 'out'].groupby('name')['action'].agg('count').reset_index()
                surv['rev_time'] = rev_time - pd.DateOffset(1)
                self.summ = self.summ.append(surv)

            #getting top editors among the token owners over all time
            top_editors = self.summ.groupby('name')['action'].agg('sum').sort_values(ascending=False).reset_index()[:15]
            first_date = self.summ.groupby('name').last().reset_index() #first date of oadd for every editor
            top_editors_merged = pd.merge(top_editors, first_date[['name', 'rev_time']], on='name').sort_values('rev_time') #adding first date for each editor and sorting by date of first oadd

            #plot
            fig = go.Figure()
            for editor in top_editors_merged['name']: 
                x = self.summ.loc[self.summ['name']==editor, 'rev_time']
                y = self.summ.loc[self.summ['name']==editor, 'action']
                fig.add_trace(go.Scatter(x=x, y=y, name = editor, stackgroup='one'))
            fig.update_layout(hovermode='x unified', showlegend=True, margin=go.layout.Margin(l=50,
                                                                  r=50,
                                                                  b=150,
                                                                  t=10,
                                                                  pad=3))
            fig.show()
            
#             data = []
#             for editor in top_editors_merged['name']: 
#                 x = self.summ.loc[self.summ['name']==editor, 'rev_time']
#                 y = self.summ.loc[self.summ['name']==editor, 'action']
#                 data.append(go.Scatter(x=x, y=y, name = editor, stackgroup='one'))

#             layout = go.Layout(hovermode='x unified', showlegend=True, margin=go.layout.Margin(l=50,
#                                                                   r=50,
#                                                                   b=150,
#                                                                   t=10,
#                                                                   pad=3))
#             plotly.offline.init_notebook_mode(connected=True)
#             plotly.offline.iplot({"data": data, "layout": layout})



            
            
            
            
            
