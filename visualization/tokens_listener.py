import copy
import qgrid
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from IPython.display import display, Markdown as md, clear_output, HTML
from ipywidgets import Output, fixed
from .wordclouder import WordClouder

from metrics.token import TokensManager
from metrics.conflict import ConflictManager


class TokensListener():

    def __init__(self, sources, lng, the_page):
        self.sources = sources
        self.lng = lng
        self.the_page = the_page
        
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
        self.token_source['action'] = self.token_source.apply(lambda x: 'oadd' if x['o_rev_id'] == x['rev_id'] else x['action'], axis=1)
        
    def get_editor_names(self):
        #get editor names by editor id
        self.token_source = self.token_source.rename(columns={"editor":'editor_id'})
        self.token_source['editor_id'] = self.token_source['editor_id'].astype(str)
        tokens_merged = self.sources['editors'][['editor_id', 'name']].merge(self.token_source, right_index=True, on='editor_id', how='outer')
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
            url = f"https://{self.lng}.wikipedia.org/w/index.php?&title={self.the_page['title'].replace(' ', '_')}&diff={diff}"
            print('Link to the wikipedia diff: ')
            print(url)
        
    def listen(self, rev_id, stopwords):
        # Get source data through ConflictManager. 
        if stopwords == 'Not included':
            self.token_source = self.sources["cm_exc_stop"].all_actions.copy()

        else:
            self.token_source = self.sources["cm_inc_stop"].all_actions.copy()

        #selected revision id:
        self.rev_id = int(rev_id)
        
        #extract editor name and timestamp to display before the table
        some_rev = self.token_source[self.token_source['rev_id']==self.rev_id]
        if len(some_rev) != 0:
            editor_name = self.sources['editors'].loc[self.sources['editors']['editor_id'] == some_rev['editor'].values[0], 'name'].values[0]
        else:
            return display(md("No tokens in this revision!"))
        timestamp = pd.DatetimeIndex(self.token_source[self.token_source['rev_id']==self.rev_id]['rev_time'])[0]
        display(md(f"***Selected revision: ID: {self.rev_id}, editor name: {str(editor_name)}, timestamp: {str(timestamp.date())} {str(timestamp.time())}***"))
                   
        # Print URL to wikipedia diff.
        url = f"https://{self.lng}.wikipedia.org/w/index.php?title={self.the_page['title']}&diff={self.rev_id}"
        display(HTML(f'<a href="{url}" target="_blank">Click here to see the Wikipedia Text DIFF</a>'))
            
        if  self.rev_id != None:
            #add necessary columns and process the dataframe:
            self.convert_oadd()
            self.get_editor_names()
            self.get_columns()
            self.token_source['time_diff'] = self.token_source['time_diff'].apply(lambda x: TokensListener.convert_time_diff(x))
            
            #sort the dataframe by timestamp and token_id:
            self.token_source.sort_values(['rev_time', 'token_id'], ascending = True, inplace=True)
                   
            #get tokens from the selected revision (from previous and future revisions as well):
            rev_tokens = self.token_source.loc[self.token_source['rev_id'] == self.rev_id, 'token_id'].values
            tokens_for_grid = self.token_source.loc[self.token_source['token_id'].isin(rev_tokens), ['token', 'token_id', 'action', 'rev_id', 'rev_time', 'name', 'o_rev_id', 'reverted_editor', 'time_diff' ]].rename(columns={'token': 'string', 'name': 'editor'})
            
            #convert the format of columns to display:
            tokens_for_grid['rev_id'] = tokens_for_grid['rev_id'].astype(int).astype(str)
            tokens_for_grid['time_diff'] = tokens_for_grid['time_diff'].astype(str)
            tokens_for_grid['token_id'] = tokens_for_grid['token_id'].astype(int).astype(str)
                   
            tokens_for_grid.set_index('token_id', inplace=True)
            self.tokens_for_grid = tokens_for_grid.copy()
                   
            #qgrid widget:
            qgrid_selected_revision = qgrid.show_grid(self.tokens_for_grid)
            self.qgrid_selected_revision = qgrid_selected_revision
            
            #preset filter to display only the selected revision
            self.qgrid_selected_revision._handle_qgrid_msg_helper({
                    'type': 'show_filter_dropdown',
                    'field': 'rev_id',
                    'search_val': str(self.rev_id)
                })
            self.qgrid_selected_revision._handle_qgrid_msg_helper({
                    'field': "rev_id",
                    'filter_info': {
                        'field': "rev_id",
                        'selected': [0],
                        'type': "text",
                        'excluded': []
                    },
                    'type': "change_filter"
                })
            
            display(self.qgrid_selected_revision)
            self.out213 = Output()
            display(self.out213)
            self.qgrid_selected_revision.observe(self.on_selection_change, names=['_selected_rows'])
        else:
            display(md(f'**The selected revision does not exist for this page. Try another**'))
            
            
            
            
            
            
            
            
