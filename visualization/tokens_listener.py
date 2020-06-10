import copy
import qgrid
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from IPython.display import display, Markdown as md, clear_output
from ipywidgets import Output, fixed
from .wordclouder import WordClouder

from metrics.token import TokensManager
from metrics.conflict import ConflictManager


class TokensListener():

    def __init__(self, sources, lng):
        self.sources = sources
        self.lng = lng
        
    def calculate_time_diff(self):
        self.token_source['time_diff'] = self.token_source['rev_time'] - self.token_source.shift(1)['rev_time']
        to_delete = (
                 #First row of each token
                 (self.token_source['o_rev_id'] == self.token_source['rev_id']))

            # delete but keep the row
        self.token_source.loc[to_delete, 'time_diff'] = np.nan

    def convert_oadd(self):
        self.token_source['action'] = self.token_source.apply(lambda x: 'oadd' if x['o_rev_id'] == x['rev_id'] else x['action'], axis=1)
        
    def get_editor_names(self):
        #editor names
        self.token_source = self.token_source.rename(columns={"editor":'editor_id'})
        self.sources['editors']['editor_id'] = self.sources['editors']['editor_id'].astype(str)
        self.token_source['editor_id'] = self.token_source['editor_id'].astype(str)
        tokens_merged = self.sources['editors'][['editor_id', 'name']].merge(self.token_source, right_index=True, on='editor_id', how='outer')
        self.token_source = tokens_merged[tokens_merged['token'].notnull()].copy()
        
    def convert_time_diff(time_diff):
        try:
            s = time_diff.seconds
            hours, remainder = divmod(s, 3600)
            minutes, seconds = divmod(remainder, 60)
            return '{:02}:{:02}:{:02}:{}'.format(int(time_diff.days), int(hours), int(minutes), int(seconds))
        except ValueError:
            return None
        
    def listen(self, rev_id, stopwords):
        # Get source data through ConflictManager. 
        if stopwords == 'Not included':
            self.token_source = self.sources["cm_exc_stop"].all_actions.copy()
#             self.add_actions = self.sources["tokens_exc_stop"]["adds"]
#             self.del_actions = self.sources["tokens_exc_stop"]["dels"]
#             self.rein_actions = self.sources["tokens_exc_stop"]["reins"]

        else:

            self.token_source = self.sources["cm_inc_stop"].all_actions.copy()
#             self.add_actions = self.sources["tokens_inc_stop"]["adds"]
#             self.del_actions = self.sources["tokens_inc_stop"]["dels"]
#             self.rein_actions = self.sources["tokens_inc_stop"]["reins"]

        self.rev_id = int(rev_id)
            
        if  self.rev_id != None:
            self.calculate_time_diff()
            self.convert_oadd()
            self.get_editor_names()
            self.token_source['time_diff'] = self.token_source['time_diff'].apply(lambda x: TokensListener.convert_time_diff(x))
            
            self.token_source.sort_values(['rev_time', 'token_id'], ascending = True, inplace=True)
            rev_tokens = self.token_source.loc[self.token_source['rev_id'] == self.rev_id, 'token_id'].values
            tokens_for_grid = self.token_source.loc[self.token_source['token_id'].isin(rev_tokens), ['token', 'token_id', 'action', 'rev_id', 'rev_time', 'name', 'o_rev_id', 'time_diff' ]].rename(columns={'token': 'string', 'name': 'editor'})
            
            tokens_for_grid['rev_id'] = tokens_for_grid['rev_id'].astype(int).astype(str)
            tokens_for_grid['time_diff'] = tokens_for_grid['time_diff'].astype(str)
            tokens_for_grid.set_index('token_id', inplace=True)
            self.tokens_for_grid = tokens_for_grid.copy()
            qgrid_selected_revision = qgrid.show_grid(self.tokens_for_grid)
            display(qgrid_selected_revision)
            self.out = Output()
            display(self.out)
            
            
            
            
            
            
            
            
