import pandas as pd
import numpy as np
import qgrid
import plotly
from plotly import graph_objs
from IPython.display import display, Markdown as md, clear_output, HTML
from ipywidgets import Output
from utils.notebooks import get_previous_notebook

from metrics.conflict import ConflictManager

import calendar
from datetime import date


class ConflictsListener():

    def __init__(self, df, bargap=None):

        # time diff to seconds
        #df['diff_secs'] = df['time_diff'].dt.total_seconds()

        # conflict time diff to seconds 
        #df['diff_secs_confl'] = np.nan
        #df['diff_secs_confl'] = df.loc[~df['conflict'].isnull(), 'diff_secs']

        self.df = df
        self.df_plotted = None
        self.bargap = bargap

    def listen(self, _range1, _range2, granularity, black, red):
        df = self.df

        df = df[(df.year_month.dt.date >= _range1) &
                (df.year_month.dt.date <= _range2)]

        # calculate the aggreated values
        df = df.groupby(pd.Grouper(
            key='year_month', freq=granularity[0])).agg({'conflicts': ['sum'],
                                                       'elegibles': ['sum'],
                                                       'revisions': ['sum'],
                                                       'conflict': ['count', 'sum'],
                                                        'total': ['sum'],
                                                        'total_surv_48h': ['sum'],
                                                        'total_stopword_count': ['sum']}).reset_index()

        df.loc[df[('conflict', 'count')] == 0, ('conflict', 'sum')] = np.nan
        #df.loc[df[('conflicts', 'count')] == 0, ('diff_secs', 'sum')] = np.nan

        self.traces = []
        self.is_norm_scale = True
        df = self.__add_trace(df, black, 'rgba(0, 0, 0, 1)')
        df = self.__add_trace(df, red, 'rgba(255, 0, 0, .8)')

        #np.all(np.array([len(sc.x) == 1 for sc in self.traces]))

        _range = None
        if self.is_norm_scale:
            _range = [0, 1]

        # if red != 'None':
        #     data.append(graph_objs.Scatter(
        #         x=list(df['rev_time']), y=list(df[red]),
        #         name=red,
        #         marker=dict(color='rgba(255, 0, 0, .8)')))

        # if blue != 'None':
        #     data.append(graph_objs.Scatter(
        #         x=list(df['rev_time']), y=list(df[blue]),
        #         name=blue,
        #         marker=dict(color='rgba(0, 128, 43, 1)')))

        # if green != 'None':
        #     data.append(graph_objs.Scatter(
        #         x=list(df['rev_time']), y=list(df[green]),
        #         name=green,
        #         marker=dict(color='rgba(0, 153, 255, .8)')))
        
        if self.bargap == None:
            layout = graph_objs.Layout(hovermode='closest',
                                       xaxis=dict(title=granularity, ticklen=5,
                                                  zeroline=True, gridwidth=2),
                                       yaxis=dict(
                                           ticklen=5, gridwidth=2, range=_range),
                                       legend=dict(x=0.5, y=1.2),
                                       showlegend=True, barmode='group')
        else:
            layout = graph_objs.Layout(hovermode='closest',
                                       xaxis=dict(title=granularity, ticklen=5,
                                                  zeroline=True, gridwidth=2),
                                       yaxis=dict(
                                           ticklen=5, gridwidth=2, range=_range),
                                       legend=dict(x=0.5, y=1.2),
                                       showlegend=True, barmode='group', bargap=self.bargap)

        self.df_plotted = df

        plotly.offline.init_notebook_mode(connected=True)
        plotly.offline.iplot({"data": self.traces, "layout": layout})

    def __add_trace(self, df, metric, color):
        sel = df.index
        if metric == 'None':
            return df
        elif metric == 'Conflict Score':
            df['conflict_score'] = df[
                ('conflict', 'sum')] / df[('elegibles', 'sum')]
            sel = ~df['conflict_score'].isnull()
            y = df.loc[sel, 'conflict_score']
            self.is_norm_scale = False

        elif metric == 'Conflict Ratio':
            df['conflict_ratio'] = df[
                ('conflicts', 'sum')] / df[('elegibles', 'sum')]
            sel = ~(df['conflict_ratio'].isnull() |  (df[('conflict', 'count')] == 0))
            y = df.loc[sel, 'conflict_ratio']

        elif metric == 'Absolute Conflict Score':
            df['absolute_conflict_score'] = df[('conflict', 'sum')]
            sel = ~df['absolute_conflict_score'].isnull() 
            y = df.loc[sel, 'absolute_conflict_score']
            self.is_norm_scale = False

        elif metric == 'Number of Conflicts':
            df['conflict_n'] = df[('conflicts', 'sum')]
            sel = df['conflict_n'] != 0
            y = df.loc[sel, 'conflict_n']
            self.is_norm_scale = False

        elif metric == 'Total':
            df['total_n'] = df[('total', 'sum')]
            sel = df['total_n'] != 0
            y = df.loc[sel, 'total_n']
            self.is_norm_scale = False
            
        elif metric == 'Total_surv_48h':
            df['total_surv_48h_n'] = df[('total_surv_48h', 'sum')]
            sel = df['total_surv_48h_n'] != 0
            y = df.loc[sel, 'total_surv_48h_n']
            self.is_norm_scale = False

        elif metric == 'Total_persistent':
            df['total_persistent_n'] = df[('total_persistent', 'sum')]
            sel = df['total_persistent_n'] != 0
            y = df.loc[sel, 'total_persistent_n']
            self.is_norm_scale = False
            
        elif metric == 'Total_stopword_count':
            df['total_stopword_count_n'] = df[('total_stopword_count', 'sum')]
            sel = df['total_stopword_count_n'] != 0
            y = df.loc[sel, 'total_stopword_count_n']
            self.is_norm_scale = False

        elif metric == 'Total Elegible Actions':
            df['elegibles_n'] = df[('elegibles', 'sum')]
            sel = df['elegibles_n'] != 0
            y = df.loc[sel, 'elegibles_n']
            self.is_norm_scale = False

#         self.traces.append(
#             graph_objs.Scatter(
#                 x=df.loc[sel,'year_month'], y=y,
#                 name=metric,
#                 marker=dict(color=color), mode='markers')
#         )

        self.traces.append(
            graph_objs.Bar(
                x=df.loc[sel,'year_month'], y=y,
                name=metric, marker_color=color)
        )

        return df
    
    
    
class ConflictsActionListener():
    
    def __init__(self, sources, lng, the_page):
        self.sources=sources
        self.lng = lng
        self.the_page = the_page
        
    def on_selection_change(self, change):
        with self.out21:
            clear_output()

            # Extract the rev_id selected and convert it to string.
            diff = self.qgrid_token_obj.get_selected_df().reset_index()['rev_id'].iloc[0]      
            
            # Print URL.
            url = f"https://{self.lng}.wikipedia.org/w/index.php?&title={self.the_page['title'].replace(' ', '_')}&diff={diff}"
            print('Link to the wikipedia diff: ')
            print(url)
        
    def add_columns(self):
        #time_diff_secs
        self.conflicts['time_diff_secs'] = self.conflicts['time_diff'].dt.total_seconds()
        #editor names
        self.conflicts = self.conflicts.rename(columns={"editor":'editor_id'})
        self.sources['Editors']['editor_id'] = self.sources['Editors']['editor_id'].astype(str)
        self.conflicts['editor_id'] = self.conflicts['editor_id'].astype(str)
        conflicts_merged = self.sources['Editors'][['editor_id', 'name']].merge(self.conflicts, right_index=True, on='editor_id', how='outer')
        self.conflicts = conflicts_merged[conflicts_merged['token'].notnull()].copy()
        
        #filling values for original insertions
        original = pd.merge(self.conflicts[self.conflicts['rev_id'] == -1], self.sources['Revisions'][['rev_time', 'rev_id']],how='left', left_on='o_rev_id', right_on='rev_id')
        self.conflicts.loc[self.conflicts['rev_id'] == -1, 'rev_time'] = original['rev_time_y'].tolist()
        self.conflicts.loc[self.conflicts['rev_id'] == -1, 'editor_id'] = self.conflicts.loc[self.conflicts['rev_id'] == -1, 'o_editor']
        self.conflicts.loc[self.conflicts['rev_id'] == -1, 'time_diff'] = 0
        self.conflicts.loc[self.conflicts['rev_id'] == -1, 'time_diff_secs'] = 0
        
        #order and count columns
        #self.conflicts.reset_index(inplace=True)
        counts = self.conflicts[['token_id', 'token']].groupby(['token_id']).count()
        #count column
        self.conflicts = counts.merge(self.conflicts, right_index=True, on='token_id', how='outer').rename(columns = {'token_x':'count','token_y':'token'}).reset_index()
        #order column
        g = self.conflicts.sort_values(['time_diff_secs']).groupby('token_id', as_index=False)
        self.conflicts['order'] = g.cumcount()
        self.conflicts['order'] = self.conflicts['order'].apply(lambda x: x + 1)

#         for token_id in self.conflicts['token_id'].unique():
#             token_df = self.conflicts.loc[self.conflicts['token_id'] == token_id].sort_values(by='time_diff_secs').copy()
#             self.conflicts.loc[token_df.index, 'order'] = list(range(1, len(token_df)+1))
#             self.conflicts.loc[token_df.index, 'count'] = len(token_df)
        
        
    
    def listen(self, stopwords, _range1, _range2):
        if stopwords == 'Not included':
            cp_all = self.sources['All content'].copy()
            cp_revisions = self.sources['Revisions'].copy()
            conflict_calculator = ConflictManager(cp_all, cp_revisions, lng=self.lng)
        else:
            cp_all = self.sources['All content'].copy()
            cp_revisions = self.sources['Revisions'].copy()
            conflict_calculator = ConflictManager(cp_all, cp_revisions, lng=self.lng, include_stopwords=True)
            
        conflict_calculator.calculate()
        self.conflict_calculator = conflict_calculator
        clear_output()

        # display the tokens, the difference in seconds and its corresponding conflict score
        self.conflicts = conflict_calculator.conflicts.copy()
        self.add_columns()

        if len(self.conflicts) > 0:
            conflicts_for_grid = self.conflicts[[
                'order', 'count', 'action',  'token', 'token_id', 'conflict', 'rev_time', 'name', 'editor_id', 'time_diff_secs','rev_id']].rename(columns={'token': 'string', 'rev_time':'timestamp', 'name':'editor_name'}).sort_values('conflict', ascending=False)
            conflicts_for_grid['timestamp'] = pd.to_datetime(conflicts_for_grid['timestamp'], cache=False, utc=True).dt.date
            conflicts_for_grid = conflicts_for_grid[(conflicts_for_grid.timestamp >= _range1) &
                (conflicts_for_grid.timestamp <= _range2)]
            conflicts_for_grid['token_id'] = conflicts_for_grid['token_id'].astype(int).astype(str)
            conflicts_for_grid['rev_id'] = conflicts_for_grid['rev_id'].astype(int).astype(str)
            conflicts_for_grid['editor_id'] = conflicts_for_grid['editor_id'].astype(str)
            conflicts_for_grid.set_index('token_id', inplace=True)
            self.df_for_grid = conflicts_for_grid.loc[conflicts_for_grid['string']!='<!--'].copy()
            qgrid_token_obj = qgrid.show_grid(self.df_for_grid,grid_options={'forceFitColumns':False})
            self.qgrid_token_obj = qgrid_token_obj
            display(self.qgrid_token_obj)
            self.out21 = Output()
            display(self.out21)
            self.qgrid_token_obj.observe(self.on_selection_change, names=['_selected_rows'])
            
        else:
            display(md(f'**There are no conflicting tokens in this page.**'))
            display(HTML(f'<a href="{get_previous_notebook()}" target="_blank">Go back to the previous workbook</a>'))
            
            
class ConflictsEditorListener():
    
    def __init__(self, conflict_manager, editor_names):
        self.conflict_manager = conflict_manager
        self.conflict_manager.calculate()
        clear_output()        
        self.token_source = self.conflict_manager.all_actions
        self.token_conflict = self.conflict_manager.conflicts
        self.token_elegible = self.conflict_manager.elegible_actions
        self.editor_names = editor_names
        
        self.qg_obj = None
        self.out = Output()
        
    
    def __change_date(self, old_date):
        new_date = pd.Timestamp(old_date.year, old_date.month, 1)
    
        return new_date
    
    
    def get_editor_month(self):
        elegible_no_init = self.token_elegible.copy()
        all_actions = self.token_source.copy()
        
        #marking reinsertions
        all_actions.loc[(all_actions['o_rev_id'].values != all_actions['rev_id'].values) & (all_actions['action'] != 'out'),'action'] = 'rein'
        dummies = pd.get_dummies(all_actions['action'], prefix='action')
        all_actions = pd.concat([all_actions, dummies], axis=1)
        
        elegible_no_init["rev_time"] = elegible_no_init["rev_time"].apply(lambda x: self.__change_date(x))
        all_actions["rev_time"] = all_actions["rev_time"].apply(lambda x: self.__change_date(x))
        
        # Classify conflicts
        conflict_agg = elegible_no_init.groupby(["rev_time", "editor"]).agg({'conflict': 'sum'}).reset_index().rename({"editor": "editor_id"}, axis=1)
        
        all_actions_agg = all_actions.groupby(["rev_time", "editor"]).agg({"action":"count", "action_in":"sum", "action_rein":"count", "action_out":"sum"})
        
        editor_group = pd.merge(conflict_agg, all_actions_agg,  how='left', left_on=['rev_time', 'editor_id'], right_on = ['rev_time', 'editor']).rename({"action_in":"additions", "action_out": "deletions", "action_rein": "reinsertions"}, axis=1)


        
        # Merge to new table
        editor_names = self.editor_names.copy()
        editor_names["editor_id"] = editor_names["editor_id"].astype(str)
        editor_month_conf = editor_names[['editor_id', 'name']].merge(editor_group, right_index=True,how="right", 
                                                           on='editor_id').sort_values("rev_time").set_index("rev_time")
        editor_month_conf["conflict"] = editor_month_conf["conflict"] / editor_month_conf["action"]
        editor_month_conf = editor_month_conf[editor_month_conf["conflict"] != 0]
        editor_month_conf.drop("action", axis=1, inplace=True)
        
        return editor_month_conf
    
    
    def __count_in_out(self, df):
        mask_out = (df["action"] == "out").astype(int)
        mask_in = (df["action"] == "in").astype(int)
        df["in_action"] = mask_in
        df["out_action"] = mask_out

        count_list = []
        for tokenid in df["token_id"].unique():
            df_token = df[df["token_id"] == tokenid]
            df_count = pd.DataFrame(data={"token_id": tokenid,
                                    "in_actions": df_token["in_action"].sum(),
                                    "out_actions": df_token["out_action"].sum()}, index=[0]).set_index("token_id")
            count_list.append(df_count)
        
        return pd.concat(count_list)
    
    
    def __date_editor_filter(self, df, year_month, editor_id=None):
        year, month = year_month
        selected_time_start = date(year, month, 1)
        selected_time_end = date(year, month, calendar.monthrange(year,month)[1])

        mask_date = (selected_time_start <= df["rev_time"].dt.date) & (selected_time_end >= df["rev_time"].dt.date)
        if editor_id != None:
            mask_editor = df["editor"] == editor_id
        else:
            mask_editor = mask_date

        return mask_date & mask_editor
    
    
    def __get_main_opponent(self, editor_id, token_indices, editor_dict):
        conflict_actions = self.conflict_manager.get_conflicting_actions(editor_id)

        main_opponents = []
        for token_id in token_indices:
            conflict_token = conflict_actions[conflict_actions["token_id"] == token_id].sort_values("conflict", ascending=False)
            main_opponents.append(conflict_token.iloc[[0]][["token_id", "editor"]].set_index("token_id"))

        main_opponents = pd.concat(main_opponents)
        main_opponents.replace((editor_dict), inplace=True)

        return main_opponents
    
    
    def get_tokens(self, df_selected):
        with self.out:
            clear_output()
            editor_id = df_selected["editor_id"].values[0]
            year_and_month = (df_selected.index[0].year, df_selected.index[0].month)
            display(md(f"In **{year_and_month[1]}.{year_and_month[0]}** you have selected the editor **{df_selected['name'].values[0]}**"))

            # All actions.
            selected_source_tokens = self.token_source.loc[self.__date_editor_filter(self.token_source, 
                                                             year_and_month, editor_id)].reset_index(drop=True)

            # Conflicts.
            selected_conflict_tokens = self.token_conflict.loc[self.__date_editor_filter(self.token_conflict, 
                                                            year_and_month, editor_id)].reset_index(drop=True)

            # Elegibles.
            selected_elegible_tokens = self.token_elegible.loc[self.__date_editor_filter(self.token_elegible, 
                                                        year_and_month, editor_id)].reset_index(drop=True)

            # Classification and merge.
            selected_source = selected_source_tokens.groupby(["token_id"]).agg({"rev_id": "count"}).rename({"rev_id": "revisions"}, axis=1)
            selected_conflicts = selected_conflict_tokens.groupby(["token_id"]).agg({"action": "count", 
                                                             "conflict": "sum"}).rename({"action": "conflicts"}, axis=1)

            selected_elegibles = selected_elegible_tokens.groupby(["token_id"]).agg({"action": "count"}).rename({"action": "elegibles"}, axis=1)

            selected_elegibles = selected_elegibles.merge(selected_source, on="token_id")

            in_out = self.__count_in_out(selected_source_tokens)
            selected_elegibles = selected_elegibles.merge(in_out, on="token_id")

            selected_df = selected_conflicts.merge(selected_elegibles, on="token_id", how="right")
            selected_df["conflict"] = selected_df["conflict"] / selected_df["elegibles"]
            selected_df = selected_df.fillna(0)
            selected_df = selected_df.merge(selected_elegible_tokens[["token_id", "token"]].drop_duplicates().set_index("token_id"), 
                            on="token_id")[["token", "elegibles", "conflicts", "conflict", "revisions", "in_actions", "out_actions"]]
            selected_df = selected_df[selected_df["conflict"] != 0]

            # Find the main opponent for each token.
            editor_to_id = self.get_editor_month()[["editor_id", "name"]]
            editor_id_dict = dict(zip(editor_to_id["editor_id"], editor_to_id["name"]))
            for k, v in editor_id_dict.items():
                if str(v) == "nan":
                    editor_id_dict[k] = k
                else:
                    pass

            main_opponent = self.__get_main_opponent(editor_id=editor_id, token_indices=selected_df.index, editor_dict=editor_id_dict)
            selected_df = selected_df.merge(main_opponent, on="token_id").rename({"editor": "main_opponent", "token": "string"}, axis=1)

            display(qgrid.show_grid(selected_df[selected_df["string"] != "<!--"]))
           
    
    def on_selection_change(self, change):
        self.get_tokens(self.qg_obj.get_selected_df())
    
    
    def listen(self):
        main_df = self.get_editor_month()
        main_df.index.name = "year_month"
        
        
        self.qg_obj = qgrid.show_grid(main_df)
        display(self.qg_obj)
        display(self.out)
        self.qg_obj.observe(self.on_selection_change, names=['_selected_rows'])
        
        
