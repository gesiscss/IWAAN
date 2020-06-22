import pandas as pd
import plotly
from plotly import graph_objs
from plotly.subplots import make_subplots
from ipywidgets.widgets import Output

from metrics.token import TokensManager


class ActionsListener():

    def __init__(self, sources, editor_column='name'):
        self.tokens_all = sources["tokens_all"]
        self.tokens = sources["tokens"]
        self.tokens_elegible = sources["elegibles"]
        self.page_id = sources["tokens_all"]["page_id"].unique()[0]
        self.editor_column = editor_column
        self.df_plotted = None
        
    
    def get_aggregation(self):
        actions_agg, tokens_inc_stop, tokens_exc_stop = self.get_actions_aggregation()
        elegible_actions, conflict_actions = self.elegibles_conflicts()
        editor_revisions = self.get_revisions()
        elegibles_merge = actions_agg.merge(elegible_actions, on="rev_time",
                            how="left").drop("editor_y", axis=1).rename({"editor_x": "editor"}, axis=1).fillna(0)
        conflicts_merge = elegibles_merge.merge(conflict_actions, on="rev_time", 
                            how="left").drop("editor_y",axis=1).rename({"editor_x": "editor"}, axis=1).fillna(0)
        agg_table = conflicts_merge.merge(editor_revisions, on="rev_time",
                            how="left").drop("editor_y", axis=1).rename({"editor_x": "editor"}, axis=1)
        agg_table = agg_table.sort_values("rev_time").reset_index(drop=True)
        agg_table.insert(2, "page_id", self.page_id)
        
        return agg_table, tokens_inc_stop, tokens_exc_stop
        
    def get_actions_aggregation(self):
        """Include stopwords, also count the stopwords."""
        print("Processing collected tokens...")
        actions_all_dict, tokens_inc_stop = self.aggregation_dicts(self.tokens_all)
        actions_dict, tokens_exc_stop = self.aggregation_dicts(self.tokens)
        
        merge_inc = self.actions_agg(actions_all_dict)
        merge_noinc = self.actions_agg(actions_dict)
        
        comp_cols = ["rev_time", "adds", "dels", "reins"]
        inc_and_noinc = merge_inc[comp_cols].merge(merge_noinc[comp_cols], on="rev_time", how="outer").fillna(0)

        inc_and_noinc["adds_stopword_count"] = inc_and_noinc["adds_x"] - inc_and_noinc["adds_y"]
        inc_and_noinc["dels_stopword_count"] = inc_and_noinc["dels_x"] - inc_and_noinc["dels_y"]
        inc_and_noinc["reins_stopword_count"] = inc_and_noinc["reins_x"] - inc_and_noinc["reins_y"]
        stopword_count = inc_and_noinc[["rev_time", "adds_stopword_count", "dels_stopword_count", "reins_stopword_count"]]

        final_merge = merge_inc.merge(stopword_count, on="rev_time")[["rev_time", "editor",
                                                "adds", "adds_surv_48h", "adds_stopword_count",
                                                "dels", "dels_surv_48h", "dels_stopword_count",
                                                "reins", "reins_surv_48h", "reins_stopword_count",]]
        
        
        return final_merge, tokens_inc_stop, tokens_exc_stop
    
    
    def __group_actions(self, actions):
        actions = actions.reset_index()
        actions["rev_time"] = actions["rev_time"].values.astype("datetime64[s]")
        group_actions = actions.groupby(["rev_time", "editor"]).agg({"token_id": "count"}).reset_index()

        return group_actions


    def aggregation_dicts(self, source):
        # Use TokensManager to analyse.
        token_manager = TokensManager(source)
        adds, dels, reins = token_manager.token_survive()
        
        adds_surv = adds[adds["survive"] == 1]
        dels_surv = dels[dels["survive"] == 1]
        reins_surv = reins[reins["survive"] == 1]

        # Get grouped data.
        grouped = {"adds":adds,
                 "adds_surv_48h":adds_surv,
                 "dels":dels,
                 "dels_surv_48h": dels_surv,
                 "reins":reins,               
                 "reins_surv_48h": reins_surv}
        for key, data in grouped.items():
            grouped[key] = self.__group_actions(data).rename({"token_id": key}, axis=1)
        
        tokenmanager_data = {"adds": adds, "dels": dels, "reins": reins}
        
        return grouped, tokenmanager_data


    def actions_agg(self, dict_for_actions):
        first_key = next(iter(dict_for_actions))
        for key, data in dict_for_actions.items():
            if key != first_key:
                data_to_merge = data_to_merge.merge(data, on="rev_time", how="outer")
                mask_editorx_nan = data_to_merge["editor_x"].isnull()
                data_to_merge.loc[mask_editorx_nan, "editor_x"] = data_to_merge["editor_y"][mask_editorx_nan]
                data_to_merge = data_to_merge.drop("editor_y", axis=1).rename({"editor_x": "editor"}, axis=1).fillna(0)
            else:
                data_to_merge = dict_for_actions[first_key]

        return data_to_merge
        
    
    def elegibles_conflicts(self):
        """Exclude stopwords."""
        # Elegibles and conflict scores (not normalized)
        elegible_actions = self.tokens_elegible.groupby(["rev_time", 
                                       "editor"]).agg({'conflict': 'sum', 
                                                 "action":"count"}).reset_index().rename({"action":"elegibles"}, axis=1)
        elegible_actions["rev_time"] = elegible_actions["rev_time"].values.astype("datetime64[s]")

        # Conflicts
        token_conflict = self.tokens_elegible[~self.tokens_elegible["conflict"].isnull()]
        conflict_actions = token_conflict.groupby(["rev_time", 
                                    "editor"]).agg({"action":"count"}).reset_index().rename({"action":"conflicts"}, axis=1)
        conflict_actions["rev_time"] = conflict_actions["rev_time"].values.astype("datetime64[s]")
        
        return elegible_actions, conflict_actions
    
    
    def get_revisions(self):
        """Include stopwords."""
        editor_revisions = self.tokens_all.groupby(["rev_time", 
                                    "editor"]).agg({'rev_id': 'nunique'}).reset_index().rename({"rev_id":"revisions"}, axis=1)
        editor_revisions["rev_time"] = editor_revisions["rev_time"].values.astype("datetime64[s]")
        
        return editor_revisions

    
    def prelisten(self, df_for_listen):
        self.df = df_for_listen
    
    
    def listen(self, _range1, _range2, editor, granularity,
               black, red, blue, green, black_conflict, red_conflict):
        
        df = self.df[(self.df.rev_time.dt.date >= _range1) &
                (self.df.rev_time.dt.date <= _range2)]
        
        df_conflict = df.groupby(pd.Grouper(
            key='rev_time', freq=granularity[0])).agg({'conflicts': ['sum'],
                                       'elegibles': ['sum'],
                                       'revisions': ['sum'],
                                       'conflict': ['count', 'sum']}).reset_index()
        self.traces = {}
        df_conflict = self.__add_trace(df_conflict, black_conflict, 'rgba(0, 0, 0, 1)')
        df_conflict = self.__add_trace(df_conflict, red_conflict, 'rgba(255, 0, 0, .8)')
        

        if editor != 'All':
            df = df[df[self.editor_column] == editor]
            
        if (granularity[0] == "D") or (granularity[0] == "W"):
            df = df.groupby(pd.Grouper(
                key='rev_time', freq=granularity[0])).sum().reset_index()
        else:
            df = df.groupby(pd.Grouper(
                key='rev_time', freq=granularity[0]+'S')).sum().reset_index()
        
        fig = make_subplots(rows=2, cols=1, start_cell="bottom-left", shared_xaxes=True, vertical_spacing=0.05)
        
        fig.add_trace(graph_objs.Scatter(
                x=df['rev_time'], y=df[black],
                name=black,
                marker=dict(color='rgba(0, 0, 0, 1)')), row=2, col=1)

        if red != 'None':
            fig.add_trace(graph_objs.Scatter(
                x=df['rev_time'], y=df[red],
                name=red,
                marker=dict(color='rgba(255, 0, 0, .8)')), row=2, col=1)

        if blue != 'None':
            fig.add_trace(graph_objs.Scatter(
                x=df['rev_time'], y=df[blue],
                name=blue,
                marker=dict(color='rgba(0, 153, 255, .8)')), row=2, col=1)          

        if green != 'None':
            fig.add_trace(graph_objs.Scatter(
                x=df['rev_time'], y=df[green],
                name=green,
                marker=dict(color='rgba(0, 128, 43, 1)')), row=2, col=1)
            
        if black_conflict != "None":
            fig.add_trace(self.traces[black_conflict], row=1, col=1)
            
        if red_conflict != "None":
            fig.add_trace(self.traces[red_conflict], row=1, col=1)

        self.df_plotted = df
        self.df_conflict_plot = df_conflict
        
        fig.update_yaxes(title_text="Actions", row=2, col=1)
        fig.update_yaxes(title_text="Conflict Scores", row=1, col=1)
        fig.update_layout(
            hovermode='closest',
            xaxis=dict(title=granularity, ticklen=5, zeroline=True, gridwidth=2),
            #yaxis=dict(title='Actions', ticklen=5, gridwidth=2),
            legend=dict(x=0.5, y=1.2),
            showlegend=True, 
            barmode='group',
            height=600,
            legend_orientation="h"
          )
        
        fig.show()
        
        
        
    
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

        elif metric == 'Absolute Conflict Score':
            df['absolute_conflict_score'] = df[('conflict', 'sum')]
            sel = ~df['absolute_conflict_score'].isnull() 
            y = df.loc[sel, 'absolute_conflict_score']
            self.is_norm_scale = False

        elif metric == 'Total Elegible Actions':
            df['elegibles_n'] = df[('elegibles', 'sum')]
            sel = df['elegibles_n'] != 0
            y = df.loc[sel, 'elegibles_n']
            self.is_norm_scale = False
        
        self.traces[metric] = graph_objs.Bar(
            x=df.loc[sel,'rev_time'], y=y,
            name=metric, marker_color=color
        )

        return df