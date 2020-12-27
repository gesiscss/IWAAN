import pandas as pd
import plotly
from plotly import graph_objs
from plotly.subplots import make_subplots
from ipywidgets.widgets import Output

from metrics.token import TokensManager

from pandas.tseries.offsets import MonthEnd
import operator
import numpy as np


class ActionsListener():
    """
    Class to aggregate actions for each revision. Directly based on WikiWho data.
    ...
    Parameters:
    -----------
    sources (dict): stores all actions and elegible actions (incl. stopwords)
            got from ConflictManager. Also a WikipediaDV object to extract
            editors' names according to their ids.
    ...
    Attributes:
    -----------
    tokens_all (pd.DataFrame): All actions incl. stopwords.
    tokens_elegibles_all (pd.DataFrame): All elegible actions incl. stopwords.
    wikidv (object): WikipediaDV object.
    page_id (np.int64): Page id.
    editor_column (str): column name storing editors' names.
    df_plotted (pd.DataFrame): In listener(), dataframe for final plotting.
    lng (str): language from {'en', 'de'}.
    df (pd.DataFrame): In get_main(), agg_actions.
    tokens_group_all (dict): In get_actions_aggregation(), dict storing
                add/del/reins got from TokensManager.get_tokens_states()
                method, incl. stopwords.
    tokens_group (dict): stopwords in tokens_group_all are removed.
    """
    def __init__(self, sources, lng, editor_column='name'):
        self.tokens_all = sources["tokens_all"]
        self.tokens_elegibles_all = sources["elegibles_all"]
        self.wikidv = sources["wiki_dv"]
        self.page_id = sources["tokens_all"]["page_id"].unique()[0]
        self.editor_column = editor_column
        self.lng = lng
        self.ores_scores = None
        
    def get_main(self):
        """Run this method before run listener.
        ...
        Returns:
        --------
        Set the attribute self.df.
        """
        rough_agg = self._get_aggregation()
        
        agg_columns = ['total', 'total_surv_48h', 'total_stopword_count']
        agg_actions = rough_agg.join(pd.DataFrame(
            rough_agg.loc[:,'adds':'adds_stopword_count'].values +\
            rough_agg.loc[:,'dels':'dels_stopword_count'].values +\
            rough_agg.loc[:,'reins':'reins_stopword_count'].values, 
            index=rough_agg.index, 
            columns=agg_columns))
        
        # Editor names as string.
        agg_actions.insert(2, "editor_str", agg_actions["editor"].copy())
        
        # Convert editor's id to int
        agg_actions["editor"] = agg_actions["editor"].apply(lambda x: self._str2int(x))
        agg_actions = agg_actions.rename({"editor": "editor_id"}, axis=1)
        
        # Grab user names using WikipediaDV.
        print("Downloading editor usernames...")
        self.editors = self.wikidv.get_editors(agg_actions['editor_id'].unique()).rename(columns = {'userid': 'editor_id'})
        
        # Merge the names of the editors to the aggregated actions dataframe.
        agg_actions = agg_actions.merge(self.editors[['editor_id', 'name']], on='editor_id')
        agg_actions.insert(3, 'editor', agg_actions['name'])
        agg_actions = agg_actions.drop(columns=['name'])
        agg_actions['editor'] = agg_actions['editor'].fillna("Unregistered")
        
        # Convert to datetime
        agg_actions['rev_time'] = pd.to_datetime(agg_actions['rev_time'])

        
        self.df = agg_actions
           
    def _get_aggregation(self):
        """
        Merge most aggregation data together.
        ...
        Returns:
        --------
        agg_table (pd.DataFrame): Aggregation data of adds/adds_surv_48h/.../reins_surv_48h/conflicts
                        /elegibles/conflict scores
        """
        actions_agg = self._get_actions_aggregation()
        elegible_actions, conflict_actions = self._elegibles_conflicts()
        editor_revisions = self._get_revisions()
        elegibles_merge = actions_agg.merge(elegible_actions, on="rev_time",
                            how="left").drop("editor_y", axis=1).rename({"editor_x": "editor"}, axis=1).fillna(0)
        conflicts_merge = elegibles_merge.merge(conflict_actions, on="rev_time", 
                            how="left").drop("editor_y",axis=1).rename({"editor_x": "editor"}, axis=1).fillna(0)
        agg_table = conflicts_merge.merge(editor_revisions, on="rev_time",
                            how="left").drop("editor_y", axis=1).rename({"editor_x": "editor"}, axis=1)
        agg_table = agg_table.sort_values("rev_time").reset_index(drop=True)
        agg_table.insert(2, "page_id", self.page_id)
        
        return agg_table

    def _get_tokens_states(self, source):
        """Called in get_actions_aggregation(). Use TokensManager to analyse tokens.
        ...
        Parameters:
        -----------
        source (pd.DataFrame): all actions incl. stopwords
        ...
        Returns:
        -----------
        adds, dels, reins (pd.DataFrame): output of TokensManager.token_survive().
        """
        token_manager = TokensManager(source)
        adds, dels, reins = token_manager.token_survive(reduce=True)
        
        return adds, dels, reins
    
    def _group_actions(self, actions):
        "Called in _aggregation_dicts(). Count actions."
        actions = actions.reset_index()
        actions["rev_time"] = actions["rev_time"].values.astype("datetime64[s]")
        group_actions = actions.groupby(["rev_time", "editor"]).agg({"token_id": "count"}).reset_index()

        return group_actions
    
    def _aggregation_dicts(self, tokens_dict):
        """Called in get_actions_aggregation()."""
        # Store adds/adds_surv_48h/dels/dels_surv_48h/reins/reins_surv_48h in a dict.
        grouped = {"adds":tokens_dict["adds"],
                 "adds_surv_48h":tokens_dict["adds"][tokens_dict["adds"]["survive"] == 1],
                 "dels":tokens_dict["dels"],
                 "dels_surv_48h": tokens_dict["dels"][tokens_dict["dels"]["survive"] == 1],
                 "reins":tokens_dict["reins"],               
                 "reins_surv_48h": tokens_dict["reins"][tokens_dict["reins"]["survive"] == 1]}
        
        # Aggregate data.
        for key, data in grouped.items():
            grouped[key] = self._group_actions(data).rename({"token_id": key}, axis=1)
        
        return grouped


    def _actions_agg(self, dict_for_actions):
        """Called in get_actions_aggregation()."""
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
    
    def _get_actions_aggregation(self):
        """Called in get_aggregation(). Get aggregation data in terms of action type,
        i.e. the columns adds, adds_surv_48h, adds_stopword_count, ..., reins_stopword_count.
        ...
        Returns:
        --------
        final_merge (pd.DataFrame): aggregation data in terms of action type.
        """
        print("Processing collected tokens...")
        # Sort actions by their types and get the states of them, using TokensManager.
        self.tokens_group_all = {}
        self.tokens_group_all["adds"], self.tokens_group_all["dels"], self.tokens_group_all["reins"] = self._get_tokens_states(self.tokens_all)
        
        # Remove all stopwords.
        self.tokens_group = self._remove_stopwords(self.tokens_group_all)
        
        # Count adds/adds_surv_48h/.../reins_surv_48h of each revision, incl.stopwords and excl.stopwords.
        actions_all_dict = self._aggregation_dicts(self.tokens_group_all)
        actions_dict = self._aggregation_dicts(self.tokens_group)
        
        # Merge df incl.sw and df excl.sw together.
        merge_inc = self._actions_agg(actions_all_dict)
        merge_noinc = self._actions_agg(actions_dict)
        comp_cols = ["rev_time", "adds", "dels", "reins"]
        inc_and_noinc = merge_inc[comp_cols].merge(merge_noinc[comp_cols], on="rev_time", how="outer").fillna(0)
        
        # Count stopwords.
        inc_and_noinc["adds_stopword_count"] = inc_and_noinc["adds_x"] - inc_and_noinc["adds_y"]
        inc_and_noinc["dels_stopword_count"] = inc_and_noinc["dels_x"] - inc_and_noinc["dels_y"]
        inc_and_noinc["reins_stopword_count"] = inc_and_noinc["reins_x"] - inc_and_noinc["reins_y"]
        stopword_count = inc_and_noinc[["rev_time", "adds_stopword_count", "dels_stopword_count", "reins_stopword_count"]]
        
        # Do the final merge.
        final_merge = merge_inc.merge(stopword_count, on="rev_time")[["rev_time", "editor",
                                                "adds", "adds_surv_48h", "adds_stopword_count",
                                                "dels", "dels_surv_48h", "dels_stopword_count",
                                                "reins", "reins_surv_48h", "reins_stopword_count",]]
                
        return final_merge        
    
    def _elegibles_conflicts(self):
        """Called in get_aggregation().
        Get elegibles, conflicts and conflict score.
        ...
        Returns:
        --------
        elegibles_actions (pd.DataFrame): Elegible actions and conflict scores
        conflict_actions (pd.DataFrame): Conflict actions.
        """
        # Elegibles and conflict scores (not normalized)
        elegible_no_stopwords = self._remove_stopwords(self.tokens_elegibles_all)  # First remove stopwords.
        elegible_actions = elegible_no_stopwords.groupby(["rev_time", 
                                       "editor"]).agg({'conflict': 'sum', 
                                                 "action":"count"}).reset_index().rename({"action":"elegibles"}, axis=1)
        elegible_actions["rev_time"] = elegible_actions["rev_time"].values.astype("datetime64[s]")

        # Count conflicts.
        token_conflict = elegible_no_stopwords[~elegible_no_stopwords["conflict"].isnull()]
        conflict_actions = token_conflict.groupby(["rev_time", 
                                    "editor"]).agg({"action":"count"}).reset_index().rename({"action":"conflicts"}, axis=1)
        conflict_actions["rev_time"] = conflict_actions["rev_time"].values.astype("datetime64[s]")
        
        return elegible_actions, conflict_actions
        
    def _get_revisions(self):
        """Called in get_aggregation(). Count revisions.
        ...
        Returns:
        --------
        editor_revisions (pd.DataFrame): |rev_time|editor|revisions|
                       2005-03-22 18:46:14|162969|   1   |
                       ...
        """
        editor_revisions = self.tokens_all.groupby(["rev_time","editor"]).agg({'rev_id': ['nunique', 'min']})
        editor_revisions.columns = [' '.join(col).strip() for col in editor_revisions.columns.values]
        editor_revisions = editor_revisions.reset_index().rename({"rev_id nunique":"revisions", "rev_id min": "rev_id"}, axis=1)
        editor_revisions["rev_time"] = editor_revisions["rev_time"].values.astype("datetime64[s]")
        
        return editor_revisions
    
    def _str2int(self, string):
        "Called in get_main()."
        try:
            integer = int(string)
        except:
            integer = 0

        return integer
    
    def _remove_stopwords(self, actions):
        """Called in get_actions_aggregation(). 
        Open a list of stopwords and remove them from the dataframe.
        ...
        Parameters:
        -----------
        actions (Union[pd.DataFrame, dict])
        """
        if self.lng == 'en':
            stopwords_fn='data/stopword_list.txt'
        elif self.lng == 'de':
            stopwords_fn='data/stopword_list_de.txt'
        else:
            stopwords_fn='data/stopword_list.txt'
            
        stop_words = open(stopwords_fn, 'r').read().split()
        
        if type(actions) == dict:
            new_actions = {}
            for key, value in actions.items():
                new_actions[key] = value[~value['token'].isin(stop_words)]
            return new_actions
        else:
            return actions[~actions['token'].isin(stop_words)]
    
    def listen(self, _range1, _range2, editor, granularity,
               black, red, blue, green, black_conflict, red_conflict, damage_t, goodwill_t, goodwill_c, damage_c):
        "Listener."
        
        df = self.df[(self.df.rev_time.dt.date >= _range1) &
                (self.df.rev_time.dt.date <= _range2)]
        
        # Added this to order the df by rev_time, so the next filters (based on ORES) work 
        df.sort_values(by=['rev_time'], inplace=True)
        
        if damage_t != 0 or goodwill_t != 0:
            not_spam = filter_vandalism_ores(self.ores_scores, 
                                             goodfaith_cmp=goodwill_c, goodfaith_threshold=goodwill_t, 
                                             damaging_cmp=damage_c, damaging_threshold=damage_t)
            
            #idea: filter the vandalism/spam for conflict calculation, as otherwise, it drowns out actual substantial disputes between editors
            to_filter = df['rev_id'].isin(not_spam)
            #filters not only the spam,but also the revision right after the spam, since otherwise, spam/vandalism fighters will contribute to conflict scores
            shifted = to_filter.shift(1)
            x = to_filter&shifted
            #replace the first row with the original, since its NaN now
            x.iloc[:1] = to_filter.iloc[:1]
            df = df[x]
            
            #NOTE: the "secondary filter" ("shifted") will take into account the "last" revision's passing through the treshold. If and what that last revision is changes with the date filter. I.e. a revision could be not filtered out with a narrow time filter, but then be filtered out when previous dates/revisions are included, the last of which triggered the initial threshold filter "to_filter" and passes it on in "shifted"
         
        
        df_conflict = df.groupby(pd.Grouper(
            key='rev_time', freq=granularity[0])).agg({'conflicts': ['sum'],
                                       'elegibles': ['sum'],
                                       'revisions': ['sum'],
                                       'conflict': ['count', 'sum']}).reset_index()
        self.traces = {}
        df_conflict = self._add_trace(df_conflict, black_conflict, 'rgba(0, 0, 0, 1)')
        df_conflict = self._add_trace(df_conflict, red_conflict, 'rgba(255, 0, 0, .8)')
        

        if editor != 'All':
            df = df[df[self.editor_column] == editor]
            
        if (granularity[0] == "D") or (granularity[0] == "W"):
            df = df.groupby(pd.Grouper(
                key='rev_time', freq=granularity[0])).sum().reset_index()
        elif granularity[0] == "M":
            df = df.groupby(pd.Grouper(
                key='rev_time', freq=granularity[0] + 'S')).sum().reset_index()
            df["rev_time"] = df["rev_time"] + MonthEnd(1)
        else:
            df = df.groupby(pd.Grouper(
                key='rev_time', freq=granularity[0] + 'S')).sum().reset_index()
            df["rev_time"] = df["rev_time"] - pd.Timedelta(days=1)
            
        
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
           
    def _add_trace(self, df, metric, color):
        "Called in listener. Add conflict score and elegible actions data."
        sel = df.index
        if metric == 'None':
            return df
        elif metric == 'Norm. Conflict Score':
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
    
    def actions_listen(self, _range1, _range2, editor, granularity,
               black, red, blue, green):
        "Original Listener."
        df = self.actions_one_editor

        df = df[(df.rev_time.dt.date >= _range1) &
                (df.rev_time.dt.date <= _range2)]

        if editor != 'All':
            df = df[df[self.editor_column] == editor]
            
        if (granularity[0] == "D") or (granularity[0] == "W"):
            df = df.groupby(pd.Grouper(
                key='rev_time', freq=granularity[0])).sum().reset_index()
            
        elif granularity[0] == "M":
            df = df.groupby(pd.Grouper(
                key='rev_time', freq=granularity[0] + 'S')).sum().reset_index()
            df["rev_time"] = df["rev_time"] + MonthEnd(1)
        else:
            df = df.groupby(pd.Grouper(
                key='rev_time', freq=granularity[0] + 'S')).sum().reset_index()
            df["rev_time"] = df["rev_time"] - pd.Timedelta(days=1)

        data = [
            graph_objs.Scatter(
                x=df['rev_time'], y=df[black],
                name=black,
                marker=dict(color='rgba(0, 0, 0, 1)'))
        ]

        if red != 'None':
            data.append(graph_objs.Scatter(
                x=df['rev_time'], y=df[red],
                name=red,
                marker=dict(color='rgba(255, 0, 0, .8)')))

        if blue != 'None':
            data.append(graph_objs.Scatter(
                x=df['rev_time'], y=df[blue],
                name=blue,
                marker=dict(color='rgba(0, 153, 255, .8)')))           

        if green != 'None':
            data.append(graph_objs.Scatter(
                x=df['rev_time'], y=df[green],
                name=green,
                marker=dict(color='rgba(0, 128, 43, 1)')))

        self.df_plotted = df

        layout = graph_objs.Layout(hovermode='closest',
                                   xaxis=dict(title=granularity, ticklen=5,
                                              zeroline=True, gridwidth=2),
                                   yaxis=dict(title='Actions',
                                              ticklen=5, gridwidth=2),
                                   legend=dict(x=0.5, y=1.2),
                                   showlegend=True, barmode='group')

        plotly.offline.init_notebook_mode(connected=True)        
        plotly.offline.iplot({"data": data, "layout": layout})
        
        
        
def filter_vandalism_ores(res, damaging_threshold=None, goodfaith_threshold=None, damaging_cmp='>', goodfaith_cmp=None):
    
    # Operator defintions
    op = {'<':operator.lt, '>':operator.gt, '=':operator.eq, '>=':operator.ge, '<=':operator.le }

    # Filter out revisions that do not fullfil the thresholds with the corresponding comparators.
    nonspam = []
    for rev in res.itertuples(): 
        if np.isnan(rev.Damaging):
            continue
        
        damaging = True 
        goodfaith = True
        if damaging_threshold: 
            damaging = op[damaging_cmp](rev.Damaging, damaging_threshold)
        if goodfaith_threshold:
            goodfaith = op[goodfaith_cmp](rev.Goodfaith, goodfaith_threshold)
            
        if damaging & goodfaith:
                nonspam.append(int(rev.rev_id))            
    return nonspam