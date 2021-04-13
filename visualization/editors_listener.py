import calendar
from datetime import date, timedelta
import pandas as pd
import numpy as np

import qgrid
import plotly
import plotly.graph_objects as go
import plotly.express as px

from IPython.display import display, clear_output, Markdown as md, HTML
from ipywidgets import Output

from external.ores import ORESAPI, ORESDV

# Auxiliary functions for date manipulating.
def week_get_sunday(some_ts):
    return some_ts + timedelta(days=6 - some_ts.weekday())

def get_last_date_month(some_ts):
    year = some_ts.year
    month = some_ts.month
    day = calendar.monthrange(year, month)[1]
    
    return date(year, month, day)

def get_same_day(some_ts):
    return date(some_ts.year, some_ts.month, some_ts.day)

def mark_last_day(df):
    df["last_day_month"] = df["rev_time"].dt.date.apply(get_last_date_month)
    df["last_day_week"] = df["rev_time"].dt.date.apply(week_get_sunday)
    df["this_day"] = df["rev_time"].dt.date.apply(get_same_day)

# Other auxiliary functions.
def merged_tokens_and_elegibles(elegibles, tokens, drop=False):
    elegible_token = elegibles.set_index("rev_id")
    comp_tokens = tokens.copy()
    comp_tokens["revision"] = comp_tokens["rev_id"].astype(str)
    comp_tokens = comp_tokens.set_index("rev_id")
    tokens_with_conflict = comp_tokens.merge(elegible_token, how="left")
    
    if drop:
        return tokens_with_conflict.drop_duplicates("revision").reset_index(drop=True)
    else:
        return tokens_with_conflict

def remove_stopwords(actions, lng):
    """Open a list of stopwords and remove them from the dataframe.
    ...
    Parameters:
    -----------
    actions (Union[pd.DataFrame, dict]): a dataframe containing tokens info.
    lng (str): Language selected from {'en', 'de'}
    ...
    Returns:
    -----------
    Union[pd.DataFrame, dict]: pd.DataFrame(s) without stopwords tokens.
    """
    if lng == 'en':
        stopwords_fn='data/stopword_list.txt'
    elif lng == 'de':
        stopwords_fn='data/stopword_list_de.txt'
    else:
        stopwords_fn='data/stopword_list.txt'

    stop_words = open(stopwords_fn, 'r').read().split()

    if type(actions) == dict:
        for key, value in actions.items():
            actions[key] = value[~value['token'].isin(stop_words)]
        return actions
    else:
        return actions[~actions['token'].isin(stop_words)]
      
def cal_scores(series, base=3600):
    return np.log(base) / np.log(series.astype('timedelta64[s]') + 2)

def fill_first_out(df):
    mask_ori_rev = df["o_rev_id"].astype(str) == df["revision"]
    mask_out = df["action"] == "out"
    df_ori = df.loc[mask_ori_rev, :]
    df_out = df.loc[mask_out, :]

    idx_first_next = df_ori.index + 1
    idx_out = df_out.index

    idx_first_out = idx_first_next.intersection(idx_out)

    df_first_two = df.loc[df_ori.index.union(idx_first_out), :]

    timediff_first_out = (df_first_two["rev_time"] - df_first_two.shift(1)["rev_time"]).loc[idx_first_out]
    scores_first_out = cal_scores(timediff_first_out)

    df.loc[idx_first_out, "time_diff"] = timediff_first_out
    df.loc[idx_first_out, "conflict"] = scores_first_out
    

class EditorsListener:
    """
    Listener to display the tables in A2.1 of NB2, where several editor-based metrics under different
    time frames are presented.
    ...
    Attributes:
    -----------
    df (pd.DataFrame): global variable agg_actions, i.e. the dataframe storing actions, conflicts, stopwords
                etc. of each revision.
    all_elegibles (pd.DataFrame): elegibles actions (including stopwords) from ConflictManager.
    all_tokens (pd.DataFrame): actions occurring on all tokens, including stopwords, from ConflictManager.
    names_id (dict): correspondence between editor's name and id.
    lng (str): Langauge selected from {'en', 'de'}.
    selected_rev (str): String of selected revision, serving for A2.3.
    wikidv (object): external.wikipedia.WikipediaDV defined before, utilized to get revision's comment 
               through wiki API.
    revision_manager (object): RevisionsManger defined below, utilized to display the second table.
    search_widget (object): the widget used in A2.3, automatically being filled with selected_rev.
    gran_dict (dict): dictionary storing opponents' info and average reaction time, created by 
               sort_by_granularity() method.
    rev_comments (dict): {revision id: comment to this revision}, created by get_comments() method.
    """
    
    def __init__(self, agg, sources, lng, search_widget=None):
        self.df = agg
        self.all_elegibles = sources["elegibles_all"]
        self.all_tokens = sources["tokens_all"]
        self.names_id = dict(zip(agg["editor_str"], agg["editor"]))
        self.lng=lng
        
        # Initializing process.
        print("Initializing...")
               
        self.selected_rev = str(self.all_tokens["rev_id"].iloc[-1])
        
        self.wikidv = sources["wiki_dv"]

        self.revision_manager = RevisionsManager(self.df, self.all_elegibles, self.all_tokens, None, self.lng)
        
        self.search_widget = search_widget
        if self.search_widget != None:            
            self.search_widget.value = self.selected_rev
        
        clear_output()
        
        
    def get_infos(self):
        """
        Run this method before creating listener.
        """
        monthly_dict = self._get_monthly_summary(remove_stopwords(self.all_tokens, self.lng))
        print("Calculating...")
        opponent_info = self._calculate(monthly_dict)
        self.revision_manager.opponents_info = opponent_info
        self._sort_by_granularity(opponent_info)
        
        clear_output()
        
    def _get_monthly_summary(self, tokens_source):
        """
        Who created which revisions in each month.
        ...
        Parameters:
        -----------
        tokens_source (pd.DataFrame): all_tokens without stopwords (could also be with stopwords).
        ...
        Returns:
        -----------
        retrieval_dict (dict): e.g. {(Period('2005-03', 'M') <-- year-month, '162969' <-- editor_id): ['12053908'] <-- rev_ids,...}
        """
        no_dup_tokens = tokens_source.drop_duplicates(["rev_id", "rev_time"]).reset_index(drop=True)
        no_dup_tokens["rev_id"] = no_dup_tokens["rev_id"].astype(str)
        no_dup_tokens = no_dup_tokens.sort_values("rev_time").reset_index(drop=True)
        no_dup_tokens["mark_month"] = no_dup_tokens["rev_time"].astype("datetime64[ns]").dt.to_period('M')
        no_dup_tokens["mark"] = list(zip(no_dup_tokens["mark_month"], no_dup_tokens["editor"]))

        merge_tokens = no_dup_tokens[["mark", "rev_id"]]
        retrieval_dict = dict(merge_tokens.groupby("mark")["rev_id"].apply(list))

        return retrieval_dict
            
    def _get_opponents(self, revs_list, tokens_df):
        """
        See calculate()
        ...
        Parameters:
        -----------
        revs_list (list): list of revision ids after stopwords being removed.
        tokens_df (pd.DataFrame): merged all_actions all_elegibles dataframe, with other 
                    additional time frame information.
        ...
        Returns:
        ------------
        final (pd.DataFrame): see opponent_info in calculate.
        """
        # Filter out the tokens edited by this editor in those revs.
        mask_rev = tokens_df["revision"].isin(revs_list)
        conflicts_filter = tokens_df.loc[mask_rev].dropna()  # .dropna() aims to eliminate self-editing

        # Get tokens info of its opponents
        opponent_idx = conflicts_filter.index - 1
        opponent_df = tokens_df.loc[opponent_idx]

        # Merge the infos we need
        opponent_part = opponent_df[["editor", "rev_time"]].rename({"rev_time": "oppo_time"}, axis=1).reset_index(drop=True)
        idx_part = conflicts_filter[["token_id", "conflict", "rev_time",
                            "revision", "time_diff", "editor",
                            "last_day_month", "last_day_week", "this_day"]].rename({"rev_time": "edit_time",
                                                        "editor": "idx_editor"}, axis=1).reset_index(drop=True)
        final = pd.concat([opponent_part,
                     idx_part], axis=1).sort_values(["token_id", "conflict"], ascending=[True, False]).set_index("token_id")

        return final    
    
    def _calculate(self, monthly_tokens):
        """
        Given revision list of each editor grouped in each month, get the detailed
        opponent information of each token. Core method: get_opponents()
        ...
        Parameters:
        -----------
        monthly_tokens (dict): Output of get_monthly_summary() method.
        ...
        Returns:
        -----------
        opponent_info (pd.DataFrame): Detailed opponent info of each editor including:
                        opponent editor (editor), original editor (idx_editor),
                        when being edited (edit_time), when being undone (oppo_time),
                        conflict score, revision, time interval, last day of this 
                        month/week/day, in which the original editing lies.
        """
        # An empty dict to store opponents.
        opponent_dict = {}
        
        # Merge all revision ids from monthly token dict into one list.
        all_revs = sum(monthly_tokens.values(), [])
        
        # Remove stopwords from all actions and elegible actions.
        tokens_no_stopwords = remove_stopwords(self.all_tokens, self.lng)
        elegibles_no_stopwords = remove_stopwords(self.all_elegibles, self.lng)
        
        # Merge the above table together thus we can use time diff and conflict score
        # information from elegible actions table.
        actions = merged_tokens_and_elegibles(elegibles_no_stopwords, tokens_no_stopwords)
        
        # Mark last day of this month/week/day
        mark_last_day(actions)
        
        # Fill time_diff, conflict for the first deletion, since this action is not considered
        # in ConflictManager.all_elegibles.
        fill_first_out(actions)
        
        # Get final opponents info through get_opponents() method.
        opponent_info = self._get_opponents(all_revs, actions)
                        
        return opponent_info
    
    def _sort_scores(self, oppo_info, col):
        "Aggregate conflict scores."
        # If the opponent is editor himself, then exclude it.
        oppo_info = oppo_info[oppo_info["editor"] != oppo_info["idx_editor"]]
        
        # Aggregate conflict scores for each editor and its opponent in each time frame.
        group_df = oppo_info.groupby(["idx_editor",
                            "editor",
                             col]).agg({"conflict": "sum"}).reset_index().rename({col: "bench_date"}, axis=1)
        
        # Sort values and rename columns.
        sort_df = group_df.sort_values(["idx_editor", "conflict"], ascending=[True, False])
        sort_df = sort_df.drop_duplicates(["idx_editor", "bench_date"]).sort_values("bench_date").reset_index(drop=True)
        sort_df = sort_df.rename({"idx_editor": "editor_id", "editor": "main_opponent", "bench_date": "rev_time"}, axis=1)
        df_display = sort_df[["editor_id", "main_opponent", "rev_time"]]
        
        return df_display
    
    def _avg_reac(self, oppo_info, col):
        "Calculate average response reaction time."
        # If the opponent is editor himself, then exclude it.
        oppo_info = oppo_info[oppo_info["editor"] != oppo_info["idx_editor"]]
        
        # Calculate reverage response time for each editor.
        avg_reac_display = oppo_info.groupby(["idx_editor", 
                    col])["time_diff"].agg(lambda x: str(x.mean()).split('.')[0]).reset_index().rename({col: "rev_time",                          "time_diff":"avg_reac_time","idx_editor":"editor_id"},axis=1).sort_values("rev_time").reset_index(drop=True)
        
        return avg_reac_display
               
    def _sort_by_granularity(self, oppo_info):
        """
        Sort the opponents info by month/week/day and calcalute average reaction time
        for each time frame.
        ...
        Parameters:
        -----------
        oppo_info (pd.DataFrame): Opponents dataframe got by calculate() method.
        ...
        Returns:
        -----------
        attribute gran_dict (dict): store opponents info and average reaction time for
                        each time frame.
        """
        # Aggregate conflict scores.
        month_opponent = self._sort_scores(oppo_info, "last_day_month")
        week_opponent = self._sort_scores(oppo_info, "last_day_week")
        daily_opponent = self._sort_scores(oppo_info, "this_day")
        
        # Average response reaction time.
        month_avg_rec = self._avg_reac(oppo_info, "last_day_month")
        week_avg_rec = self._avg_reac(oppo_info, "last_day_week")
        daily_avg_rec = self._avg_reac(oppo_info, "this_day")

        self.gran_dict = {"Monthly": [month_opponent, month_avg_rec],
                 "Weekly": [week_opponent, week_avg_rec],
                 "Daily": [daily_opponent, daily_avg_rec]}
        
    def _get_ratios(self, df_with_tf, freq):
        """Used in the listener. Get productivity and stopwords_ratio.
        ...
        Parameters:
        -----------
        df_with_tf (pd.DataFrame): self.df
        freq (str): Dropdown widget options (Monthly/Weekly/Daily)
        ...
        Returns:
        -----------
        df_ratios (pd.DataFrame): self.df with two more cols (productivity and stopwords_ratio)
        """
        # Granularity grouper.
        time_grouper = pd.Grouper(key="rev_time", freq=freq[0])
        
        # Sum per-revision actions up by graunularity
        df_ratios = df_with_tf.groupby([time_grouper, 
                             "editor", 
                             "editor_str"]).agg({key: "sum" for key in df_with_tf.columns[5:]}).reset_index()
        
        # Add two new columns.
        df_ratios["productivity"] = df_ratios["total_surv_48h"] / df_ratios["total"]
        df_ratios["stopwords_ratio"] = df_ratios["total_stopword_count"] / df_ratios["total"]
        
        return df_ratios
      
    def _merge_main(self, df_to_merge, freq):
        """
        Used in the listener.
        """
        df = df_to_merge.copy()
        df["rev_time"] = df["rev_time"].dt.date
        
        df_opponent = df.merge(self.gran_dict[freq][0], on=["rev_time", "editor_id"], how="left").sort_values("rev_time", ascending=False)
        final_df = df_opponent.merge(self.gran_dict[freq][1], on=["rev_time", "editor_id"], how="left").sort_values("rev_time", ascending=False)
        
        return final_df
    
    def get_comments(self):
        """
        Used in the listener. Get comments of each revision through wikipedia API.
        """
        page_id = self.df["page_id"].unique()[0]
        comment_content = self.wikidv.get_talk_content(page_id)[["revid", "comment"]].rename({"revid": "rev_id"}, axis=1)
        comment_content["rev_id"] = comment_content["rev_id"].astype(str)
        
        self.rev_comments = dict(zip(comment_content["rev_id"], comment_content["comment"]))
    
    
    def on_select_revision(self, change):
        "Second click and display comment."
        with self.out2:
            clear_output()
            self.selected_rev = str(self.second_qgrid.get_selected_df()["rev_id"].iloc[0]).encode("utf-8").decode("utf-8")
            self.search_widget.value = self.selected_rev
            display(md("Loading comments..."))
            self.get_comments()
            clear_output()
            if self.selected_rev not in self.rev_comments.keys():
                self.rev_comments[self.selected_rev] = ''            
            display(md(f"**Comment for the revision {self.selected_rev}:** {self.rev_comments[self.selected_rev]}"))
            display(HTML(f"<a href='https://{self.lng}.wikipedia.org/w/index.php?diff={self.selected_rev}&title=TITLEDOESNTMATTER&diffmode=source' target='_blank'>Cilck here to check revisions differences</a>"))
    
    def on_select_change(self, change):
        "First click."
        with self.out:
            clear_output()
            date_selected = self.qgrid_obj.get_selected_df().reset_index()["rev_time"].iloc[0]
            editor_selected = self.qgrid_obj.get_selected_df().reset_index()["editor_id"].iloc[0]
            editor_name = self.qgrid_obj.get_selected_df().reset_index()["editor"].iloc[0]
            page_title = self.all_tokens["article_title"].unique()[0]
            
            display(md("Loading revisions info..."))
            second_df = self.revision_manager.get_main(date_selected, editor_selected, self.current_freq)
            clear_output()
            
            display(md(f"Within **{self.current_freq}** timeframe, you have selected **{editor_name}** (id: {editor_selected})"))
            display(HTML(f"The revisions fall in <a href='https://{self.lng}.wikipedia.org/w/index.php?date-range-to={date_selected}&tagfilter=&title={page_title}&action=history' target='_blank'>{date_selected}</a>"))
            
            second_df.rename({"main_opponent": "main_op", "stopwords_ratio": "SW_ratio",
                             "productivity": "prod"}, axis=1, inplace=True)
            columns_set = {"rev_time": {"width": 165}, "rev_id": {"width": 85}, "adds": {"width": 50}, "dels": {"width": 50},
               "reins": {"width": 50}, "prod": {"width": 50, "toolTip": "productivity"}, "conflict": {"width": 70},
               "SW_ratio": {"width": 82, "toolTip": "stopwords ratio"},
               "main_op": {"width": 80, "toolTip": "main opponent"},
               "min_react": {"width": 132, "toolTip": "min reaction time"},
               "Damaging": {"width": 92}, "Goodfaith": {"width": 90}}
            self.second_qgrid = qgrid.show_grid(second_df, grid_options={'forceFitColumns': True,
                                                    'syncColumnCellResize': True}, column_definitions=columns_set)
            display(self.second_qgrid)
            
            self.out2 = Output()
            display(self.out2)
            self.second_qgrid.observe(self.on_select_revision, names=['_selected_rows'])
        
        
    def listen(self, _range1, _range2, granularity):
        "Listener."
        if (len(str(_range1.year)) < 4) | (len(str(_range2.year)) < 4):
            return display(md("Please enter the correct date!"))
        if _range1 > _range2:
            return display(md("Please enter the correct date!"))
        else:
            df = self.df[(self.df.rev_time.dt.date >= _range1) & (self.df.rev_time.dt.date <= _range2)]
        df_from_agg = self._get_ratios(df, freq=granularity)
        df_from_agg = df_from_agg.rename({"editor_str": "editor_id"}, axis=1)
        df_display = self._merge_main(df_from_agg, freq=granularity)
        df_display["conflict"] = (df_display["conflict"] / df_display["elegibles"]).fillna(0)
        
        df_display["main_opponent"] = df_display["main_opponent"].replace(self.names_id)
        
        df_display.rename({"main_opponent": "main_op", "stopwords_ratio": "SW_ratio",
                          "revisions": "revs", "productivity": "prod"}, axis=1, inplace=True)
        
        displayed = df_display[["rev_time", "editor", 
                      "adds", "dels", "reins",
                       "prod", "conflict",
                       "SW_ratio", "main_op",
                       "avg_reac_time", "revs", "editor_id"]].set_index("rev_time").sort_index(ascending=False)
        columns_set = {"rev_time": {"width": 90}, "editor": {"width": 85}, "adds": {"width": 50}, "dels": {"width": 50},
                       "reins": {"width": 50}, "prod": {"width": 50, "toolTip": "productivity"}, "conflict": {"width": 70},
                       "SW_ratio": {"width": 80, "toolTip": "stopwords ratio"},
                       "main_op": {"width": 90, "toolTip": "main opponent"},
                       "avg_reac_time": {"width": 125, "toolTip": "average reaction time"},
                       "revs": {"width": 45, "toolTip": "revisions"}, "editor_id": {"width": 80}}
        self.qgrid_obj = qgrid.show_grid(displayed, grid_options={'forceFitColumns':True}, column_definitions=columns_set)
        
        display(self.qgrid_obj)
        self.out = Output()
        display(self.out)
        
        self.current_freq = granularity
        if self.search_widget != None:
            self.qgrid_obj.observe(self.on_select_change, names=['_selected_rows'])
        
        
class RevisionsManager:
    """
    Called in EditorsListener. Calculate several metrics related to the revisions related to a 
    selected editor
    ...
    Attributes:
    -----------
    agg_actions (pd.DataFrame): global variable agg_actions, i.e. the dataframe storing actions, conflicts, stopwords
                etc. of each revision.
    all_elegibles (pd.DataFrame): elegibles actions (including stopwords) from ConflictManager.
    all_tokens (pd.DataFrame): actions occurring on all tokens, including stopwords, from ConflictManager.
    opponents_info (pd.DataFrame): Opponents information derived from EditorsListener.__calculate().
    lng (str): langauge from {'en', 'de'}
    """
    
    def __init__(self, agg, all_elegibles, all_tokens, opponents_info, lng):
        self.agg_actions = agg
        self.names_dict = agg[["editor_str", "editor"]].drop_duplicates().set_index("editor_str")["editor"].to_dict()
        
        self.all_elegibles = all_elegibles
        self.all_tokens = all_tokens
        self.opponents_info = opponents_info
        
        self.lng=lng
        
        
    def get_main(self, selected_date, selected_editor, freq):
        """
        Called in EditorsListener.on_select_change(). Merge several tables with different metrics.
        ...
        Parameters:
        -----------
        selected_date (datetime.date): Selected date.
        selected_editor (str): editor id
        freq (str): {"Monthly", "Weekly", "Daily"}
        ...
        Returns:
        --------
        df_merge2 (pd.DataFrame): the second table in A2.1.
        """
        agg = self._add_revision_id()
        filtered_df = self._get_filtered_df(agg, selected_date, selected_editor, freq).reset_index(drop=True)
        df_ratios = self._get_ratios(filtered_df).reset_index()
        df_opponents = self._get_rev_conflict_reac(df_ratios)
        df_merge1 = df_ratios.merge(df_opponents, on="rev_id", how="left")
        df_ores = self._get_ores(df_merge1)
        df_merge2 = df_merge1.merge(df_ores, on="rev_id", how="left").set_index("rev_time")
        return df_merge2
        
    def _add_revision_id(self):
        """
        Add revision id to aggregation dataframe.
        ...
        Returns:
        --------
        agg_with_revs (pd.DataFrame): agg_actions with an additional revision id column.
        """
        # Drop duplicated revisions
        no_dup_actions = merged_tokens_and_elegibles(self.all_elegibles, self.all_tokens, drop=True)
        
        # Only take rev_time and revision columns.
        no_dup_actions["rev_time"] = no_dup_actions["rev_time"].astype("datetime64[ns]")
        revs_to_merged = no_dup_actions[["rev_time", "revision"]]
        
        # Merge aggregation table and revision time/id table.
        agg_with_revs = self.agg_actions.merge(revs_to_merged, on="rev_time", how="left")
        #agg_with_revs.insert(1, "rev_id", agg_with_revs["revision"])
        agg_with_revs = agg_with_revs.drop("revision", axis=1).sort_values("rev_time").reset_index(drop=True)

        return agg_with_revs
    
    
    def _get_filtered_df(self, df, input_date, editor, freq):
        """
        Filter the aggregation dataframe using input year/month, editor
        and granularity.
        ...
        Parameters:
        -----------
        df (pd.DataFrame): aggregation dataframe with revision ids.
        input_date (datetime.date): rev_time in selected row
        ...
        Returns:
        -----------
        filtered_df (pd.DataFrame): revisions with some agg metrics
                        in a particular time frame.
        """
        # Decompose inputs
        years = df["rev_time"].dt.year
        months = df["rev_time"].dt.month
        days = df["rev_time"].dt.day
        
        # Create some masks.
        mask_year = years == input_date.year
        mask_month = months == input_date.month
        mask_editor = df["editor_str"] == editor
        
        # Filter by granularities.
        if freq == "Monthly":
            filtered_df = df.loc[mask_year & mask_month & mask_editor]
        elif freq == "Weekly":
            date_diff = input_date - df["rev_time"].dt.date
            mask_within_week = (date_diff <= timedelta(days=6)) & (timedelta(days=0) <= date_diff)
            filtered_df = df.loc[mask_within_week & mask_editor]
        else:
            mask_day = days == input_date.day
            filtered_df = df.loc[mask_year & mask_month & mask_day & mask_editor]

        return filtered_df
    
    
    def _get_ratios(self, filtered_agg):
        """
        Calculate ratios like productivity, stopwords ratio. Also standardize conflict score
        using elegible actions.
        ...
        Parameters:
        -----------
        filtered_agg (pd.DataFrame): output of get_filtered_df() method.
        ...
        Returns:
        -----------
        to_display (pd.DataFrame): dataframe with addtional several ratio columns.
        """
        filtered_agg["productivity"] = filtered_agg["total_surv_48h"] / filtered_agg["total"]
        filtered_agg["stopwords_ratio"] = filtered_agg["total_stopword_count"] / filtered_agg["total"]
        filtered_agg["conflict"] = filtered_agg["conflict"] / filtered_agg["elegibles"]
        filtered_agg["conflict"] = filtered_agg["conflict"].fillna(0)
        to_display = filtered_agg[["rev_time", "rev_id", "adds", "dels",
                       "reins", "productivity", "conflict", "stopwords_ratio"]].set_index("rev_time")

        return to_display
    
    
    def _get_most_conflict_from_rev(self, rev_df):
        """
        Called in get_rev_conflict_reac() method.
        Analyse the main opponent and min reaction time for each revision.
        ...
        Parameters:
        -----------
        rev_df (pd.DataFrame): actions without stopwords of a particular revision.
        ...
        Returns:
        -----------
        main_opponent (str): main opponent's name.
        min_react (str): minimum reaction time.
        """
        # Sort conflict score in desc order and find the fastest response.
        sort_rev = rev_df.sort_values("conflict", ascending=False)
        min_react = str(sort_rev.iloc[0]["time_diff"])
        
        # Find the main opponent.
        rev_id = rev_df["revision"].unique()[0]
        editor = rev_df["editor"].unique()[0]
        period = rev_df["rev_time"].astype("datetime64[ns]").dt.to_period('M').unique()[0]
        mask_date = self.opponents_info["edit_time"].astype("datetime64[ns]").dt.to_period('M') == period
        mask_editor = self.opponents_info["idx_editor"] == editor
        opponent_info = self.opponents_info[mask_date & mask_editor]
        opponent_info = opponent_info[opponent_info["revision"] == rev_id]
        main_opponent_id = opponent_info.groupby(["editor"]).agg({"conflict": "sum"}).sort_values("conflict", ascending=False).iloc[0].name
        main_opponent = self.names_dict[main_opponent_id]

        return main_opponent, min_react
    
    
    def _get_rev_conflict_reac(self, df_agg):
        """
        Get main opponent and minium reaction time of each given revision.
        ...
        Parameters:
        -----------
        df_agg (pd.DataFrame): output of get_ratios() method.
        ...
        Returns:
        -----------
        rev_conflicts (pd.DataFrame): dataframe with main opponent and min reaction time
        """
        # Revisions array.
        #df_agg = df_agg.loc[~(df_agg["conflict"] == 0)]
        second_revs = df_agg["rev_id"].values
        
        # Only consider non-stopwords.
        rev_conflicts = pd.DataFrame(columns=["rev_id", "main_opponent", "min_react"])
        actions_exc_stop = remove_stopwords(merged_tokens_and_elegibles(self.all_elegibles, self.all_tokens), self.lng).reset_index(drop=True)
        
        # Mark the last day of each time frame.
        mark_last_day(actions_exc_stop)
        
        # Also consider the first deletion (that is not considered in elegible actions).
        fill_first_out(actions_exc_stop)
        
        # For each revision analyse the main opponent using get_most_conflict_from_rev method.
        for idx, rev in enumerate(second_revs):
            some_rev = actions_exc_stop[actions_exc_stop["revision"] == rev]
            some_rev = some_rev.dropna(subset=["conflict"])
            if len(some_rev) != 0:
                self.problem_rev = some_rev
                rev_conflicts.loc[idx] = [rev] + list(self._get_most_conflict_from_rev(some_rev))
                
        return rev_conflicts
    
    def _split_arr(self, arr, threshold=50):
        return [arr[i: i + threshold] for i in range(0, len(arr), threshold)]
        
    def _get_ores(self, merge1):
        """
        Get Goodfaith and Damaging scores from ORES API.
        ...
        Parameters:
        -----------
        merge1 (pd.DataFrame): df_merge1 in get_main() method.
        ...
        Returns:
        -----------
        ores_df (pd.DataFrame): df storing scores.
        """
        # Revsion list
        revs_list = merge1["rev_id"].values
        
        # Use ORES API
        ores_dv = ORESDV(ORESAPI(lng=self.lng))
        if len(revs_list) > 50:
            revs_container = []
            for chunk in self._split_arr(revs_list):
                chunk_df = ores_dv.get_goodfaith_damage(chunk)
                revs_container.append(chunk_df)
            ores_df = pd.concat(revs_container).reset_index(drop=True)
        else:
            ores_df = ores_dv.get_goodfaith_damage(revs_list)
        
        return ores_df
    

class RankedEditorsListener:
    """Class for ranking editors by 48-hour survival actions.
    ...
    Attributes:
    -----------
    df (pd.DataFrame): stores all survival actions for each editor (if "Unregistered",
                replace it by IP.) 
    """
    def __init__(self, agg):
        # Specify unregistered id.
        surv_total = agg[["rev_time", "editor_str", "editor", "total_surv_48h"]]
        new_editor = pd.DataFrame(np.where(surv_total["editor"] == "Unregistered", surv_total["editor_str"], surv_total["editor"]), columns=["editor"])
        surv_total = pd.concat([surv_total[["rev_time", "total_surv_48h"]], new_editor], axis=1)
        self.df = surv_total
        
    def listen(self, _range1, _range2, granularity, top):
        "Listener."
        df_time = self.df[(self.df.rev_time.dt.date >= _range1) &
                (self.df.rev_time.dt.date <= _range2)].reset_index(drop=True)
        
        # Get top editors list.
        group_only_surv = df_time.groupby("editor")\
                            .agg({"total_surv_48h": "sum"}).sort_values("total_surv_48h", ascending=False).reset_index()
        editors_top = list(group_only_surv.iloc[0:top,:]["editor"])
        
        # For displaying
        group_to_display = group_only_surv.iloc[0:top,:].reset_index(drop=True)
        group_to_display["rank"] = group_to_display.index + 1
        group_to_display = group_to_display.rename({"editor": "editor", "total_surv_48h": "total 48h-survival actions"}, axis=1).set_index("rank")
                
        # Sort over time
        group_surv = df_time.groupby(["rev_time", "editor"]).agg({"total_surv_48h": "sum"}).reset_index()
        group_surv = group_surv.sort_values(["rev_time", "total_surv_48h"], ascending=(True, False))
        
        # Pick up top 5/10/20 editors.
        mask_inlist = group_surv["editor"].isin(editors_top)
        group_surv_top = group_surv.loc[mask_inlist]
        merge_df = group_surv[["rev_time"]].merge(group_surv_top[["rev_time", "editor", "total_surv_48h"]], how="left").reset_index(drop=True)
        
        #Table
        self.qgrid_obj = qgrid.show_grid(group_to_display, grid_options={"minVisibleRows": 2})
        display(self.qgrid_obj)
            
        # Generate pivot table
        pivoted = merge_df.pivot(index="rev_time", columns="editor", values="total_surv_48h")
        pivot_table = pd.DataFrame(pivoted.to_records())

        if "nan" in pivot_table.columns:
            pivot_table = pivot_table.drop("nan", axis=1).fillna(0)
        else:
            pivot_table = pivot_table.fillna(0)

        cols = list(pivot_table.columns)
        cols.remove("rev_time")
        agg_dict = {editor: "sum" for editor in cols}
        if granularity != "Timestamp (Revision)":
            group_pivot = pivot_table.groupby(pd.Grouper(key="rev_time", freq=granularity[0])).agg(agg_dict).reset_index()
        else:
            group_pivot = pivot_table
            
        group_pivot = group_pivot[["rev_time"] + editors_top]
        fig = go.Figure()
        for editor in group_pivot.columns:
            if editor == "rev_time":
                pass
            else:
                fig.add_trace(go.Scatter(x=group_pivot["rev_time"], y=group_pivot[editor], mode="lines", name=editor))
        fig.update_layout(showlegend=True, hovermode="x unified", margin=go.layout.Margin(l=50, r=50, b=195, t=10, pad=3))
        fig.update_yaxes(title_text="Total 48h-survival actions")
        fig.update_xaxes(title_text=f"{granularity}")
        fig.show()
        
        self.test_fig = fig
        

        
        
        
        
        
        