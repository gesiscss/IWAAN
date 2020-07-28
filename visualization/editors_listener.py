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

# Some auxiliary functions for date filtering

def week_get_sunday(some_ts):
    return some_ts + timedelta(days=6 - some_ts.weekday())

def get_last_date_month(some_ts):
    year = some_ts.year
    month = some_ts.month
    day = calendar.monthrange(year, month)[1]
    
    return date(year, month, day)

def get_same_day(some_ts):
    return date(some_ts.year, some_ts.month, some_ts.day)

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
    """Open a list of stop words and remove from the dataframe the tokens that 
    belong to this list.
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
    
def mark_last_day(df):
    df["last_day_month"] = df["rev_time"].dt.date.apply(get_last_date_month)
    df["last_day_week"] = df["rev_time"].dt.date.apply(week_get_sunday)
    df["this_day"] = df["rev_time"].dt.date.apply(get_same_day)
    
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
    
    def __init__(self, agg, sources, lng, search_widget=None):
        self.df = agg
        self.all_elegibles = sources["elegibles_all"]
        self.all_tokens = sources["tokens_all"]
        self.names_id = dict(zip(agg["editor_str"], agg["editor"]))
        self.lng=lng
        
        print("Initializing...")
               
        self.selected_rev = str(self.all_tokens["rev_id"].iloc[-1])
        
        self.wikidv = sources["wiki_dv"]

        self.revision_manager = RevisionsManager(self.df, self.all_elegibles, self.all_tokens, None, self.lng)
        
        self.search_widget = search_widget
        if self.search_widget != None:            
            self.search_widget.value = self.selected_rev
        
        clear_output()
        
        
    def get_infos(self):
        monthly_dict = self.get_daily_tokens(remove_stopwords(self.all_tokens, self.lng))
        print("Calculating...")
        opponent_info = self.calculate(monthly_dict)
        self.revision_manager.opponents_info = opponent_info
        self.sort_by_granularity(opponent_info)
        
        clear_output()
           
    def get_main(self, freq):
        monthly_df = self.get_ratios(self.df, freq=freq)
               
        return monthly_df
           
    def get_ratios(self, df_with_tf, freq):
        time_grouper = pd.Grouper(key="rev_time", freq=freq[0])
        df_ratios = df_with_tf.groupby([time_grouper, 
                             "editor", 
                             "editor_str"]).agg({key: "sum" for key in df_with_tf.columns[5:]}).reset_index()
        
        df_ratios["productivity"] = df_ratios["total_surv_48h"] / df_ratios["total"]
        df_ratios["stopwords_ratio"] = df_ratios["total_stopword_count"] / df_ratios["total"]
        
        return df_ratios
    
    
    def get_daily_tokens(self, tokens_source):
        no_dup_tokens = tokens_source.drop_duplicates(["rev_id", "rev_time"]).reset_index(drop=True)
        no_dup_tokens["rev_id"] = no_dup_tokens["rev_id"].astype(str)
        no_dup_tokens = no_dup_tokens.sort_values("rev_time").reset_index(drop=True)
        no_dup_tokens["mark_month"] = no_dup_tokens["rev_time"].astype("datetime64[ns]").dt.to_period('M')
        no_dup_tokens["mark"] = list(zip(no_dup_tokens["mark_month"], no_dup_tokens["editor"]))

        merge_tokens = no_dup_tokens[["mark", "rev_id"]]
        retrieval_dict = dict(merge_tokens.groupby("mark")["rev_id"].apply(list))

        return retrieval_dict
            
    def get_opponents(self, revs_list, tokens_df):
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
    
    
    
    def calculate(self, monthly_tokens):
        opponent_dict = {}
        all_revs = sum(monthly_tokens.values(), [])
        
        tokens_no_stopwords = remove_stopwords(self.all_tokens, self.lng)
        elegibles_no_stopwords = remove_stopwords(self.all_elegibles, self.lng)
        actions = merged_tokens_and_elegibles(elegibles_no_stopwords, tokens_no_stopwords)        
        mark_last_day(actions)
        
        fill_first_out(actions)
        
        self.test_actions = actions
        self.test_revs = all_revs
        opponent_info = self.get_opponents(all_revs, actions)
                        
        return opponent_info
    
    
    def sort_scores(self, oppo_info, col):
        group_df = oppo_info.groupby(["idx_editor",
                             "editor",
                             col]).agg({"conflict": "sum"}).reset_index().rename({col: "bench_date"}, axis=1)
        sort_df = group_df.sort_values(["idx_editor", "conflict"], ascending=[True, False])
        sort_df = sort_df.drop_duplicates(["idx_editor", "bench_date"]).sort_values("bench_date").reset_index(drop=True)
        sort_df = sort_df.rename({"idx_editor": "editor_id", "editor": "main_opponent", "bench_date": "rev_time"}, axis=1)
        df_display = sort_df[["editor_id", "main_opponent", "rev_time"]]
        
        return df_display
    
    def avg_reac(self, oppo_info, col):
        avg_reac_display = oppo_info.groupby(["idx_editor", 
                    col])["time_diff"].agg(lambda x: str(x.mean()).split('.')[0]).reset_index().rename({col: "rev_time",                                           "time_diff":"avg_reac_time", "idx_editor":"editor_id"},axis=1).sort_values("rev_time").reset_index(drop=True)
        
        return avg_reac_display
           
    
    def sort_by_granularity(self, oppo_info):
        
        month_opponent = self.sort_scores(oppo_info, "last_day_month")
        week_opponent = self.sort_scores(oppo_info, "last_day_week")
        daily_opponent = self.sort_scores(oppo_info, "this_day")
        
        month_avg_rec = self.avg_reac(oppo_info, "last_day_month")
        week_avg_rec = self.avg_reac(oppo_info, "last_day_week")
        daily_avg_rec = self.avg_reac(oppo_info, "this_day")

        self.gran_dict = {"Monthly": [month_opponent, month_avg_rec],
                 "Weekly": [week_opponent, week_avg_rec],
                 "Daily": [daily_opponent, daily_avg_rec]}
    
    
    def merge_main(self, df_to_merge, freq):
        df = df_to_merge.copy()
        df["rev_time"] = df["rev_time"].dt.date
        
        df_opponent = df.merge(self.gran_dict[freq][0], on=["rev_time", "editor_id"], how="left").sort_values("rev_time", ascending=False)
        final_df = df_opponent.merge(self.gran_dict[freq][1], on=["rev_time", "editor_id"], how="left").sort_values("rev_time", ascending=False)
        
        return final_df
    
    def get_comments(self):
        page_id = self.df["page_id"].unique()[0]
        comment_content = self.wikidv.get_talk_content(page_id)[["revid", "comment"]].rename({"revid": "rev_id"}, axis=1)
        comment_content["rev_id"] = comment_content["rev_id"].astype(str)
        
        self.rev_comments = dict(zip(comment_content["rev_id"], comment_content["comment"]))
    
    
    def on_select_revision(self, change):
        with self.out2:
            clear_output()
            self.selected_rev = self.second_qgrid.get_selected_df()["rev_id"].iloc[0]
            self.search_widget.value = self.selected_rev
            display(md("Loading comments..."))
            self.get_comments()
            clear_output()
            if self.selected_rev not in self.rev_comments.keys():
                self.rev_comments[self.selected_rev] = ''            
            display(md(f"**Comment for the revision {self.selected_rev}:** {self.rev_comments[self.selected_rev]}"))
            display(HTML(f"<a href='https://{self.lng}.wikipedia.org/w/index.php?diff={self.selected_rev}&title=TITLEDOESNTMATTER&diffmode=source' target='_blank'>Cilck here to check revisions differences</a>"))
    
    def on_select_change(self, change):
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
        if (len(str(_range1.year)) < 4) | (len(str(_range2.year)) < 4):
            return display(md("Please enter the correct date!"))
        if _range1 > _range2:
            return display(md("Please enter the correct date!"))
        else:
            df = self.df[(self.df.rev_time.dt.date >= _range1) & (self.df.rev_time.dt.date <= _range2)]
        df_from_agg = self.get_ratios(df, freq=granularity)
        df_from_agg = df_from_agg.rename({"editor_str": "editor_id"}, axis=1)
        self.test_df_agg = df_from_agg
        df_display = self.merge_main(df_from_agg, freq=granularity)
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
    
    def __init__(self, agg, all_elegibles, all_tokens, opponents_info, lng):
        self.agg_actions = agg
        self.names_dict = agg[["editor_str", "editor"]].drop_duplicates().set_index("editor_str")["editor"].to_dict()
        
        self.all_elegibles = all_elegibles
        self.all_tokens = all_tokens
        self.opponents_info = opponents_info
        
        self.lng=lng
        
        
    def get_main(self, selected_date, selected_editor, freq):
        agg = self.add_revision_id()
        filtered_df = self.get_filtered_df(agg, selected_date, selected_editor, freq).reset_index(drop=True)
        df_ratios = self.get_ratios(filtered_df).reset_index()
        df_opponents = self.get_rev_conflict_reac(df_ratios)
        df_merge1 = df_ratios.merge(df_opponents, on="rev_id", how="left")
        self.test_merge1 = df_merge1
        df_ores = self.get_ores(df_merge1)
        self.test_ores = df_ores
        df_merge2 = df_merge1.merge(df_ores, on="rev_id", how="left").set_index("rev_time")
        
        return df_merge2

    def add_revision_id(self):
        no_dup_actions = merged_tokens_and_elegibles(self.all_elegibles, self.all_tokens, drop=True)
        no_dup_actions["rev_time"] = no_dup_actions["rev_time"].astype("datetime64[ns]")
        revs_to_merged = no_dup_actions[["rev_time", "revision"]]
        agg_with_revs = self.agg_actions.merge(revs_to_merged, on="rev_time", how="left")
        agg_with_revs.insert(1, "rev_id", agg_with_revs["revision"])
        agg_with_revs = agg_with_revs.drop("revision", axis=1).sort_values("rev_time").reset_index(drop=True)

        return agg_with_revs
    
    
    def get_filtered_df(self, df, input_date, editor, freq):
        years = df["rev_time"].dt.year
        months = df["rev_time"].dt.month
        days = df["rev_time"].dt.day

        mask_year = years == input_date.year
        mask_month = months == input_date.month
        mask_editor = df["editor_str"] == editor

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
    
    
    def get_ratios(self, filtered_agg):    
        filtered_agg["productivity"] = filtered_agg["total_surv_48h"] / filtered_agg["total"]
        filtered_agg["stopwords_ratio"] = filtered_agg["total_stopword_count"] / filtered_agg["total"]
        filtered_agg["conflict"] = filtered_agg["conflict"] / filtered_agg["elegibles"]
        filtered_agg["conflict"] = filtered_agg["conflict"].fillna(0)
        to_display = filtered_agg[["rev_time", "rev_id", "adds", "dels",
                       "reins", "productivity", "conflict", "stopwords_ratio"]].set_index("rev_time")

        return to_display
    
    
    def get_most_conflict_from_rev(self, rev_df):
        sort_rev = rev_df.sort_values("conflict", ascending=False)
        min_react = str(sort_rev.iloc[0]["time_diff"])
        
        # Find most opponent
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
    
    
    def get_rev_conflict_reac(self, df_agg):
        df_agg = df_agg.loc[~(df_agg["conflict"] == 0)]
        second_revs = df_agg["rev_id"].values

        rev_conflicts = pd.DataFrame(columns=["rev_id", "main_opponent", "min_react"])
        actions_exc_stop = remove_stopwords(merged_tokens_and_elegibles(self.all_elegibles, self.all_tokens), self.lng).reset_index(drop=True)
        mark_last_day(actions_exc_stop)
        
        fill_first_out(actions_exc_stop)
        for idx, rev in enumerate(second_revs):
            some_rev = actions_exc_stop[actions_exc_stop["revision"] == rev]
            if len(some_rev) != 0:
                rev_conflicts.loc[idx] = [rev] + list(self.get_most_conflict_from_rev(some_rev))
                
        return rev_conflicts
    
    def __split_arr(self, arr, threshold=50):
        return [arr[i: i + threshold] for i in range(0, len(arr), threshold)]
        
    def get_ores(self, merge1):
        # Revsion list
        revs_list = merge1["rev_id"].values
        
        # Use ORESAPI
        ores_dv = ORESDV(ORESAPI(lng=self.lng))
        if len(revs_list) > 50:
            revs_container = []
            for chunk in self.__split_arr(revs_list):
                chunk_df = ores_dv.get_goodfaith_damage(chunk)
                revs_container.append(chunk_df)
            ores_df = pd.concat(revs_container).reset_index(drop=True)
        else:
            ores_df = ores_dv.get_goodfaith_damage(revs_list)
        
        return ores_df
    

class RankedEditorsListener:
    
    def __init__(self, agg):        
        # Specify unregistered id.
        surv_total = agg[["rev_time", "editor_str", "editor", "total_surv_48h"]]
        new_editor = pd.DataFrame(np.where(surv_total["editor"] == "Unregistered", surv_total["editor_str"], surv_total["editor"]), columns=["editor"])
        surv_total = pd.concat([surv_total[["rev_time", "total_surv_48h"]], new_editor], axis=1)
        self.df = surv_total
        
    def df_to_plot(self, df, editor_name):
        example_editor = df[df["editor"] == editor_name]
        example_to_plot = df[["rev_time"]].merge(example_editor, how="left")
        example_to_plot["editor"] = example_editor["editor"].unique()[0]
        example_to_plot.fillna(0, inplace=True)

        return example_to_plot
        
    def listen(self, _range1, _range2, granularity, top):
        df_time = self.df[(self.df.rev_time.dt.date >= _range1) &
                (self.df.rev_time.dt.date <= _range2)].reset_index(drop=True)
        
        mark_last_day(df_time)
        # Granularity.
        if granularity == "Daily":
            df = df_time.groupby(["this_day", "editor"]).agg({"total_surv_48h": "sum"}).reset_index().rename({"this_day": "rev_time"}, axis=1)
        elif granularity == "Weekly":
            df = df_time.groupby(["last_day_week", "editor"]).agg({"total_surv_48h": "sum"}).reset_index()\
                    .rename({"last_day_week": "rev_time"}, axis=1)
        elif granularity == "Monthly":
            df = df_time.groupby(["last_day_month", "editor"]).agg({"total_surv_48h": "sum"}).reset_index()\
                    .rename({"last_day_month": "rev_time"}, axis=1)
        else:
            df = df_time
        
        # Get top editors list.
        group_only_surv = df.groupby("editor")\
                            .agg({"total_surv_48h": "sum"}).sort_values("total_surv_48h", ascending=False).reset_index()
        editors_list_20 = list(group_only_surv.iloc[0:top,:]["editor"])
        
        # For displaying
        group_to_display = group_only_surv.iloc[0:top,:].reset_index(drop=True)
        group_to_display["rank"] = group_to_display.index + 1
        group_to_display = group_to_display.rename({"editor": "editor", "total_surv_48h": "total 48h-survival actions"}, axis=1).set_index("rank")
                
        # Sort over time
        group_surv = df.groupby(["rev_time", "editor"]).agg({"total_surv_48h": "sum"}).reset_index()
        group_surv = group_surv.sort_values(["rev_time", "total_surv_48h"], ascending=(True, False))
        
        # Pick up top20 editors.
        mask_inlist = group_surv["editor"].isin(editors_list_20)
        group_surv_20 = group_surv.loc[mask_inlist]
        merge_df = group_surv[["rev_time"]].merge(group_surv_20[["rev_time", "editor", "total_surv_48h"]], how="left").reset_index(drop=True)
        self.test_merge = merge_df
        
        #Table
        self.qgrid_obj = qgrid.show_grid(group_to_display, grid_options={"minVisibleRows": 2})
        display(self.qgrid_obj)
        
        # Plot
        fig = go.Figure()
        for editor in editors_list_20:
            df_plot = merge_df[merge_df["editor"] == editor].reset_index(drop=True)
#             df_plot = self.df_to_plot(merge_df, editor)
            fig.add_trace(go.Scatter(x=df_plot["rev_time"], y=df_plot["total_surv_48h"], mode="lines+markers", name=editor))
        fig.update_layout(showlegend=True)
        fig.update_yaxes(title_text="Total 48h-survival actions")
        fig.show()
        

        
        
        
        
        
        