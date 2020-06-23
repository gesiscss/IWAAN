import calendar
from tqdm import tqdm
from datetime import date, timedelta
import pandas as pd

import qgrid

from IPython.display import display, clear_output, Markdown as md, HTML
from ipywidgets import Output

from external.ores import ORESAPI, ORESDV

# Some auxiliary functions for date filtering

def same_month(oppo_df):
    mask_same_year = oppo_df["oppo_time"].dt.year == oppo_df["edit_time"].dt.year
    mask_same_month = oppo_df["oppo_time"].dt.month == oppo_df["edit_time"].dt.month
    return mask_same_year & mask_same_month

def same_week(oppo_df):
    df = oppo_df.copy()
    df["edit_time"] = df["edit_time"].dt.date
    df["oppo_time"] = df["oppo_time"].dt.date
    
    diff = (df["edit_time"] - df["oppo_time"]).apply(lambda x: x.days)
    days_to_mon = df["edit_time"].unique()[0].weekday()
    
    return ~(diff > days_to_mon)

def same_day(oppo_df):
    return oppo_df["oppo_time"].dt.date == oppo_df["edit_time"].dt.date

def week_get_sunday(some_ts):
    return some_ts + timedelta(days=6 - some_ts.weekday())

def get_last_date_month(some_ts):
    year = some_ts.year
    month = some_ts.month
    day = calendar.monthrange(year, month)[1]
    
    return date(year, month, day)

def get_same_day(some_ts):
    return date(some_ts.year, some_ts.month, some_ts.day)

def within_month(series, some_date):
    mask_in_year = series.dt.year == some_date.year
    mask_in_month = series.dt.month == some_date.month
    
    return mask_in_year & mask_in_month

def merged_tokens_and_elegibles(elegibles, tokens):
    elegible_token = elegibles.set_index("rev_id")
    comp_tokens = tokens.copy()
    comp_tokens["revision"] = comp_tokens["rev_id"].astype(str)
    comp_tokens = comp_tokens.set_index("rev_id")
    tokens_with_conflict = comp_tokens.merge(elegible_token, how="left")
    
    return tokens_with_conflict
    


class EditorsListener:
    
    def __init__(self, sources, lng):
        self.df = sources["agg_actions"]
        self.elegibles = sources["elegibles"]
        self.tokens = sources["tokens"]
        self.all_elegibles = sources["all_elegibles"]
        self.all_tokens = sources["all_tokens"]
        self.names_id = dict(zip(sources["agg_actions"]["editor_str"], sources["agg_actions"]["editor"]))
        self.lng=lng
        
        print("Initializing...")
        self.actions = merged_tokens_and_elegibles(self.elegibles, self.tokens)
        self.all_actions = merged_tokens_and_elegibles(self.all_elegibles, self.all_tokens)
        self.selected_rev = self.all_actions["revision"].iloc[-1]
        
        self.revision_manager = RevisionsManager(self.df, self.all_actions, self.actions, None, self.lng)
        
        clear_output()
        
        
    def get_infos(self):
        monthly_dict = self.get_daily_tokens(self.tokens)
        opponent_info, scores_info, reac_info = self.calculate(monthly_dict)
        self.revision_manager.opponents_info = opponent_info
        self.test_opponent = opponent_info
        self.sort_by_granularity(scores_info, reac_info)
        
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
                            "revision", "time_diff", "editor"]].rename({"rev_time": "edit_time",
                                                        "editor": "idx_editor"}, axis=1).reset_index(drop=True)
        final = pd.concat([opponent_part,
                     idx_part], axis=1).sort_values(["token_id", "conflict"], ascending=[True, False]).set_index("token_id")

        return final
    
    
    def rank_conflict_under_tf(self, tf_oppo_df, freq):
        "freq=M, W, D"
        conflict_scores_tf = tf_oppo_df.copy()

        if freq == "M":        
            conflict_scores_tf["bench_date"] = conflict_scores_tf["edit_time"].dt.date.apply(get_last_date_month)
        if freq == "W":
            conflict_scores_tf["bench_date"] = conflict_scores_tf["edit_time"].dt.date.apply(week_get_sunday)
        if freq == "D":
            conflict_scores_tf["bench_date"] = conflict_scores_tf["edit_time"].dt.date.apply(get_same_day)

        conflict_scores_tf = conflict_scores_tf.groupby(["idx_editor", "editor", "bench_date"]).agg({"conflict": "sum"}).reset_index()
        conflict_scores_tf = conflict_scores_tf.sort_values("conflict", ascending=False).drop_duplicates("bench_date").reset_index(drop=True)

        return conflict_scores_tf


    def get_avg_rec_time(self, tf_oppo_df, freq):
        avg_rec_df = tf_oppo_df.copy()

        if freq == "M":        
            avg_rec_df["bench_date"] = avg_rec_df["edit_time"].dt.date.apply(get_last_date_month)
        if freq == "W":
            avg_rec_df["bench_date"] = avg_rec_df["edit_time"].dt.date.apply(week_get_sunday)
        if freq == "D":
            avg_rec_df["bench_date"] = avg_rec_df["edit_time"].dt.date.apply(get_same_day)

        avg_rec_df = avg_rec_df.groupby(["idx_editor",
                            "bench_date"])["time_diff"].agg(lambda x: str(x.mean()).split('.')[0]).reset_index()
        avg_rec_df = avg_rec_df.rename({"time_diff": "avg_reac_time"}, axis=1)

        if len(avg_rec_df) == 0:
            avg_rec_df = pd.DataFrame(columns=["idx_editor", "bench_date", "avg_rec_time"])

        return avg_rec_df
    
    
    def select(self, oppo_df):
        empty_df = pd.DataFrame(columns=["idx_editor", "editor", "bench_date", "conflict"])
        empty_rec_df = pd.DataFrame(columns=["idx_editor", "bench_date", "avg_rec_time"])
        empty_revs_df = pd.DataFrame(columns=["idx_editor", "bench_date", "revision"])
        display_cols = ["idx_editor", "editor", "oppo_time", "conflict", "edit_time", "time_diff", "revision"]

        if len(oppo_df) != 0:       
#             tf_oppo_month = oppo_df.loc[oppo_df["same_month"] == 1][display_cols]
#             tf_oppo_week = oppo_df.loc[oppo_df["same_week"] == 1][display_cols]
#             tf_oppo_day = oppo_df.loc[oppo_df["same_day"] == 1][display_cols]
            
            tf_oppo_month = oppo_df[display_cols]
            tf_oppo_week = oppo_df[display_cols]
            tf_oppo_day = oppo_df[display_cols]

            # Month
            conflict_scores_month = self.rank_conflict_under_tf(tf_oppo_month, freq="M")
            avg_time_month = self.get_avg_rec_time(tf_oppo_month, freq="M")

            # Weekly
            conflict_scores_week = self.rank_conflict_under_tf(tf_oppo_week, freq="W")
            avg_time_week = self.get_avg_rec_time(tf_oppo_week, freq="W")

            # Daily
            conflict_scores_day = self.rank_conflict_under_tf(tf_oppo_day, freq="D")
            avg_time_day = self.get_avg_rec_time(tf_oppo_day, freq="D")

        else:
            conflict_scores_month = empty_df
            conflict_scores_week = empty_df
            conflict_scores_day = empty_df

            avg_time_month = empty_rec_df
            avg_time_week = empty_rec_df
            avg_time_day = empty_rec_df

        return conflict_scores_month, conflict_scores_week, conflict_scores_day, avg_time_month, avg_time_week, avg_time_day
    
    
    def calculate(self, monthly_tokens):
        opponent_info = {}
        scores_info = {}
        avg_reac_info = {}
        for key, value in tqdm(monthly_tokens.items()):
            if len(value) != 0:
                opponents = self.get_opponents(value, self.actions)

#                 if len(opponents) != 0:
#                     opponents["same_day"], opponents["same_week"], opponents["same_month"] = [same_day(opponents).astype(int),                                                                 same_week(opponents).astype(int), same_month(opponents).astype(int)]

                scores_month, scores_week, scores_day, reac_month, reac_week, reac_day = self.select(opponents)
                opponent_info[key] = opponents
                scores_info[key] = [scores_month, scores_week, scores_day]
                avg_reac_info[key] = [reac_month, reac_week, reac_day]
            else:
                pass
                
        return opponent_info, scores_info, avg_reac_info
    
    def sort_by_granularity(self, scores_dict, reac_dict):
        empty_opponent = pd.DataFrame(columns=["idx_editor", "editor", "bench_date", "conflict"])
        month_opponent = []
        week_opponent = []
        daily_opponent = []

        for value in scores_dict.values():
            month_opponent.append(value[0])
            week_opponent.append(value[1])
            daily_opponent.append(value[2])

        month_opponent = pd.concat(month_opponent, sort=False).reset_index(drop=True).rename({"idx_editor": "editor_id",                                                                                  "editor": "main_opponent",                                                                                  "bench_date": "rev_time",}, axis=1).drop("conflict", axis=1)
        week_opponent = pd.concat(week_opponent, sort=False).reset_index(drop=True).rename({"idx_editor": "editor_id",                                                                                  "editor": "main_opponent",                                                                                  "bench_date": "rev_time",}, axis=1).drop("conflict", axis=1)
        daily_opponent = pd.concat(daily_opponent, sort=False).reset_index(drop=True).rename({"idx_editor": "editor_id",                                                                                  "editor": "main_opponent",                                                                                  "bench_date": "rev_time",}, axis=1).drop("conflict", axis=1)

        empty_avg_rec = pd.DataFrame(columns=["idx_editor", "bench_date", "avg_rec_time"])
        month_avg_rec = []
        week_avg_rec = []
        daily_avg_rec = []

        for value in reac_dict.values():
            month_avg_rec.append(value[0])
            week_avg_rec.append(value[1])
            daily_avg_rec.append(value[2])

        month_avg_rec = pd.concat(month_avg_rec,
                          sort=False).reset_index(drop=True).rename({"idx_editor": "editor_id", "bench_date": "rev_time",}, axis=1)
        week_avg_rec = pd.concat(week_avg_rec,
                         sort=False).reset_index(drop=True).rename({"idx_editor": "editor_id", "bench_date": "rev_time",}, axis=1)
        daily_avg_rec = pd.concat(daily_avg_rec,
                          sort=False).reset_index(drop=True).rename({"idx_editor": "editor_id", "bench_date": "rev_time",}, axis=1)

        self.gran_dict = {"Monthly": [month_opponent, month_avg_rec],
                 "Weekly": [week_opponent, week_avg_rec],
                 "Daily": [daily_opponent, daily_avg_rec]}
    
    
    def merge_main(self, df_to_merge, freq):
        df = df_to_merge.copy()
        df["rev_time"] = df["rev_time"].dt.date
        
        df_opponent = df.merge(self.gran_dict[freq][0], on=["rev_time", "editor_id"], how="left").sort_values("rev_time", ascending=False)
        final_df = df_opponent.merge(self.gran_dict[freq][1], on=["rev_time", "editor_id"], how="left").sort_values("rev_time", ascending=False)
        
        return final_df
    
    
    def on_select_revision(self, change):
        with self.out2:
            clear_output()
            self.selected_rev = self.second_qgrid.get_selected_df()["rev_id"].iloc[0]
            display(HTML(f"<a href='https://{self.lng}.wikipedia.org/w/index.php?diff={self.selected_rev}&title=TITLEDOESNTMATTER&diffmode=source' target='_blank'>Cilck here to check revisions differences</a>"))
    
    def on_select_change(self, change):
        with self.out:
            clear_output()
            date_selected = self.qgrid_obj.get_selected_df().reset_index()["rev_time"].iloc[0]
            editor_selected = self.qgrid_obj.get_selected_df().reset_index()["editor_id"].iloc[0]
            editor_name = self.qgrid_obj.get_selected_df().reset_index()["editor"].iloc[0]
            page_title = self.actions["article_title"].unique()[0]
            display(md(f"Within **{self.current_freq}** timeframe, you have selected **{editor_name}** (id: {editor_selected})"))
            display(HTML(f"The revisions fall in <a href='https://{self.lng}.wikipedia.org/w/index.php?date-range-to={date_selected}&tagfilter=&title={self.actions['article_title'].unique()[0]}&action=history' target='_blank'>{date_selected}</a>"))

            second_df = self.revision_manager.get_main(date_selected, editor_selected, self.current_freq)
            columns_set = {"rev_time": {"width": 180}, "rev_id": {"width": 85}, "adds": {"width": 55}, "dels": {"width": 55},
               "reins": {"width": 55}, "productivity": {"width": 100}, "conflict": {"width": 70},
               "stopwords_ratio": {"width": 125, "toolTip": "stopwords ratio"},
               "main_opponent": {"width": 120, "toolTip": "main opponent"},
               "min_react": {"width": 90, "toolTip": "min reaction time"},
               "Damaging": {"width": 90}, "Goodfaith": {"width": 90}}
            self.second_qgrid = qgrid.show_grid(second_df, grid_options={'forceFitColumns': True}, column_definitions=columns_set)
            display(self.second_qgrid)
            
            self.out2 = Output()
            display(self.out2)
            self.second_qgrid.observe(self.on_select_revision, names=['_selected_rows'])
        
    
    def listen(self, _range1, _range2, granularity):
        if _range1 > _range2:
            return display(md("Please enter the correct date!"))
        else:
            df = self.df[(self.df.rev_time.dt.date >= _range1) & (self.df.rev_time.dt.date <= _range2)]
        df_from_agg = self.get_ratios(df, freq=granularity)
        df_from_agg = df_from_agg.rename({"editor_str": "editor_id"}, axis=1)
        df_display = self.merge_main(df_from_agg, freq=granularity)
        df_display["conflict"] = (df_display["conflict"] / df_display["elegibles"]).fillna(0)
        
        df_display["main_opponent"] = df_display["main_opponent"].replace(self.names_id)
        
        displayed = df_display[["rev_time", "editor", 
                      "adds", "dels", "reins",
                       "productivity", "conflict",
                       "stopwords_ratio", "main_opponent",
                       "avg_reac_time", "revisions", "editor_id"]].set_index("rev_time").sort_index(ascending=False)
        columns_set = {"rev_time": {"width": 90}, "editor": {"width": 85}, "adds": {"width": 55}, "dels": {"width": 55},
                       "reins": {"width": 55}, "productivity": {"width": 100}, "conflict": {"width": 70},
                       "stopwords_ratio": {"width": 125, "toolTip": "stopwords ratio"},
                       "main_opponent": {"width": 120, "toolTip": "main opponent"},
                       "avg_reac_time": {"width": 115, "toolTip": "average reaction time"},
                       "revisions": {"width": 80}, "editor_id": {"width": 80}}
        self.qgrid_obj = qgrid.show_grid(displayed, grid_options={'forceFitColumns':True}, column_definitions=columns_set)
        
        display(self.qgrid_obj)
        self.out = Output()
        display(self.out)
        
        self.current_freq = granularity
        self.qgrid_obj.observe(self.on_select_change, names=['_selected_rows'])
        
        
class RevisionsManager:
    
    def __init__(self, agg, merged_all_actions, merged_actions, opponents_info, lng):
        self.agg_actions = agg
        self.names_dict = agg[["editor_str", "editor"]].drop_duplicates().set_index("editor_str")["editor"].to_dict()
        
        self.actions_inc_stop = merged_all_actions
        self.actions_exc_stop = merged_actions
        self.opponents_info = opponents_info
        
        self.lng=lng
        
        
    def get_main(self, selected_date, selected_editor, freq):
        agg = self.add_revision_id()
        filtered_df = self.get_filtered_df(agg, selected_date, selected_editor, freq).reset_index(drop=True)
        df_ratios = self.get_ratios(filtered_df).reset_index()
        df_opponents = self.get_rev_conflict_reac(df_ratios)
        df_merge1 = df_ratios.merge(df_opponents, on="rev_id", how="left")
        df_ores = self.get_ores(df_merge1)
        df_merge2 = df_merge1.merge(df_ores, on="rev_id", how="left").set_index("rev_time")
        
        return df_merge2

    def add_revision_id(self):
        no_dup_actions = self.actions_inc_stop.drop_duplicates("revision").reset_index(drop=True)
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
        editor = rev_df["editor"].unique()[0]
        period = rev_df["rev_time"].astype("datetime64[ns]").dt.to_period('M').unique()[0]
        key = (period, editor)
        opponent_info = self.opponents_info[key]
        main_opponent_id = opponent_info.groupby(["editor"]).agg({"conflict": "sum"}).sort_values("conflict", ascending=False).iloc[0].name
        main_opponent = self.names_dict[main_opponent_id]

        return main_opponent, min_react
    
    
    def get_rev_conflict_reac(self, df_agg):
        df_agg = df_agg.loc[~(df_agg["conflict"] == 0)]
        second_revs = df_agg["rev_id"].values

        rev_conflicts = pd.DataFrame(columns=["rev_id", "main_opponent", "min_react"])
        for idx, rev in enumerate(second_revs):
            some_rev = self.actions_exc_stop[self.actions_exc_stop["revision"] == rev]
            if len(some_rev) != 0:
                rev_conflicts.loc[idx] = [rev] + list(self.get_most_conflict_from_rev(some_rev))
                
        return rev_conflicts
    
    
    def get_ores(self, merge1):
        # Revsion list
        revs_list = merge1["rev_id"].values
        
        # Use ORESAPI
        ores_dv = ORESDV(ORESAPI(lng=self.lng))
        ores_df = ores_dv.get_goodfaith_damage(revs_list)
        
        return ores_df
        
        
        
        