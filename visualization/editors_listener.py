import calendar
from tqdm import tqdm
from datetime import date, timedelta
import pandas as pd

import qgrid

from IPython.display import display, clear_output, Markdown as md
from ipywidgets import Output

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
    
    def __init__(self, sources):
        self.df = sources["agg_actions"]
        self.elegibles = sources["elegibles"]
        self.tokens = sources["tokens"]
        self.all_elegibles = sources["all_elegibles"]
        self.all_tokens = sources["all_tokens"]
        self.names_id = dict(zip(sources["agg_actions"]["editor_str"], sources["agg_actions"]["editor"]))
        
        print("Initializing...")
        self.actions = merged_tokens_and_elegibles(self.elegibles, self.tokens)
        self.all_actions = merged_tokens_and_elegibles(self.all_elegibles, self.all_tokens)
        
        self.revision_manager = RevisionsManager(self.actions, self.all_actions)
        
        clear_output()
        
        
    def get_infos(self):
        monthly_dict = self.get_daily_tokens(self.tokens)
        opponent_info, scores_info, reac_info = self.calculate(monthly_dict)
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
            tf_oppo_month = oppo_df.loc[oppo_df["same_month"] == 1][display_cols]
            tf_oppo_week = oppo_df.loc[oppo_df["same_week"] == 1][display_cols]
            tf_oppo_day = oppo_df.loc[oppo_df["same_day"] == 1][display_cols]

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

                if len(opponents) != 0:
                    opponents["same_day"], opponents["same_week"], opponents["same_month"] = [same_day(opponents).astype(int),                                                                 same_week(opponents).astype(int), same_month(opponents).astype(int)]

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
        self.selected_rev = self.second_qgrid.get_selected_df()["revision"].iloc[0]
    
    def on_select_change(self, change):
        with self.out:
            clear_output()
            date_selected = self.qgrid_obj.get_selected_df().reset_index()["rev_time"].iloc[0]
            editor_selected = self.qgrid_obj.get_selected_df().reset_index()["editor_id"].iloc[0]
            editor_name = self.qgrid_obj.get_selected_df().reset_index()["editor"].iloc[0]
            display(md(f"Within **{self.current_freq}** timeframe, you have selected **{editor_name}** (id: {editor_selected})"))
            display(md(f"The revisions fall in **{str(date_selected)}**"))

            second_df = self.revision_manager.get_main(date_selected, editor_selected, self.current_freq)
            self.second_qgrid = qgrid.show_grid(second_df, grid_options={'forceFitColumns': False})
            display(self.second_qgrid)
            self.second_qgrid.observe(self.on_select_revision, names=['_selected_rows'])
        
    
    def listen(self, granularity):        
        df_from_agg = self.get_ratios(self.df, freq=granularity)
        df_from_agg = df_from_agg.rename({"editor_str": "editor_id"}, axis=1)
        df_display = self.merge_main(df_from_agg, freq=granularity)
        df_display["conflict"] = (df_display["conflict"] / df_display["elegibles"]).fillna(0)
        self.qgrid_obj = qgrid.show_grid(df_display[["rev_time", "editor", 
                                  "adds", "dels", "reins",
                                   "productivity", "conflict",
                                   "stopwords_ratio", "main_opponent",
                                   "avg_reac_time", "revisions", "editor_id"]].set_index("rev_time").sort_index(ascending=False),
                                   grid_options={'forceFitColumns':False})
        
        display(self.qgrid_obj)
        self.out = Output()
        display(self.out)
        
        self.current_freq = granularity
        self.qgrid_obj.observe(self.on_select_change, names=['_selected_rows'])
        
        
class RevisionsManager:
    
    def __init__(self, merged_actions, merged_all_actions):
        self.actions_ex_stop = merged_actions
        self.actions_inc_stop = merged_all_actions
        
    def get_main(self, selected_date, selected_editor, freq):
        editor_revs_all = self.get_filtered_df(self.actions_inc_stop, selected_date, selected_editor, freq).reset_index(drop=True)
        editor_revs = self.get_filtered_df(self.actions_ex_stop, selected_date, selected_editor, freq).reset_index(drop=True)
        
        editor_revs_sum_all = self.get_revisions_stat(editor_revs_all, editor_revs)
        editor_revs_sum = self.get_revisions_stat(editor_revs, editor_revs)
        
        df_display = self.get_final(editor_revs_sum_all, editor_revs_sum)
        
        return df_display
        
    def get_filtered_df(self, df, input_date, editor, freq):
        years = df["rev_time"].dt.year
        months = df["rev_time"].dt.month
        days = df["rev_time"].dt.day

        mask_year = years == input_date.year
        mask_month = months == input_date.month
        mask_editor = df["editor"] == editor

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
    
    
    def get_revisions_stat(self, df_tf, df_tf_no_stopwords):
        editor_revs_tf = df_tf.copy()
        editor_revs_tf["o_rev_id"] = editor_revs_tf["o_rev_id"].astype(str)
        mask_adds = editor_revs_tf["o_rev_id"] == editor_revs_tf["revision"]
        mask_reins = (editor_revs_tf["action"] == "in") & (~mask_adds)
        mask_dels = editor_revs_tf["action"] == "out"
        mask_stopwords = ~editor_revs_tf["token"].isin(df_tf_no_stopwords["token"].unique())
        mask_survival = ~(editor_revs_tf["time_diff"].dt.days.fillna(100) < 2)
        editor_revs_tf["adds"], editor_revs_tf["dels"], editor_revs_tf["reins"] = [mask_adds.astype(int),
                                                       mask_dels.astype(int), mask_reins.astype(int)]
        editor_revs_tf["stopwords"] = mask_stopwords.astype(int)
        editor_revs_tf["adds_survive"] = (mask_adds & mask_survival).astype(int)
        editor_revs_tf["dels_survive"] = (mask_dels & mask_survival).astype(int)
        editor_revs_tf["reins_survive"] = (mask_reins & mask_survival).astype(int)

        return editor_revs_tf
    
    
    def get_final(self, revs_all, revs):
        revs_stats_all = revs_all.groupby(["revision", "rev_time"]).agg({"adds": "sum", "dels": "sum", "reins": "sum", "stopwords": "sum",
                                               "adds_survive": "sum", "dels_survive": "sum", "reins_survive": "sum"}).reset_index()

        revs_stats = revs.groupby(["revision", "rev_time"]).agg({"adds": "sum", "dels": "sum", "reins": "sum", "stopwords": "sum",
                                 "adds_survive": "sum", "dels_survive": "sum", "reins_survive": "sum","conflict": "sum"}).reset_index()
        revs_stats_all["conflict"] = revs_stats["conflict"] / (revs_stats["dels"] + revs_stats["reins"])
        revs_stats_all["conflict"] = revs_stats_all["conflict"].fillna(0)

        revs_stats_all["productivity"] = revs_stats_all.loc[:, "adds_survive": "reins_survive"].sum(axis=1) / revs_stats_all.loc[:, "adds": "reins"].sum(axis=1)
        revs_stats_all["stopwords_ratio"] = revs_stats_all["stopwords"] / revs_stats_all.loc[:, "adds": "reins"].sum(axis=1)

        display_df = revs_stats_all[["revision", "rev_time", "adds", "dels", "reins", "productivity", "conflict", "stopwords_ratio"]].sort_values("rev_time", ascending=False).set_index("rev_time")

        return display_df
        
        
        