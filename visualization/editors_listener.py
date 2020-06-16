import calendar
from tqdm import tqdm
from datetime import date, timedelta
import pandas as pd

import qgrid

from IPython.display import display, clear_output

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


class EditorsListener:
    
    def __init__(self, sources):
        self.df = sources["agg_actions"]
        self.elegibles = sources["elegibles"]
        self.all_tokens = sources["token"]
        self.names_id = dict(zip(sources["agg_actions"]["editor_str"], sources["agg_actions"]["editor"]))
        
        elegible_token = self.elegibles.set_index("rev_id")
        comp_tokens = sources["token"].copy()
        comp_tokens["revision"] = comp_tokens["rev_id"].astype(str)
        comp_tokens = comp_tokens.set_index("rev_id")
        self.tokens_with_conflict = comp_tokens.merge(elegible_token, how="left") 
    
    def get_infos(self):
        monthly_df = self.get_ratios(self.df, freq="M")
        monthly_dict = self.get_daily_tokens(monthly_df, self.all_tokens)
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
    
    
    def get_daily_tokens(self, df, tokens_source, only_date=False):
        tokens_dict = {}
        for idx, row in df.iterrows():

            # Filter the tokens out.
            editor_id = row["editor_str"]
            year, month, day = row["rev_time"].year, row["rev_time"].month, row["rev_time"].day
            mask_date = within_month(tokens_source["rev_time"], date(year, month, day))
            mask_editor = tokens_source["editor"] == editor_id

            if only_date:
                mask_selected = mask_date
            else:
                mask_selected = mask_date & mask_editor

            tokens = tokens_source.loc[mask_selected].reset_index(drop=True)
            tokens_dict[idx] = tokens

        return tokens_dict
    
    
    def get_opponents(self, df, tokens_df):
        # Extract revs info.
        revs_list = df["rev_id"].astype(str).unique()

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
                opponents = self.get_opponents(value, self.tokens_with_conflict)

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
    
    
    def listen(self, granularity):
        df_from_agg = self.get_ratios(self.df, freq=granularity)
        df_from_agg = df_from_agg.rename({"editor_str": "editor_id"}, axis=1)
        df_display = self.merge_main(df_from_agg, freq=granularity)
        df_display["conflict"] = (df_display["conflict"] / df_display["elegibles"]).fillna(0)
        qgrid_obj = qgrid.show_grid(df_display[["rev_time", "editor", 
                                  "adds", "dels", "reins",
                                   "productivity", "conflict",
                                   "stopwords_ratio", "main_opponent",
                                   "avg_reac_time", "revisions", "editor_id"]].set_index("rev_time").sort_index(ascending=False),
                                   grid_options={'forceFitColumns':False})
        
        display(qgrid_obj)
        