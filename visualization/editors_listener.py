import calendar
from datetime import date, timedelta
import pandas as pd

import qgrid

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

def merged_tokens_and_elegibles(elegibles, tokens):
    elegible_token = elegibles.set_index("rev_id")
    comp_tokens = tokens.copy()
    comp_tokens["revision"] = comp_tokens["rev_id"].astype(str)
    comp_tokens = comp_tokens.set_index("rev_id")
    tokens_with_conflict = comp_tokens.merge(elegible_token, how="left")
    
    return tokens_with_conflict
    

class EditorsListener:
    
    def __init__(self, sources, lng, search_widget):
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
        
        self.actions["last_day_month"] = self.actions["rev_time"].dt.date.apply(get_last_date_month)
        self.actions["last_day_week"] = self.actions["rev_time"].dt.date.apply(week_get_sunday)
        self.actions["this_day"] = self.actions["rev_time"].dt.date.apply(get_same_day)
        
        
        self.selected_rev = self.all_actions["revision"].iloc[-1]
        
        self.rev_comments = dict(zip(sources["comments"]["rev_id"], sources["comments"]["comment"]))

        self.revision_manager = RevisionsManager(self.df, self.all_actions, self.actions, None, self.lng)
        
        self.search_widget = search_widget
        self.search_widget.value = self.selected_rev
        
        clear_output()
        
        
    def get_infos(self):
        monthly_dict = self.get_daily_tokens(self.tokens)
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
        opponent_info = self.get_opponents(all_revs, self.actions)
                        
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
    
    
    def on_select_revision(self, change):
        with self.out2:
            clear_output()
            self.selected_rev = self.second_qgrid.get_selected_df()["rev_id"].iloc[0]
            self.search_widget.value = self.selected_rev
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
            page_title = self.actions["article_title"].unique()[0]
            display(md(f"Within **{self.current_freq}** timeframe, you have selected **{editor_name}** (id: {editor_selected})"))
            display(HTML(f"The revisions fall in <a href='https://{self.lng}.wikipedia.org/w/index.php?date-range-to={date_selected}&tagfilter=&title={self.actions['article_title'].unique()[0]}&action=history' target='_blank'>{date_selected}</a>"))

            second_df = self.revision_manager.get_main(date_selected, editor_selected, self.current_freq)
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
        mask_date = self.opponents_info["edit_time"].astype("datetime64[ns]").dt.to_period('M') == period
        mask_editor = self.opponents_info["idx_editor"] == editor
        opponent_info = self.opponents_info[mask_date & mask_editor]
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
    
        
        
        
        
        