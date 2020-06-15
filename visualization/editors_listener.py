import pandas as pd

import qgrid

from IPython.display import display


class EditorsListener:
    
    def __init__(self, sources, editor_names):
        self.df = sources["agg_actions"]
        self.elegibles = sources["elegibles"]
        self.editor_names = editor_names
        
    
    def get_main(self, freq):
        df_surv_ratio = self.get_ratios(self.df, freq=freq)
        
        return df_surv_ratio
        
    
    def get_ratios(self, df_with_tf, freq):
        time_grouper = pd.Grouper(key="rev_time", freq=freq[0])
        df_ratios = df_with_tf.groupby([time_grouper, 
                             "editor", 
                             "editor_id"]).agg({key: "sum" for key in df_with_tf.columns[3:]}).reset_index()
        
        df_ratios["productivity"] = df_ratios["total_surv_48h"] / df_ratios["total"]
        df_ratios["stopwords_ratio"] = df_ratios["total_stopword_count"] / df_ratios["total"]
        
        return df_ratios
    
    def temp_pres(self, display_dict):
        self.gran_dict = display_dict
        
        agg = self.df.copy()
        self.df = agg.drop(["editor_id", "editor_str"], axis=1)
        self.df.insert(1, "editor_id" ,agg["editor_str"])
    
    def merge_main(self, df_to_merge, freq):
        df = df_to_merge.copy()
        df["rev_time"] = df["rev_time"].dt.date
        
        df_opponent = df.merge(self.gran_dict[freq][0], on=["rev_time", "editor_id"], how="left").sort_values("rev_time", ascending=False)
        final_df = df_opponent.merge(self.gran_dict[freq][1], on=["rev_time", "editor_id"], how="left").sort_values("rev_time", ascending=False)
        
        return final_df

        
    def listen(self, granularity):
        df_from_agg = self.get_main(freq=granularity)
        df_display = self.merge_main(df_from_agg, freq=granularity)
        
        qgrid_obj = qgrid.show_grid(df_display[["rev_time", "editor", 
                                  "adds", "dels", "reins",
                                   "productivity", "conflict",
                                   "stopwords_ratio", "main_opponent",
                                   "avg_rec_time", "revisions"]].set_index("rev_time").sort_index(ascending=False),
                                   grid_options={'forceFitColumns':False})
        
        display(qgrid_obj)
        