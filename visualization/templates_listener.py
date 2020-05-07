import pandas as pd
from IPython.display import display, Markdown as md
from datetime import datetime
import plotly.figure_factory as ff
import qgrid
import re


class ProtectListener():
    
    def __init__(self, pp_log):
        self.df = pp_log
            
    def get_protect(self):
        """"""
        if len(self.df) == 0:
            display(md("No protection records!"))
            return None, None
        
        only_expiry = self.extract_expiry()
        with_unprotect = self.get_unprotect()
        final_table = self.get_final()
        
        # For Gantt chart
        protect_plot = final_table[["type", "timestamp", "finish"]].rename({"type": "Task", "timestamp": "Start", "finish": "Finish"}, axis=1)
        protect_plot["Task"] = protect_plot["Task"].replace("protect", "Protect")
        protect_plot["Resource"] = protect_plot["Task"]
        
        mask_null_finish = pd.isnull(protect_plot["Finish"])
        col_finish = protect_plot["Finish"].loc[mask_null_finish]
        for idx, value in col_finish.iteritems():
            if idx == 0:
                protect_plot.loc[idx, "Finish"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            else:
                protect_plot.loc[idx, "Finish"] = protect_plot["Start"].loc[idx - 1]
        
        return final_table, protect_plot
        
    def extract_expiry(self):
        """"""
        protect_log = (self.df).copy()
        
        # Convert timestamp to datetime.
        protect_log["timestamp"] = protect_log["timestamp"].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ"))
        
        # Filter expiry date out.
        expiry = {}

        if "params" in protect_log.columns:
            for idx, com in protect_log['params'].iteritems():
                try:
                    expiry[idx] = re.search('\(expires\ (.*?) \(UTC\)\)', com).group(1)
                    expiry[idx] = datetime.strptime(expiry[idx], "%H:%M, %d %B %Y")
                except:
                    pass

        for idx, com in protect_log['comment'].iteritems():
            try:
                expiry[idx] = re.search('\(expires\ (.*?) \(UTC\)\)', com).group(1)
                try:
                    expiry[idx] = datetime.strptime(expiry[idx], "%H:%M, %d %B %Y")
                except:
                    expiry[idx] = datetime.strptime(expiry[idx], "%H:%M, %B %d, %Y")
            except:
                pass

        for k, v in expiry.items():
            protect_log.loc[k, "expiry"] = v

        # Do some check.
        if "expiry" in protect_log.columns:
            if protect_log["expiry"].dtype == "O":
                raise Exception("Not all datetime type in expiry column!")
        else:
            protect_log["expiry"] = pd.NaT
            
        return protect_log
    
                
    def get_unprotect(self):
        """"""
        df = self.extract_expiry()
        pp_log_shift = df.shift(1)
        pp_unprotect = pp_log_shift[pp_log_shift["action"] == "unprotect"]["timestamp"]
        
        for idx, unprotect_date in pp_unprotect.iteritems():   
            df.loc[idx, "unprotect"] = unprotect_date
            
        df["expiry"] = df["expiry"].fillna(pd.Timestamp.max)
        try:
            df["unprotect"] = df["unprotect"].fillna(pd.Timestamp.max)
        except KeyError:
            df["unprotect"] = pd.Timestamp.max
            
        return df
    
    
    def get_final(self):
        """"""
        df = self.get_unprotect()
        df["finish"] = df[["expiry", "unprotect"]].min(axis=1).replace(pd.Timestamp.max, pd.NaT)
        df = df.drop(["expiry", "unprotect"], axis=1)
        df = df.drop(df[df["action"] == "unprotect"].index)
        
        if ("params" in df.columns) & ("indefinite" in df["params"].loc[0]):
            df.loc[0, "finish"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return df



class TemplateListener():
    
    def __init__(self, all_actions, protect_logs, templates=["Featured Article", "Good Article", "Disputed", "POV", "NPOV", "Neutrality"]):
        self.df = all_actions
        self.templates = templates
        self.tl = [tl.lower().split()[0] for tl in templates]
        
        protect_instance = ProtectListener(protect_logs)
        self.protect, self.plot_protect = protect_instance.get_protect()
                
    def get_adjacent(self, tl):
        """Label adjacent '{{' and template name."""
        # Sort by revision time and token id.
        match_source = (self.df).sort_values(['rev_time', 'token_id'])
        
        # Convert boolean to 0-1.
        mask_symbol = (match_source['token'] == '{{').astype(int)
        mask_str = (match_source['token'] == tl).astype(int)

        # Label '{{' and template name and extract them.
        match_source['{{'] = mask_symbol
        match_source['tl'] = mask_str        
        match_rough = match_source[(match_source['{{'] == 1) | (match_source['tl'] == 1)].reset_index()
        
        # Label adjacency.
        mask_tokenid = ((match_rough.shift(-1)["token_id"] - match_rough["token_id"]) == 1).astype(int)
        match_rough['adjcent'] = mask_tokenid
        
        return match_rough
    
    def get_pattern(self, tl):
        """Capture '{{template_name...' pattern."""
        match_rough = self.get_adjacent(tl)
        
        # Capture adjacent tokens.
        adjcent_idx = match_rough[match_rough["adjcent"] == 1].index.union(match_rough[match_rough["adjcent"] == 1].index + 1)
        match_rough2 = match_rough.loc[adjcent_idx]
        
        # Only "{{" and template name are adjacent
        match_rough2['sum'] = match_rough2['{{'] + match_rough2.shift(-1)['tl']
        match_rough2['final'] = ((match_rough2["adjcent"] == 1) & (match_rough2["sum"] == 2)).astype(int)
        
        return match_rough2
    
    def __get_diff(self, df1, df2):
        join_df = pd.concat([df1, df2]).reset_index(drop=True)
        unique_idx = [x[0] for x in join_df.groupby(list(join_df.columns)).groups.values() if len(x) == 1]
        
        return join_df.reindex(unique_idx).sort_values(["rev_time"]).reset_index(drop=True)
    
    def get_template(self, tl):
        """Get final template data."""
        match_rough2 = self.get_pattern(tl)
        
        final_idx = match_rough2[match_rough2["final"] == 1].index.union(match_rough2[match_rough2["final"] == 1].index + 1)
        match_rough3 = match_rough2.loc[final_idx]
        
        # Is there any potential template editing history we have missed?
        match_rough = self.get_adjacent(tl)
        
        suspicious = match_rough[match_rough["token"] == tl].iloc[:, :11].drop(["index"], axis=1).reset_index(drop=True)
        captured = match_rough3[match_rough3["token"] == tl].iloc[:, :11].drop(["index"], axis=1).reset_index(drop=True)
        
        return captured, suspicious, self.__get_diff(suspicious, captured)                                            
            
    def listen(self):
        plot_revs = []        
        missing_revs = []
        df_templates = []
        for idx, tl in enumerate(self.tl):
            # For plot.
            captured, _, diff = self.get_template(tl)
            plot_df = captured[["action", "rev_time"]].rename({"action": "Task", "rev_time": "Start"}, axis=1)
            plot_df["Start"] = plot_df["Start"].dt.strftime('%Y-%m-%d %H:%M:%S')
            plot_df["Finish"] = plot_df.shift(-1)["Start"].fillna(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                       
            plot_df = plot_df[plot_df["Task"] == "in"]
            plot_df["Task"].replace({"in": self.templates[idx]}, inplace=True)
            plot_df["Resource"] = plot_df["Task"]
            plot_df = plot_df.reset_index(drop=True)
            
            plot_revs.append(plot_df)
            
            # For missing revisions.
            missing_revs.append(diff)
            
            # For captured revs.
            df_templates.append(captured)
        
        # For protection.
        plot_revs.append(self.plot_protect)
        
        plot_revs = pd.concat(plot_revs).reset_index(drop=True)
        missing_revs = pd.concat(missing_revs).reset_index(drop=True)
        df_templates = pd.concat(df_templates).reset_index(drop=True)
        
        if len(missing_revs) !=0:
            display(md("**Warning: there are perhaps missing records for template editing!**")) 
            display(md("The following revisions are potentially missing:"))                
            display(qgrid.show_grid(missing_revs))
        else:
            pass
                
        if len(plot_revs) != 0:
            display(md("The following revisions are captured:"))
            display(qgrid.show_grid(df_templates))
            display(
                ff.create_gantt(plot_revs, colors={self.templates[0]:'#056ded',
                                       self.templates[1]:'#d9331c',
                                       self.templates[2]:'#ff0505',
                                       self.templates[3]:'#5cdb9a',
                                       self.templates[4]:'#5cdb9a',
                                       self.templates[5]:'#5cdb9a',
                                       "Protect":'#939996'}, 
                           showgrid_x=True, showgrid_y=True, bar_width=0.1, group_tasks=True, 
                           index_col='Resource', show_colorbar=False))
        else:
            display(md("No templates or protection records found!"))
            
        
        
        self.plot = plot_revs
        
            
        
        
        
        
        