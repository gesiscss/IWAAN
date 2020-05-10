import pandas as pd
from IPython.display import display, Markdown as md
from datetime import datetime, timedelta
import plotly.figure_factory as ff
import qgrid
import re


class ProtectListener():
    
    def __init__(self, pp_log):
        self.df = pp_log

        
    def get_protect(self, level="semi"):
        """"""
        if len(self.df) == 0:
            display(md("No protection records!"))
            return None, None
        
        df_with_expiry = self.__get_expiry()
        df_with_unknown = self.__check_unknown(df_with_expiry)
        df_checked_unprotect = self.__check_unprotect(df_with_unknown)
        df_select_level = self.__select_level(df_checked_unprotect, level=level)
        df_with_unprotect = self.__get_unprotect(df_select_level)
        
        final_table = self.__get_final(df_with_unprotect)
        plot_table = self.__get_plot(final_table, level=level)
                
        return final_table, plot_table
        
        
    def __regrex1(self, captured_content):
        reg0 = re.findall('\[(.*?)\]\ \((.*?)\)', captured_content)
        return reg0

    
    def __regrex2(self, captured_content):
        reg0 = re.findall('\[(.*?)\:(.*?)\]$', captured_content)
        reg1 = re.findall('\[(.*?)\]$', captured_content)
        if len(reg0) != 0:
            reg0[0] = (reg0[0][0] + ":" + reg0[0][1], "indefinite")
            return reg0
        else:
            reg1[0] = (reg1[0], "indefinite")
            return reg1

        
    def __extract_date(self, date_content):
        if not "indefinite" in date_content:
            extract_str = re.findall('expires\ (.*?)\ \(UTC', date_content)[0]
            return extract_str
        else:
            return (pd.Timestamp.max).to_pydatetime(warn=False).strftime("%H:%M, %-d %B %Y")
        
        
    def __check_state(self, extract):
        states_dict = {"autoconfirmed": 0, "expiry1": None,
                      "sysop": 0, "expiry2": None}

        len_extract = len(extract)
        for i in range(len_extract):
            action_tup = extract[i]
            mask_auto = "autoconfirmed" in action_tup[0]
            mask_sysop = "sysop" in action_tup[0]            

            if mask_auto:
                states_dict["autoconfirmed"] = int(mask_auto)
                states_dict["expiry1"] = self.__extract_date(action_tup[1])

            if mask_sysop:
                states_dict["sysop"] = int(mask_sysop)
                states_dict["expiry2"] = self.__extract_date(action_tup[1])
    
        return states_dict
    
    
    def __get_expiry(self):
        """"""
        protect_log = (self.df).copy()
        
        # Convert timestamp date format.
        protect_log["timestamp"] = protect_log["timestamp"].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ"))
        
        # Create an empty dict to store protection types and expiry dates.
        expiry = {}
        
        # First check "params" column.
        if "params" in protect_log.columns:
            for idx, com in protect_log['params'].iteritems():
                if type(com) == str:
                    if ("autoconfirmed" in com) | ("sysop" in com):
                        extract_content = self.__regrex1(com) if len(self.__regrex1(com)) != 0 else self.__regrex2(com)
                        expiry[idx] = self.__check_state(extract_content)
                    else:
                        pass
                else:
                    pass
        
        # Then check "comment" column.
        for idx, com in protect_log['comment'].iteritems():
            if ("autoconfirmed" in com) | ("sysop" in com):
                extract_content = self.__regrex1(com) if len(self.__regrex1(com)) != 0 else self.__regrex2(com)
                expiry[idx] = self.__check_state(extract_content)
            else:
                pass
        
        # Fill expiry into the dataframe.
        for k, v in expiry.items():
            protect_log.loc[k, "autoconfirmed"] = v["autoconfirmed"]

            if v["expiry1"] != None:
                try:
                    protect_log.loc[k, "expiry1"] = datetime.strptime(v["expiry1"], "%H:%M, %d %B %Y")
                except:
                    protect_log.loc[k, "expiry1"] = datetime.strptime(v["expiry1"], "%H:%M, %B %d, %Y")

            protect_log.loc[k, "sysop"] = v["sysop"]

            if v["expiry2"] != None:
                try:
                    protect_log.loc[k, "expiry2"] = datetime.strptime(v["expiry2"], "%H:%M, %d %B %Y")
                except:
                    protect_log.loc[k, "expiry2"] = datetime.strptime(v["expiry2"], "%H:%M, %B %d, %Y")
        
        return protect_log
    
    
    def __check_unknown(self, protect_log):
        """"""
        mask_unknown_auto = (protect_log["action"] != "unprotect") & (protect_log["autoconfirmed"].isnull())
        mask_unknown_sys = (protect_log["action"] != "unprotect") & (protect_log["sysop"].isnull())
        mask_unknown = (mask_unknown_auto & mask_unknown_sys)
        protect_log.loc[mask_unknown_auto, "autoconfirmed"] = 0
        protect_log.loc[mask_unknown_sys, "sysop"] = 0
        protect_log.loc[mask_unknown, "unknown"] = 1
        
        # Delete move action.
        protect_log = protect_log.drop(protect_log[protect_log["action"] == "move_prot"].index).reset_index(drop=True)
        
        # Fill non-unknown with 0.
        protect_log["unknown"] = protect_log["unknown"].fillna(0)
        
        return protect_log

    
    def __check_unprotect(self, protect_log):
        """"""        
        idx_unprotect = protect_log[protect_log["action"] == "unprotect"].index
        for col_name in ["autoconfirmed", "sysop", "unknown"]:
            for idx in reversed(idx_unprotect):
                if protect_log[col_name].loc[idx + 1] == 1:
                    protect_log.loc[idx, col_name] = 1
        
        return protect_log
    
    
    def __select_level(self, protect_log, level):
        """'fully', 'semi', 'unknown'"""
        
        protect_log[["autoconfirmed", "sysop"]] = protect_log[["autoconfirmed", "sysop"]].fillna(2)
        protect_auto = protect_log[protect_log["autoconfirmed"] == 1]  # Semi-protected
        protect_sys = protect_log[protect_log["sysop"] == 1]  # Fully-protected
        protect_unknown = protect_log[protect_log["unknown"] == 1]  # Unknown 

        if level == "semi":
            protect_table = protect_auto.copy()
            if "expiry1" in protect_table.columns:
                try:
                    protect_table = protect_table.drop(["autoconfirmed", "sysop", "expiry2", "unknown"], 
                                                       axis=1).rename({"expiry1": "expiry"}, axis=1)
                except KeyError:
                    protect_table = protect_table.drop(["autoconfirmed", "sysop", "unknown"], axis=1).rename({"expiry1": "expiry"}, axis=1)
            else:
                protect_table["expiry"] = pd.NaT


        elif level == "fully":
            protect_table = protect_sys.copy()
            if "expiry2" in protect_table.columns:
                try:
                    protect_table = protect_table.drop(["autoconfirmed", "sysop", "expiry1", "unknown"], 
                                                       axis=1).rename({"expiry2": "expiry"}, axis=1)
                except KeyError:        
                    protect_table = protect_table.drop(["autoconfirmed", "sysop", "unknown"], axis=1).rename({"expiry2": "expiry"}, axis=1)
            else:            
                protect_table["expiry"] = pd.NaT


        elif level == "unknown":
            protect_table = protect_unknown.copy()
            protect_table["expiry"] = pd.NaT
            try:
                protect_table = protect_table.drop(["autoconfirmed", "sysop", "expiry1", "expiry2", "unknown"], axis=1)
            except KeyError:
                try:
                    protect_table = protect_table.drop(["autoconfirmed", "sysop", "expiry1", "unknown"], axis=1)
                except KeyError:
                    protect_table = protect_table.drop(["autoconfirmed", "sysop", "expiry2", "unknown"], axis=1)
                    
        else:
            raise ValueError("Please choose one level from 'semi', 'fully' and 'unknown'.")


        protect_table = protect_table.reset_index(drop=True)
        
        return protect_table
                
                    
    def __get_unprotect(self, protect_table):
        """"""
        pp_log_shift = protect_table.shift(1)
        pp_unprotect = pp_log_shift[pp_log_shift["action"] == "unprotect"]["timestamp"]
        
        for idx, unprotect_date in pp_unprotect.iteritems():   
            protect_table.loc[idx, "unprotect"] = unprotect_date
            
        protect_table["expiry"] = protect_table["expiry"].fillna(pd.Timestamp.max.replace(second=0))
        try:
            protect_table["unprotect"] = protect_table["unprotect"].fillna(pd.Timestamp.max.replace(second=0))
        except KeyError:
            protect_table["unprotect"] = pd.Timestamp.max
            
        return protect_table
    
    
    def __get_final(self, protect_table):
        """"""
        protect_table["finish"] = protect_table[["expiry", "unprotect"]].min(axis=1).astype('datetime64[s]')
        protect_table = protect_table.drop(["expiry", "unprotect"], axis=1)
        protect_table = protect_table.drop(protect_table[protect_table["action"] == "unprotect"].index).reset_index(drop=True)
        
        inf_date = pd.Series(pd.Timestamp.max.replace(second=0)).astype('datetime64[s]').loc[0]
        now_date = pd.Series(pd.Timestamp(datetime.now())).astype("datetime64[s]").loc[0]
        
        for idx, time in protect_table["finish"].iteritems():
            if idx == 0:
                if time == inf_date:
                    protect_table.loc[idx, "finish"] = now_date
            else:
                previous_time = protect_table["finish"].loc[idx - 1]
                mask_aos = (time - previous_time) > timedelta(days=0, hours=0, minutes=0, seconds=0)
                if mask_aos:
                    protect_table.loc[idx, "finish"] = previous_time
        
        return protect_table
    
    
    def __get_plot(self, final_table, level):
        """"""
        # Level's name
        levels = {"semi": "Semi-protection", "fully": "Full-protection", "unknown": "Unknown protection"}
        
        # For Gantt chart
        protect_plot = final_table[["type", "timestamp", "finish"]].rename({"type": "Task", "timestamp": "Start", "finish": "Finish"}, axis=1)
        protect_plot["Task"] = protect_plot["Task"].replace("protect", levels[level])
        protect_plot["Resource"] = protect_plot["Task"]

        mask_null_finish = pd.isnull(protect_plot["Finish"])
        col_finish = protect_plot["Finish"].loc[mask_null_finish]
        for idx, value in col_finish.iteritems():
            if idx == 0:
                protect_plot.loc[idx, "Finish"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            else:
                protect_plot.loc[idx, "Finish"] = protect_plot["Start"].loc[idx - 1]
                
        return protect_plot

    
    
class TemplateListener():
    
    def __init__(self, all_actions, protection_plot, templates=["Featured Article", "Good Article", "Disputed", "POV", "NPOV", "Neutrality"]):
        self.df = all_actions
        self.templates = templates
        self.tl = [tl.lower().split()[0] for tl in templates]
        
        self.plot_protect = protection_plot
                
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
                                       "Semi-protection":'#939996',
                                       "Full-protection":'#939996',
                                       "Unknown protection":'#939996'}, 
                           showgrid_x=True, showgrid_y=True, bar_width=0.1, group_tasks=True, 
                           index_col='Resource', show_colorbar=False))
        else:
            display(md("No templates or protection records found!"))
            
        
        
        self.plot = plot_revs
        