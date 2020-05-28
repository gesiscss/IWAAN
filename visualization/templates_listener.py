import numpy as np
import pandas as pd
from IPython.display import display, Markdown as md, clear_output
from datetime import datetime, timedelta
import plotly.figure_factory as ff
import qgrid
import re


class ProtectListener():
    
    def __init__(self, pp_log, lng):
        self.lng = lng
        self.df = pp_log
        
        if self.lng == "en":
            self.inf_str = "indefinite"
            self.exp_str = "expires"
        elif self.lng == "de":
            self.inf_str = "unbeschränkt"
            self.exp_str = "bis"
        else:
            display(md("This language is not supported yet."))
            self.inf_str = "indefinite"
            self.exp_str = "expires"
        
    def get_protect(self, level="semi"):
        """"""
        if len(self.df) == 0:
            display(md(f"No {level} protection records!"))
            return None, pd.DataFrame(columns=["Task", "Start", "Finish", "Resource"])
        
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
            reg0[0] = (reg0[0][0] + ":" + reg0[0][1], self.inf_str)
            return reg0
        else:
            reg1[0] = (reg1[0], self.inf_str)
            return reg1

        
    def __extract_date(self, date_content):
        if not self.inf_str in date_content:
            extract_str = re.findall(f'{self.exp_str}\ (.*?)\ \(UTC', date_content)[0]              
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
    
    
    def __month_lng(self, string):
        if self.lng == "de":
            de_month = {"März": "March", "Dezember": "December", "Mär": "Mar", "Mai": "May", "Dez": "Dec", "Januar": "January", 
                    "Februar": "February", "Juni": "June", 
                    "Juli": "July", "Oktobor": "October"}
            for k, v in de_month.items():
                new_string = string.replace(k, v)
                if new_string != string:
                    break
                
            return new_string
        else:
            return string
            
    
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
                    try:
                        protect_log.loc[k, "expiry1"] = datetime.strptime(v["expiry1"], "%H:%M, %B %d, %Y")
                    except:
                        v["expiry1"] = self.__month_lng(v["expiry1"])
                        try:
                            protect_log.loc[k, "expiry1"] = datetime.strptime(v["expiry1"], "%H:%M, %d. %b. %Y")
                        except:
                            protect_log.loc[k, "expiry1"] = datetime.strptime(v["expiry1"], "%d. %B %Y, %H:%M Uhr")

            protect_log.loc[k, "sysop"] = v["sysop"]

            if v["expiry2"] != None:
                try:
                    protect_log.loc[k, "expiry2"] = datetime.strptime(v["expiry2"], "%H:%M, %d %B %Y")
                except:
                    try:
                        protect_log.loc[k, "expiry2"] = datetime.strptime(v["expiry2"], "%H:%M, %B %d, %Y")
                    except:
                        v["expiry2"] = self.__month_lng(v["expiry2"])
                        try:
                            protect_log.loc[k, "expiry2"] = datetime.strptime(v["expiry2"], "%H:%M, %d. %b. %Y")
                        except:
                            protect_log.loc[k, "expiry2"] = datetime.strptime(v["expiry2"], "%d. %B %Y, %H:%M Uhr")
                        
        
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
    
    def __init__(self, all_actions, protection_plot, lng, wikipediadv_api):
        self.df = all_actions
        self.lng = lng
        self.api = wikipediadv_api
        if lng == "en":
            self.templates = ["Featured Article", "Good Article", "Disputed", "POV", "Pov", "PoV", 
                        "NPOV", "Npov", "Neutrality", "Neutral", "Point Of View", "Systemic bias"]
        elif lng == "de":
            self.templates = ["Exzellent", "Lesenswert", "Neutralität"]
        else:
            display(md("This language is not supported yet."))
            self.templates = ["oajdfoijelkjdf"]
            
        self.tl = [tl.lower().split()[0] for tl in self.templates]        
        self.plot_protect = protection_plot
                
    def get_adjacent(self, tl):
        """Label adjacent '{{' and template name."""
        # Sort by revision time and token id.
        match_source = (self.df.copy()).sort_values(['rev_time', 'token_id'])
        
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

    
    def get_prev_rev(self, current_rev):
        tokens_all = self.df.copy()
        tokens_all = tokens_all.sort_values("rev_time").reset_index(drop=True)
        token_rev_ids = tokens_all["rev_id"].unique()

        loc_current_rev = np.where(token_rev_ids == current_rev)[0][0]
        prev_rev = token_rev_ids[loc_current_rev - 1]

        return prev_rev
    
    
    def template_capturer(self, html_content, template):
        html_content = html_content.lower()
        pat1 = f'{{<del class="diffchange diffchange-inline">{template}</del> article}}'
        pat2 = f'{{<ins class="diffchange diffchange-inline">{template}</ins> article}}'
        bool_missing = (pat1 in html_content) | (pat2 in html_content)

        return int(bool_missing)
    
    
    def get_missing_tl(self, sus):
        # Find previous rev_id.
        sus_rev_id = sus["rev_id"].unique()
        prev_ids = {key: self.get_prev_rev(key) for key in sus_rev_id}
        
        # Retrieve revision changes in the form of html.
        diff_response = {}
        for cur, prev in prev_ids.items():
            diff_response[cur] = self.api.get_talk_rev_diff(cur, prev)["*"]
        
        # Mark missing status.
        sus["missing"] = 0
        for sus_row in sus.iterrows():
            content = diff_response[sus_row[1]["rev_id"]]
            tl_token = sus_row[1]["token"]
            sus.loc[sus_row[0], "missing"] = self.template_capturer(content, tl_token)
            
        missing = sus[sus["missing"] == 1].drop("missing", axis=1)
        
        return missing
    
    
    def to_plot_df(self, standard_df, tl_idx):
        plot_df = standard_df[["action", "rev_time"]].rename({"action": "Task", "rev_time": "Start"}, axis=1)
        plot_df["Start"] = plot_df["Start"].dt.strftime('%Y-%m-%d %H:%M:%S')
        plot_df["Finish"] = plot_df.shift(-1)["Start"].fillna(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        plot_df = plot_df[plot_df["Task"] == "in"]
        plot_df["Task"].replace({"in": self.templates[tl_idx]}, inplace=True)
        plot_df["Resource"] = plot_df["Task"]
        plot_df = plot_df.reset_index(drop=True)
        
        return plot_df
        
    
    def rebuild_plot_df(self, unfinish_plot):
        mask_not_last_row = (unfinish_plot["Task"] == unfinish_plot.shift(-1)["Task"])
        mask_plus = pd.to_datetime(unfinish_plot["Finish"]) - pd.to_datetime(unfinish_plot.shift(-1)["Start"]) > timedelta()
        mask_same_start = (unfinish_plot["Start"] == unfinish_plot.shift(-1)["Start"])
        mask_to_delete = mask_not_last_row & mask_plus & mask_same_start
        
        return unfinish_plot.loc[~mask_to_delete].reset_index(drop=True)
    
    
    def listen(self):
        plot_revs = []        
        missing_revs = []
        df_templates = []
        for idx, tl in enumerate(self.tl):
            # For plot.
            captured, _, diff = self.get_template(tl)
            plot_df = self.to_plot_df(captured, idx)            
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
        
        # Capture missing values
        if len(missing_revs) != 0:
            missing_values = self.get_missing_tl(missing_revs)
            df_templates = pd.concat([missing_values, df_templates]).sort_values(["token", "rev_time"]).reset_index(drop=True)
        
        # Prepare for plotting
        plot_merge_task = plot_revs.copy()
        if self.lng == "en":
            plot_merge_task["Task"] = plot_merge_task["Task"].replace(["POV", "PoV", "Pov", "Npov", "NPOV", 
                                                   "Neutrality", "Neutral", "Point Of View"], "POV*")
        plot_merge_task["Resource"] = plot_merge_task["Task"]
        
        # Handle upgraded unknown protection while it doesn't expire.
        tasks = plot_merge_task["Task"].unique()
        if "Unknown protection" in tasks:
            unknown_start = plot_merge_task[plot_merge_task["Task"] == "Unknown protection"].iloc[0].iloc[1]
            unknown_end = plot_merge_task[plot_merge_task["Task"] == "Unknown protection"].iloc[0].iloc[2]
            unknown_end_idx = plot_merge_task[plot_merge_task["Task"] == "Unknown protection"].iloc[0].name
            if "Semi-protection" in tasks:
                protect_start = plot_merge_task[plot_merge_task["Task"] == "Semi-protection"].iloc[-1].iloc[1]
            elif "Full-protection" in tasks:
                protect_start = plot_merge_task[plot_merge_task["Task"] == "Full-protection"].iloc[-1].iloc[1]
            if (unknown_end > protect_start) & (protect_start > unknown_start):
                plot_merge_task.loc[unknown_end_idx, "Finish"] = protect_start
            else:
                pass
        else:
            pass
        
        # New plot df for missing values
        new_plot = []
        for tl in df_templates["token"].unique():
            name_idx = self.tl.index(tl)
            cap_one_tl = df_templates[df_templates["token"] == tl]
            new_plot.append(self.to_plot_df(cap_one_tl, name_idx))
            
        if len(new_plot) != 0:    
            new_plot = pd.concat(new_plot)

            semi_plot = pd.concat([plot_merge_task, new_plot]).sort_values(["Task", "Start"]).reset_index(drop=True)
            plot_merge_task = self.rebuild_plot_df(semi_plot)
                
        self.plot = plot_merge_task
        
        # Color.
        if self.lng == "en":
            templates_color = {"Featured Article": '#056ded', "Good Article": '#d9331c', "Disputed": '#ff0505',
                         "POV*": '#5cdb9a', "Systemic bias":'#02f77a', 
                         "Semi-protection":'#939996', "Full-protection":'#939996', "Unknown protection":'#939996'}
        elif self.lng == "de":
            templates_color = {"Exzellent": '#056ded', "Lesenswert": '#d9331c', "Neutralität": '#5cdb9a', 
                         "Semi-protection":'#939996', "Full-protection":'#939996', "Unknown protection":'#939996'}
        else:
            templates_color = {"Semi-protection":'#939996', "Full-protection":'#939996', "Unknown protection":'#939996'}
        
#         if len(missing_revs) !=0:
#             display(md("**Warning: there are perhaps missing records for template editing!**")) 
#             display(md("The following revisions are possibly missing:"))                
#             display(qgrid.show_grid(missing_revs))
#         else:
#             pass
                
        if len(plot_revs) != 0:
            self.cap = df_templates
            display(md("The following revisions are captured:"))
            display(qgrid.show_grid(df_templates))
            display(
                ff.create_gantt(plot_merge_task, colors=templates_color, 
                           showgrid_x=True, showgrid_y=True, bar_width=0.1, group_tasks=True, 
                           index_col='Resource', show_colorbar=False))
            if "POV*" in self.plot["Task"].unique():
                display(md("\*Includes the templates [POV/NPOV/Neutrality/Neutral/Point Of View](https://en.wikipedia.org/wiki/Template:POV)"))
        else:
            display(md("No templates or protection records found!"))
            
            
            
        
        