import numpy as np
import pandas as pd
from IPython.display import display, Markdown as md, clear_output
from datetime import datetime, timedelta
import plotly.figure_factory as ff
import qgrid
import re
from tqdm import tqdm


class ProtectListener():
    
    def __init__(self, pp_log, lng):
        """
        Class to analyse protection information.
        ...
        Attributes:
        -----------
        df (pd.DataFrame): raw data extracted from Wikipedia API.
        lng (str): langauge from {'en', 'de'}
        inf_str / exp_str (str): "indefinite" / "expires" for English
                        "unbeschränkt" / "bis" for Deutsch
        """
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
        
    def get_protect(self, level="semi_edit"):
        """
        Main function of ProtectListener.
        ...
        Parameters: 
        -----------
        level (str): select one from {"semi_edit", "semi_move", "fully_edit", "fully_move", "unknown"}
        ...
        Returns:
        -----------
        final_table (pd.DataFrame): detailed dataframe containing protection records for a particular type/level.
        plot_table (pd.DataFrame): dataframe for further Gantt Chart plotting.
        """        
        if len(self.df) == 0:
            display(md(f"No {level} protection records!"))
            return None, pd.DataFrame(columns=["Task", "Start", "Finish", "Resource"])
        else:
            self.df = self.df.drop(self.df[self.df["action"] == "move_prot"].index).reset_index(drop=True)
            if len(self.df) == 0:
                display(md(f"No {level} protection records!"))
                return None, pd.DataFrame(columns=["Task", "Start", "Finish", "Resource"])
        
        df_with_expiry = self._get_expiry()
        df_with_unknown = self._check_unknown(df_with_expiry)
        df_checked_unprotect = self._check_unprotect(df_with_unknown)
        df_select_level = self._select_level(df_checked_unprotect, level=level)
        df_with_unprotect = self._get_unprotect(df_select_level)
        
        final_table = self._get_final(df_with_unprotect)
        plot_table = self._get_plot(final_table, level=level)
                
        return final_table, plot_table
        
        
    def _regrex1(self, captured_content):
        """Called in _get_expiry() method. Capture expriry date.
        ...
        Parameters:
        -----------
        captured_content (str): contents in "params" or "comment" column
                    including "autoconfirmed" or "sysop".
        ...
        Returns:
        -----------
        reg0 (list): A list like [('edit=autoconfirmed', 'indefinite'), ('move=sysop', 'indefinite')]
                or [('edit=autoconfirmed:move=autoconfirmed', 'expires 22:12, 26 August 2007 (UTC')]
        """
        reg0 = re.findall('\[(.*?)\]\ \((.*?)\)', captured_content)
        return reg0

    
    def _regrex2(self, captured_content):
        "Called in _get_expiry() method. Capture expriry date. Parameters and returns similar as _regrex1."
        reg0 = re.findall('\[(.*?)\:(.*?)\]$', captured_content)
        reg1 = re.findall('\[(.*?)\]$', captured_content)
        if len(reg0) != 0:
            reg0[0] = (reg0[0][0] + ":" + reg0[0][1], self.inf_str)
            return reg0
        else:
            try:
                reg1[0] = (reg1[0], self.inf_str)
            except:
                pass
            
            return reg1

        
    def _extract_date(self, date_content):
        """Called in _check_state(). Extract expiry date.
        If inf, then return max Timestamp of pandas.
        """
        if not self.inf_str in date_content:
            extract_str = re.findall(f'{self.exp_str}\ (.*?)\ \(UTC', date_content)[0]              
            return extract_str
        else:
            return (pd.Timestamp.max).to_pydatetime(warn=False).strftime("%H:%M, %-d %B %Y")
        
        
    def _check_state(self, extract):
        """
        Called in _get_expiry().
        Given a list of extracted expiry date, further label it using 
        protection type ({edit, move}) and level (semi (autoconfirmed) or full (sysop)).      
        ...
        Parameters:
        -----------
        extract (list): output of _regrex1 or _regrex2
        ...
        Returns:
        -----------
        states_dict (dict): specify which level and which type, and also 
                    respective expiry date.
        """
        states_dict = {"autoconfirmed_edit": 0, "expiry1": None,
                  "autoconfirmed_move": 0, "expiry11": None,
                  "sysop_edit": 0, "expiry2": None,
                  "sysop_move": 0, "expiry21": None}
        
        len_extract = len(extract)
        for i in range(len_extract):
            action_tup = extract[i]
            mask_auto_edit = "edit=autoconfirmed" in action_tup[0]
            mask_auto_move = "move=autoconfirmed" in action_tup[0]
            mask_sysop_edit = "edit=sysop" in action_tup[0]
            mask_sysop_move = "move=sysop" in action_tup[0]

            if mask_auto_edit:
                states_dict["autoconfirmed_edit"] = int(mask_auto_edit)
                states_dict["expiry1"] = self._extract_date(action_tup[1])
            if mask_auto_move:
                states_dict["autoconfirmed_move"] = int(mask_auto_move)
                states_dict["expiry11"] = self._extract_date(action_tup[1])

            if mask_sysop_edit:
                states_dict["sysop_edit"] = int(mask_sysop_edit)
                states_dict["expiry2"] = self._extract_date(action_tup[1])
            if mask_sysop_move:
                states_dict["sysop_move"] = int(mask_sysop_move)
                states_dict["expiry21"] = self._extract_date(action_tup[1])
    
        return states_dict
    
    
    def _month_lng(self, string):
        """Called in _get_expiry. Substitute non-english month name with english one.
        For now only support DE.
        """
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
            
    
    def _get_expiry(self):
        """
        Called in get_protect(). Extract expiry time from self.df["params"] and self.df["comment"].
        ...
        Returns:
        --------
        protect_log (pd.DataFrame): expiry1: autoconfirmed_edit;expiry11: autoconfirmed_move; expiry2: sysop_edit
                        expiry21: sysop_move.
        """
        protect_log = (self.df).copy()
        self.test_log = protect_log
        
        # Convert timestamp date format.
        protect_log["timestamp"] = protect_log["timestamp"].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ"))
        
        # Create an empty dict to store protection types and expiry dates.
        expiry = {}
        
        # First check "params" column.
        if "params" in protect_log.columns:
            for idx, com in protect_log['params'].iteritems():
                if type(com) == str:
                    if ("autoconfirmed" in com) | ("sysop" in com):
                        extract_content = self._regrex1(com) if len(self._regrex1(com)) != 0 else self._regrex2(com)
                        expiry[idx] = self._check_state(extract_content)  # Which type it belongs to?
                    else:
                        pass
                else:
                    pass
        
        # Then check "comment" column.
        for idx, com in protect_log['comment'].iteritems():
            if ("autoconfirmed" in com) | ("sysop" in com):
                extract_content = self._regrex1(com) if len(self._regrex1(com)) != 0 else self._regrex2(com)
                expiry[idx] = self._check_state(extract_content)  # Which type it belongs to?
            else:
                pass
        
        # Fill expiry date into the dataframe.
        for k, v in expiry.items():
            protect_log.loc[k, "autoconfirmed_edit"] = v["autoconfirmed_edit"]

            if v["expiry1"] != None:
                try:
                    protect_log.loc[k, "expiry1"] = datetime.strptime(v["expiry1"], "%H:%M, %d %B %Y")
                except:
                    try:
                        protect_log.loc[k, "expiry1"] = datetime.strptime(v["expiry1"], "%H:%M, %B %d, %Y")
                    except:
                        v["expiry1"] = self._month_lng(v["expiry1"])
                        try:
                            protect_log.loc[k, "expiry1"] = datetime.strptime(v["expiry1"], "%H:%M, %d. %b. %Y")
                        except:
                            protect_log.loc[k, "expiry1"] = datetime.strptime(v["expiry1"], "%d. %B %Y, %H:%M Uhr")
                            
            protect_log.loc[k, "autoconfirmed_move"] = v["autoconfirmed_move"]
                            
            if v["expiry11"] != None:
                try:
                    protect_log.loc[k, "expiry11"] = datetime.strptime(v["expiry11"], "%H:%M, %d %B %Y")
                except:
                    try:
                        protect_log.loc[k, "expiry11"] = datetime.strptime(v["expiry11"], "%H:%M, %B %d, %Y")
                    except:
                        v["expiry11"] = self._month_lng(v["expiry11"])
                        try:
                            protect_log.loc[k, "expiry11"] = datetime.strptime(v["expiry11"], "%H:%M, %d. %b. %Y")
                        except:
                            protect_log.loc[k, "expiry11"] = datetime.strptime(v["expiry11"], "%d. %B %Y, %H:%M Uhr")

            protect_log.loc[k, "sysop_edit"] = v["sysop_edit"]

            if v["expiry2"] != None:
                try:
                    protect_log.loc[k, "expiry2"] = datetime.strptime(v["expiry2"], "%H:%M, %d %B %Y")
                except:
                    try:
                        protect_log.loc[k, "expiry2"] = datetime.strptime(v["expiry2"], "%H:%M, %B %d, %Y")
                    except:
                        v["expiry2"] = self._month_lng(v["expiry2"])
                        try:
                            protect_log.loc[k, "expiry2"] = datetime.strptime(v["expiry2"], "%H:%M, %d. %b. %Y")
                        except:
                            protect_log.loc[k, "expiry2"] = datetime.strptime(v["expiry2"], "%d. %B %Y, %H:%M Uhr")
                            
            protect_log.loc[k, "sysop_move"] = v["sysop_move"]
                            
            if v["expiry21"] != None:
                try:
                    protect_log.loc[k, "expiry21"] = datetime.strptime(v["expiry21"], "%H:%M, %d %B %Y")
                except:
                    try:
                        protect_log.loc[k, "expiry21"] = datetime.strptime(v["expiry21"], "%H:%M, %B %d, %Y")
                    except:
                        v["expiry21"] = self._month_lng(v["expiry21"])
                        try:
                            protect_log.loc[k, "expiry21"] = datetime.strptime(v["expiry21"], "%H:%M, %d. %b. %Y")
                        except:
                            protect_log.loc[k, "expiry21"] = datetime.strptime(v["expiry21"], "%d. %B %Y, %H:%M Uhr")
                        
        
        return protect_log
    
    
    def _check_unknown(self, protect_log):
        """
        Called in get_protect(). Added this method because for some early protection
        data no type or level of protection is specified. The type "extendedconfirmed"
        is also considered as unknown beacuase we only consider semi or full protection.
        ...
        Parameters:
        -----------
        protect_log (pd.DataFrame): output of _get_expiry.
        ...
        Returns:
        -----------
        protect_log (pd.DataFrame): dataframe in which unknown action is already labeled.
        """
        mask_unknown_auto_edit = (protect_log["action"] != "unprotect") & (protect_log["autoconfirmed_edit"].isnull())
        mask_unknown_auto_move = (protect_log["action"] != "unprotect") & (protect_log["autoconfirmed_move"].isnull())
        mask_unknown_sys_edit = (protect_log["action"] != "unprotect") & (protect_log["sysop_edit"].isnull())
        mask_unknown_sys_move = (protect_log["action"] != "unprotect") & (protect_log["sysop_move"].isnull())
        mask_extendedconfirmed = protect_log["params"].str.contains("extendedconfirmed").fillna(False)
        mask_unknown = (mask_unknown_auto_edit & mask_unknown_sys_edit & mask_unknown_auto_move & mask_unknown_sys_move)
        mask_unknown = (mask_unknown | mask_extendedconfirmed)
        protect_log.loc[mask_unknown_auto_edit, "autoconfirmed_edit"] = 0
        protect_log.loc[mask_unknown_auto_move, "autoconfirmed_move"] = 0
        protect_log.loc[mask_unknown_sys_edit, "sysop_edit"] = 0
        protect_log.loc[mask_unknown_sys_move, "sysop_move"] = 0
        protect_log.loc[mask_unknown, "unknown"] = 1
        
        # Delete move action.
        #protect_log = protect_log.drop(protect_log[protect_log["action"] == "move_prot"].index).reset_index(drop=True)
        
        # Fill non-unknown with 0.
        protect_log["unknown"] = protect_log["unknown"].fillna(0)
        
        return protect_log
    
     
    def _insert_row(self, row_number, df, row_value):
        "Called in _check_unprotect(). Function to insert row in the dataframe."
        start_upper = 0
        end_upper = row_number 
        start_lower = row_number 
        end_lower = df.shape[0]  
        upper_half = [*range(start_upper, end_upper, 1)] 
        lower_half = [*range(start_lower, end_lower, 1)] 
        lower_half = [x.__add__(1) for x in lower_half] 
        index_ = upper_half + lower_half 
        df.index = index_

        df.loc[row_number] = row_value  

        return df

    
    def _check_unprotect(self, protect_log):
        """Called in get_protect. Check which type of protection is cancelled.
        ...
        Parameters:
        -----------
        protect_log (pd.DataFrame): dataframe in which unprotect type is labeled.
        """
        # Get indices of all unprotect records.
        idx_unprotect = protect_log[protect_log["action"] == "unprotect"].index
        
        # Label which type is unprotected.
        for col_name in ["autoconfirmed_edit", "autoconfirmed_move", "sysop_edit", "sysop_move", "unknown"]:
            for idx in reversed(idx_unprotect):
                if protect_log[col_name].loc[idx + 1] == 1:
                    protect_log.loc[idx, col_name] = 1
                    
        # Deal with upgraded unknown protection, normally omitted.
        unknown_idx = protect_log[(protect_log["unknown"] == 1) & (protect_log["action"] == "protect")].index
        upgrade_sus = protect_log.loc[unknown_idx - 1]
        
        contains_upgrade = upgrade_sus[upgrade_sus["action"] == "protect"]
        if len(contains_upgrade) != 0:
            higher_level_idx = contains_upgrade.index
            upgrade_idx = higher_level_idx + 1
            
            aux_unprotect = protect_log.loc[upgrade_idx].copy()
            aux_unprotect.loc[:,"action"] = "unprotect"
            aux_unprotect.loc[:, "timestamp"] = upgrade_sus.loc[higher_level_idx]["timestamp"].values
            
            for row in aux_unprotect.iterrows():
                self._insert_row(row[0], protect_log, row[1].values)
        else:
            pass
        
        return protect_log.sort_index()
    
    
    def _select_level(self, protect_log, level):
        """
        Called in get_protect. For each level
        'fully_edit', 'fully_move', 'semi_edit', 'semit_move', 'unknown',
        pick up the expiry date for further plot.
        ...
        Parameters:
        -----------
        protect_log (pd.DataFrame): output of _check_unprotect.
        level (str): one of {"semi_edit", "semi_move", "fully_edit", "fully_move", "unknown"}.
        ...
        Returns:
        -----------
        protect_table (pd.DataFrame): 
        """
        
        protect_log[["autoconfirmed_edit",
                 "autoconfirmed_move",
                 "sysop_edit",
                 "sysop_move"]] = protect_log[["autoconfirmed_edit","autoconfirmed_move", "sysop_edit", "sysop_move"]].fillna(2)
        protect_auto_edit = protect_log[protect_log["autoconfirmed_edit"] == 1]  # Semi-protected (edit)
        protect_auto_move = protect_log[protect_log["autoconfirmed_move"] == 1]  # Semi-protected (move)
        protect_sys_edit = protect_log[protect_log["sysop_edit"] == 1]  # Fully-protected (edit)
        protect_sys_move = protect_log[protect_log["sysop_move"] == 1]  # Fully-protected (move)
        protect_unknown = protect_log[protect_log["unknown"] == 1]  # Unknown
        self.test_auto_edit = protect_auto_edit
        common_drop_cols = ["autoconfirmed_edit", "autoconfirmed_move", "sysop_edit", "sysop_move", "unknown"]
        expiry_cols = ["expiry1", "expiry11", "expiry2", "expiry21"]

        if level == "semi_edit":
            protect_table = protect_auto_edit.copy()
            if "expiry1" in protect_table.columns:
                try:
                    protect_table = protect_table.drop(common_drop_cols + ["expiry11", "expiry2", "expiry21"], axis=1).rename({"expiry1": "expiry"}, axis=1)
                except KeyError:
                    protect_table = protect_table.drop(common_drop_cols, axis=1).rename({"expiry1": "expiry"}, axis=1)
            else:
                protect_table["expiry"] = pd.NaT
                
        elif level == "semi_move":
            protect_table = protect_auto_move.copy()
            if "expiry11" in protect_table.columns:
                try:
                    protect_table = protect_table.drop(common_drop_cols + ["expiry1", "expiry2", "expiry21"], axis=1).rename({"expiry11": "expiry"}, axis=1)
                except KeyError:
                    protect_table = protect_table.drop(common_drop_cols, axis=1).rename({"expiry11": "expiry"}, axis=1)
            else:
                protect_table["expiry"] = pd.NaT
                
        elif level == "fully_edit":
            protect_table = protect_sys_edit.copy()
            if "expiry2" in protect_table.columns:
                try:
                    protect_table = protect_table.drop(common_drop_cols + ["expiry1", "expiry11", "expiry21"], axis=1).rename({"expiry2": "expiry"}, axis=1)
                except KeyError:
                    protect_table = protect_table.drop(common_drop_cols, axis=1).rename({"expiry2": "expiry"}, axis=1)
            else:
                protect_table["expiry"] = pd.NaT


        elif level == "fully_move":
            protect_table = protect_sys_move.copy()
            if "expiry21" in protect_table.columns:
                try:
                    protect_table = protect_table.drop(common_drop_cols + ["expiry1", "expiry11", "expiry2"], axis=1).rename({"expiry21": "expiry"}, axis=1)
                except KeyError:        
                    protect_table = protect_table.drop(common_drop_cols, axis=1).rename({"expiry21": "expiry"}, axis=1)
            else:            
                protect_table["expiry"] = pd.NaT


        elif level == "unknown":
            protect_table = protect_unknown.copy()
            protect_table["expiry"] = pd.NaT
            try:
                protect_table = protect_table.drop(common_drop_cols + expiry_cols, axis=1)
            except KeyError:
                try:
                    protect_table = protect_table.drop(common_drop_cols + ["expiry1"], axis=1)
                except KeyError:
                    try:
                        protect_table = protect_table.drop(common_drop_cols + ["expiry11"], axis=1)                   
                    except KeyError:
                        try:
                            protect_table = protect_table.drop(common_drop_cols + ["expiry2"], axis=1)
                        except:
                            protect_table = protect_table.drop(common_drop_cols + ["expiry21"], axis=1)
        else:
            raise ValueError("Please choose one level from 'semi_edit', 'semi_move', 'fully_edit', 'fully_move' and 'unknown'.")


        protect_table = protect_table.reset_index(drop=True)
        
        return protect_table
                
                    
    def _get_unprotect(self, protect_table):
        """Set unprotect time as a new column, in order to compare it with expiry time."""
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
    
    
    def _get_final(self, protect_table):
        """Called in get_protect(). Determine the true finish time."""
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
    
    
    def _get_plot(self, final_table, level):
        """Called in get_protect(). Get the dataframe for Gantt Chart plotting."""
        # Level's name
        levels = {"semi_edit": "Semi-protection (edit)",
              "semi_move": "Semi-protection (move)",
              "fully_edit": "Full-protection (edit)",
              "fully_move": "Full-protection (move)",
              "unknown": "Other protection"}

        # For plotly Gantt chart
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
    """
    Class to extract templates information from WikiWho and HTML content.
    ...
    Attributes:
    -----------
    df (pd.DataFrame): all actions from ConflictManager including stopwords.
    page (pd.Series): page_id, title and namespace.
    lng(str): langauge from {'en', 'de'}.
    api (Object): Wikipedia API.
    templates (list): the templates we are interested in.
    tl (list): first word of templates, in lowercase
    plot_protect (pd.DataFrame): plot dataframe from ProtectListener 
    """
    def __init__(self, all_actions, protection_plot, lng, wikipediadv_api, page):
        self.df = all_actions
        self.lng = lng
        self.api = wikipediadv_api
        self.page = page
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
        
    def get_template(self, tl):
        """Called in listener().
        Get template tokens, either certainly or suspicious.
        ...
        Parameters:
        -----------
        tl (str): first word of template, in lowercase form.
        ...
        Returns:
        -----------
        captured (pd.DataFrame): template tokens for sure.
        suspicious (pd.DataFrame): these tokens are probably template tokens.
        self._get_diff(suspicious, captured) (pd.DataFrame): tokens in suspicous
                                   but not in captured.
        """
        # Get template tokens whose ids are adjacent, also non-adjacent tokens as suspicious tokens.
        match_rough2, match_rough = self._get_pattern(tl)
        
        final_idx = match_rough2[match_rough2["final"] == 1].index.union(match_rough2[match_rough2["final"] == 1].index + 1)
        match_rough3 = match_rough2.loc[final_idx]
        
        suspicious = match_rough[match_rough["token"] == tl].iloc[:, :11].drop(["index"], axis=1).reset_index(drop=True)
        captured = match_rough3[match_rough3["token"] == tl].iloc[:, :11].drop(["index"], axis=1).reset_index(drop=True)
        
        return captured, suspicious, self._get_diff(suspicious, captured)
    
    def _get_adjacent(self, tl):
        """Called in _get_pattern().
        Label adjacent '{{' and template name whose ids are adjacent.
        ...
        Parameters:
        -----------
        tl (str): first word of template, in lowercase form.
        ...
        Returns:
        -----------
        match_rough (pd.DataFrame): column "adjcent = 1" means these two tokens
                        are adjacent; "{{ = 1" means token is "{{";
                        "tl = 1" means token is template's name.
        """
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
    
    def _get_pattern(self, tl):
        """Called in get_template(). Capture '{{template_name...' pattern.
        ...
        Parameters:
        -----------
        tl (str): first word of template, in lowercase form.
        ...
        Returns:
        -----------
        match_rough2 (pd.DataFrame): Template tokens, like '{{','good', their ids are adjacent.
        match_rough (pd.DataFrame): column "adjcent = 1" means these two tokens
                        are adjacent; "{{ = 1" means token is "{{";
                        "tl = 1" means token is template's name.
        """
        match_rough = self._get_adjacent(tl)
        
        # Capture adjacent tokens.
        adjcent_idx = match_rough[match_rough["adjcent"] == 1].index.union(match_rough[match_rough["adjcent"] == 1].index + 1)
        match_rough2 = match_rough.loc[adjcent_idx]
        
        # Only "{{" and template name are adjacent
        match_rough2['sum'] = match_rough2['{{'] + match_rough2.shift(-1)['tl']
        match_rough2['final'] = ((match_rough2["adjcent"] == 1) & (match_rough2["sum"] == 2)).astype(int)
        
        return match_rough2, match_rough
    
    def _get_diff(self, df1, df2):
        """Called in get_template(). Get the elements in df2 but not
        in df1.
        """
        join_df = pd.concat([df1, df2]).reset_index(drop=True)
        unique_idx = [x[0] for x in join_df.groupby(list(join_df.columns)).groups.values() if len(x) == 1]
        
        return join_df.reindex(unique_idx).sort_values(["rev_time"]).reset_index(drop=True)
   
    def _get_prev_rev(self, current_rev):
        """Called in get_missing_tl(). Given a revision, get previous revision.
        ...
        Parameters:
        -----------
        current_rev (str): current revision.
        ...
        Returns:
        -----------
        prev_rev (str): previous revision.
        """
        loc_current_rev = np.where(self.token_rev_ids == current_rev)[0][0]
        prev_rev = self.token_rev_ids[loc_current_rev - 1]

        return prev_rev
    
    
    def _template_capturer(self, html_content, template):
        """Called in get_missing_tl(). Capture template from HTML contents.
        ...
        Parameters:
        -----------
        html_content (str): revision difference content in HTML, extracted using
                WikipediaDV.api.get_talk_rev_diff.
        template (str): template name we want to detect.
        ...
        Returns:
        -----------
        0: not missed; 1: missed
        """
        html_content = html_content.lower()
        pat1 = f'<td class="diff-deletedline"><div>{{{{<del class="diffchange diffchange-inline">{template}</del> article}}}}'
        pat2 = f'<td class="diff-addedline"><div>{{{{<ins class="diffchange diffchange-inline">{template}</ins> article}}}}'
        pat3 = f'</a>{{{{<ins class="diffchange diffchange-inline">{template}</ins> article}}}}'
        pat4 = f'</a>{{{{<del class="diffchange diffchange-inline">{template}</del> article}}}}'
        pat5 = f'{{{{{template} article}}}}'
        pat6 = f'</a>{{{{{template} article}}}}'
        pat7 = f'<td class="diff-addedline"><div>{{{{<ins class="diffchange diffchange-inline">{template}</ins>'
        pat8 = f'<td class="diff-deletedline"><div>{{{{<del class="diffchange diffchange-inline">{template}</del>'
        pat9 = f'<td class="diff-addedline"><div>{{{{{template}'
        pat10 = f'<td class="diff-deletedline"><div>{{{{{template}'
        bool_missing = (pat1 in html_content) | (pat2 in html_content) | (pat3 in html_content) | (pat4 in html_content) | (pat5 in html_content) | (pat6 in html_content) | (pat7 in html_content) | (pat8 in html_content) | (pat9 in html_content) | (pat10 in html_content)

        return int(bool_missing)
    
    
    def get_missing_tl(self, sus):
        """Called in listener().
        Check suspicous tokens by HTML contents.
        ...
        Parameters:
        -----------
        sus (pd.DataFrame): suspicious tokens for all templates.
        ...
        Returns:
        -----------
        missing (pd.DataFrame): missing tokens that belongs to template names.
        """
        # Find previous rev_id.
        sus_rev_id = sus["rev_id"].unique()
        
        self.token_rev_ids = self.df.sort_values("rev_time").reset_index(drop=True)["rev_id"].unique()
        prev_ids = {key: self._get_prev_rev(key) for key in sus_rev_id}
        
        # Retrieve revision changes in form of HTML.
        diff_response = {}
        for cur, prev in tqdm(prev_ids.items()):
            diff_response[cur] = self.api.get_talk_rev_diff(cur, prev)["*"]
        
        # Mark missing status.
        sus["missing"] = 0
        for sus_row in sus.iterrows():
            content = diff_response[sus_row[1]["rev_id"]]
            tl_token = sus_row[1]["token"]
            sus.loc[sus_row[0], "missing"] = self._template_capturer(content, tl_token)
            
        missing = sus[sus["missing"] == 1].drop("missing", axis=1)
        
        return missing
    
    
    def _to_plot_df(self, standard_df, tl_idx):
        "Plot method called in listener()."
        plot_df = standard_df[["action", "rev_time"]].rename({"action": "Task", "rev_time": "Start"}, axis=1)
        plot_df["Start"] = plot_df["Start"].dt.strftime('%Y-%m-%d %H:%M:%S')
        plot_df["Finish"] = plot_df.shift(-1)["Start"].fillna(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        plot_df = plot_df[plot_df["Task"] == "in"]
        plot_df["Task"].replace({"in": self.templates[tl_idx]}, inplace=True)
        plot_df["Resource"] = plot_df["Task"]
        plot_df = plot_df.reset_index(drop=True)
        
        return plot_df

    def _plot_org(self, plot_df):
        "Plot method called in listener()."
        mask_move = (plot_df["Task"] == "Semi-protection (move)") | (plot_df["Task"] == "Full-protection (move)")
        mask_edit = (plot_df["Task"] == "Semi-protection (edit)") | (plot_df["Task"] == "Full-protection (edit)")
        final_plot = plot_df.loc[(~mask_move) & (~mask_edit)]

        for mask in (mask_move, mask_edit):
            if mask.sum() != 0:
                plot_slice = plot_df.loc[mask]
                plot_slice = plot_slice.sort_values("Start", ascending=False)

                time_diff = (plot_slice["Finish"] - plot_slice.shift(1)["Start"]).fillna(timedelta(-1,0,0))
                mask_adjusted = time_diff > timedelta(0,0,0)
                plot_slice.loc[mask_adjusted, "Finish"] = plot_slice.shift(1).loc[mask_adjusted, "Start"]
                final_plot = pd.concat([final_plot, plot_slice], axis=0)
                
        return final_plot
    
    
    def listen(self):
        "Listener."
        display(md("Analysing templates data..."))
        #plot_revs = []        
        missing_revs = []
        df_templates = []
        for idx, tl in enumerate(self.tl):
            # For plot.
            captured, _, diff = self.get_template(tl)
            
            # For missing revisions.
            missing_revs.append(diff)
            
            # For captured revs.
            df_templates.append(captured)
        
        missing_revs = pd.concat(missing_revs).reset_index(drop=True).drop_duplicates()
        df_templates = pd.concat(df_templates).reset_index(drop=True).drop_duplicates()
        
        clear_output()
        
        # Capture missing values
        if len(missing_revs) != 0:
            display(md("Checking if there are missing templates..."))
            missing_values = self.get_missing_tl(missing_revs)
            df_templates = pd.concat([missing_values, df_templates]).sort_values(["token", "rev_time"]).reset_index(drop=True)
            clear_output()
            display(md(f"***Page: {self.page['title']} ({self.lng.upper()})***"))
            
                
        # Create plot df for missing values
        plot = []
        for tl in df_templates["token"].unique():
            name_idx = self.tl.index(tl)
            cap_one_tl = df_templates[df_templates["token"] == tl]
            plot.append(self._to_plot_df(cap_one_tl, name_idx))
            
        # For protection.
        plot.append(self.plot_protect)
                       
        if len(plot) != 0:
            plot_merge_task = pd.concat(plot)
            #semi_plot = pd.concat([plot_merge_task, new_plot]).sort_values(["Task", "Start"]).reset_index(drop=True)
            #plot_merge_task = self.rebuild_plot_df(semi_plot) 
            
        # Handle upgraded unknown protection while it doesn't expire.
        tasks = plot_merge_task["Task"].unique()        
            
        if self.lng == "en":
            plot_merge_task["Task"] = plot_merge_task["Task"].replace(["POV", "PoV", "Pov", "Npov", "NPOV", 
                                                   "Neutrality", "Neutral", "Point Of View"], "POV*")
        plot_merge_task["Resource"] = plot_merge_task["Task"]
        
        
        self.plot = self._plot_org(plot_merge_task)
        
        
        
        # Color.
        if self.lng == "en":
            templates_color = {"Featured Article": '#056ded', "Good Article": '#d9331c', "Disputed": '#ff0505',
                         "POV*": '#5cdb9a', "Systemic bias":'#02f77a', 
                         "Semi-protection (edit)":'#262626', "Semi-protection (move)": "#939996",
                         "Full-protection (edit)":'#262626', "Full-protection (move)":'#939996', "Other protection":'#939996'}
        elif self.lng == "de":
            templates_color = {"Exzellent": '#056ded', "Lesenswert": '#d9331c', "Neutralität": '#5cdb9a', 
                         "Semi-protection (edit)":'#262626', "Semi-protection (move)": "#939996",
                         "Full-protection (edit)":'#262626', "Full-protection (move)":'#939996', "Other protection":'#939996'}
        else:
            templates_color = {"Semi-protection (edit)":'#262626', "Semi-protection (move)": "#939996",
                         "Full-protection (edit)":'#262626', "Full-protection (move)":'#939996', "Other protection":'#939996'}
        
        if len(missing_revs) !=0:
            display(md("**Warning: there are perhaps missing records for template editing!**")) 
            display(md("The following revisions are possibly missing:"))                
            display(qgrid.show_grid(missing_revs))
        else:
            pass
        
        if len(self.plot) != 0:
            display(md("The following templates are captured:"))
            display(qgrid.show_grid(df_templates))
            display(
                ff.create_gantt(self.plot, colors=templates_color, 
                           showgrid_x=True, showgrid_y=True, bar_width=0.1, group_tasks=True, 
                           index_col='Resource', show_colorbar=False))
            if "POV*" in self.plot["Task"].unique():
                display(md("\*Includes the templates [POV/NPOV/Neutrality/Neutral/Point Of View](https://en.wikipedia.org/wiki/Template:POV)"))
        else:
            display(md("No templates or protection records found!"))
            
            
            
        
        