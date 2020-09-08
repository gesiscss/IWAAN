import pandas as pd
from IPython.display import clear_output
from datetime import timedelta
from .conflict import ConflictManager


class TokensManager:
    """
    A class to detect survival states of each action (i.e. whether an add, del or reins survives
    more than 48 hours) and calculate the survival ratios grouped by each string (not token!). Core method is
    get_states().
    ...
    Attributes:
    -----------
    all_actions (pd.DataFrame): actions occurring on all tokens, including or excluding stopwords, from
                      ConflictManager.all_actions
    """
    
    def __init__(self, all_actions):
        self.all_actions = all_actions
        
        
    def get_states(self):
        """
        Provides two types of info for each action:
            1. Is this action an add, del or reins?
            2. A survival action?
        Labeled by 0-1.
        ...
        Returns:
        --------
        sample (pd.DataFrame): actions occurring on all tokens + columns labeling add/del/reins and survival state.
                      Relationships of three columns:
                      (sample["bool_adds"] + sample["bool_dels"] + sample["bool_reins"]).unique() = array([1])
        """
        sample = self.all_actions.copy().reset_index(drop=True)

        # Get differences of columns 'token_id' and 'rev_time'
        diff_actions = sample[['token_id', 'rev_time']] - sample.shift(1)[['token_id', 'rev_time']]
        diff_actions = diff_actions.rename({'token_id': 'tokenid_diff', 'rev_time': 'time_diff'}, axis=1)
        
        # Attach the differences to the original table and fill the first tokenid_diff with 1.
        sample[['tokenid_diff', 'time_diff']] = diff_actions
        sample.loc[0, "tokenid_diff"] = 1.0

        # Boolean for adds action
        bool_adds = sample['tokenid_diff'] != 0
        sample['bool_adds'] = bool_adds
        
        # Boolean for dels action
        bool_dels = sample['action'] == 'out'
        sample['bool_dels'] = bool_dels
        
        # Boolean for reins action
        sample['bool_reins'] = ~bool_adds
        sample[['bool_adds', 'bool_dels', 'bool_reins']] = sample[['bool_adds', 'bool_dels', 'bool_reins']].astype(int)
        sample['reins-dels'] = sample['bool_reins'] - sample['bool_dels']
        sample = sample.drop('bool_reins', axis=1).rename({'reins-dels':'bool_reins'}, axis=1)
        
        # Boolean for time.
        sample['time_diff'] = sample['time_diff'].shift(-1)
        
        # Label the last action for each token
        adds_index = sample[sample['bool_adds'] == 1].index
        last_index = pd.Index(list(adds_index[1:] - 1) + [sample.index[-1]])

        sample['bool_last'] = 0
        sample.loc[last_index, 'bool_last']=1
        
        # Filter out all non-last tokens that are survival after 48h.
        sample['bool_survival'] = 0
        survival_df = pd.DataFrame(
            ~(sample[sample['bool_last'] == 0]['time_diff'] < timedelta(2,0,0))).rename({'time_diff':'survival'},axis=1).astype(int)        
        survival_idx = survival_df[survival_df['survival'] == 1].index
        
        # Note that all the last actions of tokens are survival actions.
        sample.loc[survival_idx, 'bool_survival'] = 1
        sample['bool_survive'] = sample['bool_survival'] + sample['bool_last']
        sample = sample.drop(['bool_last', 'bool_survival'], axis=1)
        
        return sample
    
    
    def __action_survival(self, df_with_bools, bool_col):
        """
        Filter the table by given action type.
        ...
        Parameters:
        -----------
        df_with_bools (pd.DataFrame): actions occurring on all tokens + columns labeling add/del/reins and survival state.
        bool_col (str): column names selected from the set {'bool_adds', 'bool_dels', 'bool_reins'}
        ...
        Returns:
        -----------
        action (pd.DataFrame): dataframe of a particular action, for example, all "add" actions.
        """
        action = df_with_bools.copy()
        action['survive'] = action[bool_col] * action['bool_survive']
        action = action[action[bool_col] == 1].reset_index(drop=True)
        action = action.drop(['tokenid_diff', 'time_diff','bool_adds', 'bool_dels', 'bool_reins', 'bool_survive'], axis=1)
        action.set_index('rev_id', inplace=True)
    
        return action
    
    def token_survive(self, reduce=False):
        """
        Split the dataframe got by get_states() method into three sub-tables with 
        respective survival states, according to the types of actions.
        ...
        Parameters:
        -----------
        reduce (bool): False by default. True then only 5 selected columns will
                 be displayed.
        ...
        Returns:
        -----------
        add_actions, dels_actions, reins_actions (pd.DataFrame):
                  add/del/rein actions with surviving labels.
        
        """
        sample = self.get_states()
        
        # Survival states for all actions.
        adds_actions = self.__action_survival(sample, 'bool_adds')
        dels_actions = self.__action_survival(sample, 'bool_dels')
        reins_actions = self.__action_survival(sample, 'bool_reins')
        
        if reduce:
            cols_kept = ["rev_time", "editor", "token", "token_id", "survive"]
            adds_actions = adds_actions[cols_kept]
            dels_actions = dels_actions[cols_kept]
            reins_actions = reins_actions[cols_kept]
        
        return adds_actions, dels_actions, reins_actions
    
    def __count(self, actions):
        """
        For each string (not token!), count how many times it appears in total and has survived.
        ...
        Parameters:
        -----------
        actions (pd.DataFrame): add or del or rein actions occurring on all tokens.
        ...
        Returns:
        -----------
        total (pd.DataFrame): counts for each string in total
        survival (pd.DataFrame): counts for survival strings.
        """
        total = pd.DataFrame(actions['token'].value_counts()).reset_index().rename({'index': 'token', 'token': 'counts'}, axis=1)
        survival = pd.DataFrame(actions[actions['survive'] == 1]['token'].value_counts()).reset_index().rename(
        {'index': 'token', 'token': 'counts'}, axis=1)

        return total, survival
        
    def join_and_rank(self, total, survival, maxwords):
        """
        Join total-counts table and survival-counts table, first maxwords records.
        ...
        Parameters:
        -----------
        total (pd.DataFrame): dataframe counting total numbers of actions for this string.
        survival (pd.DataFrame): dataframe counting numbers of survival actions for this string.
        maxwords (int): only consider first 'maxwords' records of both tables.
        ...
        Returns:
        --------
        Sorted merged dataframe.
        """
        # Modify columns names
        survival = survival.rename({'counts': 'survival'}, axis=1)
        total = total.rename({'counts': 'total'}, axis=1)
        
        # Outer join the tables (first maxwords rows) for all strings and survival strings.
        merge_rough = total.iloc[:maxwords,:].merge(survival.iloc[:maxwords,:], on='token', how='outer')
        merge_rough = merge_rough.set_index('token')

        # Fill survival NaN values, i.e. these strings are in the first maxwords rows of total counts, but not in
        # the first maxwords rows of survival counts.
        sur_token_idx = merge_rough[merge_rough['survival'].isnull()].index
        survival_token = survival.set_index('token')
        sur_values = survival_token.reindex(sur_token_idx).values.reshape(-1,)

        merge_rough['survival'].loc[merge_rough['survival'].isnull()] = sur_values

        # Fill total NaN values, i.e. these strings are in the first maxwords rows of survival counts, but not in
        # the first maxwords rows of total counts.
        tot_token_idx = merge_rough[merge_rough['total'].isnull()].index
        tot_token = total.set_index('token')
        tot_values = tot_token.reindex(tot_token_idx).values.reshape(-1,)

        merge_rough['total'].loc[merge_rough['total'].isnull()] = tot_values

        # Sort
        return merge_rough.sort_values(by=['total', 'survival'], ascending=False).reset_index().rename({'index': 'token'})
        
    def get_all_tokens(self, adds, dels, reins, maxwords=100, ratio=True):
        """
        Get the most 100 (by default) active strings in terms of the actions imposed on their related tokens.
        ...
        Parameters:
        -----------
        adds/dels/reins (pd.DataFrame): add/del/rein actions with surviving labels obtained by token_survive() method.
        maxwords (int): 100 by default. The first 100 most active strings that will be displayed.
        ratio (bool): True by default. Display survival rate if True, otherwise directly display how many 
                 survival actions are there.
        ...
        Returns:
        --------
        Dataframe containing those sorted strings and their total and survival statistic for add/del/rein actions..
        
        """
        # Count token strings.
        adds_total, adds_survival = self.__count(adds)
        dels_total, dels_survival = self.__count(dels)
        reins_total, reins_survival = self.__count(reins)
        
        # The most 100 (by default) popluar tokens.
        adds_100 = self.join_and_rank(adds_total, adds_survival, maxwords)
        dels_100 = self.join_and_rank(dels_total, dels_survival, maxwords)
        reins_100 = self.join_and_rank(reins_total, reins_survival, maxwords)

        adds_100.rename({'total': 'adds', 'survival': 'adds_48h'}, inplace=True, axis=1)
        dels_100.rename({'total': 'dels', 'survival': 'dels_48h'}, inplace=True, axis=1)
        reins_100.rename({'total': 'reins', 'survival': 'reins_48h'}, inplace=True, axis=1)
        
        # Outer join three tables.
        adds_dels_rough = adds_100.merge(dels_100, on=['token'], how='outer')
        merge_init = adds_dels_rough.merge(reins_100, on=['token'], how='outer').set_index('token')
        
        # Connect each column to data.
        column_names = list(merge_init.columns)
        datasets_list = [adds_total, adds_survival, dels_total, dels_survival, reins_total, reins_survival]
        actions_datasets = dict(zip(column_names, datasets_list))

        # Fill NaN values for each type of actions.
        for col_name, data in actions_datasets.items():   
            null_idx = merge_init[merge_init[col_name].isnull()].index
            null_values = data.set_index('token').reindex(null_idx).values.reshape(-1,)

            merge_init[col_name].loc[merge_init[col_name].isnull()] = null_values
        
        # Sort the dataframe by DESC order.
        merge_init = merge_init.sort_values(by=list(merge_init.columns), ascending=False).fillna(0)
        merge_init_noratio = merge_init.copy()
        
        # Survival ratio
        ratio_columns = ['adds_48h', 'dels_48h', 'reins_48h']
        for col in ratio_columns:
            merge_init[col] = round(merge_init[col] / merge_init[col[:-4]], 2)
            merge_init[col[:-4]] = merge_init[col[:-4]].astype(int)
            merge_init.rename({col: col+'_ratio'}, axis=1, inplace=True)
        df_merge = merge_init.fillna(0)
        
        # If ratio=True, then return the ratio table; otherwise return the numbers of survival actions.
        if ratio:
            return df_merge.sort_values(by=['adds'], ascending=False)
        else:
            return merge_init_noratio