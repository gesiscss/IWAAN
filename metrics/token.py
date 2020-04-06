import pandas as pd
from IPython.display import clear_output
from datetime import timedelta
from .conflict import ConflictManager


class TokensManager:
    """
    """
    
    def __init__(self, all_actions, maxwords):
        self.maxwords = maxwords
        self.all_actions = all_actions
        
        
    def get_states(self):
        sample = self.all_actions.copy().reset_index(drop=True)
        
        # Get differences of columns 'token_id' and 'rev_time'
        diff_actions = sample[['token_id', 'rev_time']] - sample.shift(1)[['token_id', 'rev_time']]
        diff_actions = diff_actions.rename({'token_id': 'tokenid_diff', 'rev_time': 'time_diff'}, axis=1)

        sample[['tokenid_diff', 'time_diff']] = diff_actions

        sample.fillna(1.0, inplace=True)
        
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

        adds_index = sample[sample['bool_adds'] == 1].index
        last_index = pd.Index(list(adds_index[1:] - 1) + [adds_index[-1]])

        sample['bool_last'] = 0
        sample.loc[last_index, 'bool_last']=1

        sample['bool_survival'] = 0

        survival_df = pd.DataFrame(
            ~(sample[sample['bool_last'] == 0]['time_diff'] < timedelta(2,0,0))).rename({'time_diff':'survival'},axis=1).astype(int)

        survival_idx = survival_df[survival_df['survival'] == 1].index

        sample.loc[survival_idx, 'bool_survival'] = 1
        sample['bool_survive'] = sample['bool_survival'] + sample['bool_last']
        sample = sample.drop(['bool_last', 'bool_survival'], axis=1)
        
        return sample
    
    
    def _action_survival(self, df_with_bools, bool_col):
        action = df_with_bools.copy()
        action['survive'] = action[bool_col] & action['bool_survive']
        action = action[action[bool_col] == True].reset_index(drop=True)
        action = action.drop(['tokenid_diff', 'time_diff','bool_adds', 'bool_dels', 'bool_reins', 'bool_survive'], axis=1)
        action['survive'] = action['survive'].astype(int)
        action.set_index('rev_id', inplace=True)
    
        return action
    
    def token_survive(self):
        sample = self.get_states()
        
        # Convert all 0-1 to boolean.
        sample[['bool_adds', 'bool_dels', 'bool_reins', 'bool_survive']] = sample[
            ['bool_adds', 'bool_dels', 'bool_reins', 'bool_survive']].astype(bool)
        
        # Survival states for all actions.
        adds_actions = self._action_survival(sample, 'bool_adds')
        dels_actions = self._action_survival(sample, 'bool_dels')
        reins_actions = self._action_survival(sample, 'bool_reins')
        
        return adds_actions, dels_actions, reins_actions
        
    def _odd_true(self, number):
        """
        """
        if type(number) == int:
            if number % 2 == 0:
                return False
            else:
                return True

        elif len(number) == 1:
            if number[0] % 2 == 0:
                return False
            else:
                return True
        else:
            results = []
            for i in number:
                if i % 2 == 0:
                    results.append(False)
                else:
                    results.append(True)

            return pd.Series(results)

    
    
    def survive_fill_zeros(self, actions, actions_filtered, tokenid):
        """
        """
        idx_token_id = actions[actions['token_id'] == tokenid].index
        isin_mask = pd.Series(idx_token_id.isin(actions_filtered['rev_id']), index=idx_token_id)

        token_id_mask = pd.DataFrame(actions['token_id'] == tokenid).rename({'token_id':0}, axis=1)
        df_isin_mask = pd.DataFrame(isin_mask)
        merge_mask = token_id_mask.merge(df_isin_mask, on=['rev_id'], how='left')
        merge_mask.fillna(False, inplace=True)
        intercept_mask = merge_mask['0_x'] & merge_mask['0_y']

        actions.loc[intercept_mask, 'survive'] = 0
    
    
    def token_survive_loop(self):
        """
        """
        # Add actions.
        mask_minus_one = (self.all_actions['o_rev_id'] == self.all_actions['rev_id'])
        add_actions = self.all_actions.loc[mask_minus_one]
        add_actions.insert(10, 'survive', 1)
        add_actions = add_actions.set_index('rev_id')

        # Del actions.
        del_actions = self.all_actions[self.all_actions['action'] == 'out']
        del_actions.insert(10, 'survive', 1)
        del_actions = del_actions.set_index('rev_id')

        # Rein actions.
        rein_actions = self.all_actions[self.all_actions['action'] == 'in'].loc[~mask_minus_one]
        rein_actions.insert(10, 'survive', 1)
        rein_actions = rein_actions.set_index('rev_id')

        # Loop over token_id.
        df_unique_id = self.all_actions['token_id'].unique()
        for token_id in df_unique_id:
            df_id = self.all_actions[self.all_actions['token_id'] == token_id].reset_index(drop=True)
            time_diff = self.all_actions[self.all_actions['token_id'] == token_id]['rev_time'].diff().reset_index(drop=True).dropna()
            time_diff.index = range(0, len(time_diff))

            odd_mask = self._odd_true(time_diff.index)
            time_mask = (time_diff < timedelta(2, 0, 0))

            filter_mask_del = (odd_mask & time_mask)
            filter_mask_rein = (~odd_mask & time_mask)
            filter_mask_rein.loc[0] = False

            filter_mask_add = time_mask.copy()
            filter_mask_add.loc[1:] = False

            try:
                ori_token_filter_add = df_id.loc[filter_mask_add]
            except:
                #print(len(df_id), len(filter_mask_add))       
                filter_mask_add_append = filter_mask_add.append(pd.Series([False]), ignore_index=True)
                ori_token_filter_add = df_id.loc[filter_mask_add_append]

            try:
                ori_token_filter_del = df_id.loc[filter_mask_del]
            except:
                #print(len(df_id), len(filter_mask_del))
                filter_mask_del_append = filter_mask_del.append(pd.Series([False]), ignore_index=True)
                ori_token_filter_del = df_id.loc[filter_mask_del_append]

            try:
                ori_token_filter_rein = df_id.loc[filter_mask_rein]
            except:
                #print(len(df_id), len(filter_mask_rein))
                filter_mask_rein_append = filter_mask_rein.append(pd.Series([False]), ignore_index=True)
                ori_token_filter_rein = df_id.loc[filter_mask_rein_append]                       
                            
            if len(ori_token_filter_add) != 0:
                self.survive_fill_zeros(add_actions, ori_token_filter_add, token_id)

            if len(ori_token_filter_del) != 0:
                self.survive_fill_zeros(del_actions, ori_token_filter_del, token_id)

            if len(ori_token_filter_rein) != 0:
                self.survive_fill_zeros(rein_actions, ori_token_filter_rein, token_id)
            
            
        return add_actions, del_actions, rein_actions
    
    def count(self, actions):
        """
        """
        total = pd.DataFrame(actions['token'].value_counts()).reset_index().rename({'index': 'token', 'token': 'counts'}, axis=1)
        survival = pd.DataFrame(actions[actions['survive'] == 1]['token'].value_counts()).reset_index().rename(
        {'index': 'token', 'token': 'counts'}, axis=1)

        return total, survival
    
    
    def join_and_rank(self, total, survival):
        """
        Join tables of adds actions.
        """
        survival = survival.rename({'counts': 'survival'}, axis=1)
        total = total.rename({'counts': 'total'}, axis=1)

        merge_rough = total.iloc[:self.maxwords,:].merge(survival.iloc[:self.maxwords,:], on='token', how='outer')
        merge_rough = merge_rough.set_index('token')

        # Fill NaN for adds_survival.
        sur_token_idx = merge_rough[merge_rough['survival'].isnull()].index
        survival_token = survival.set_index('token')
        sur_values = survival_token.reindex(sur_token_idx).values.reshape(-1,)

        merge_rough['survival'].loc[merge_rough['survival'].isnull()] = sur_values

        # Fill NaN for adds_total.
        tot_token_idx = merge_rough[merge_rough['total'].isnull()].index
        tot_token = total.set_index('token')
        tot_values = tot_token.reindex(tot_token_idx).values.reshape(-1,)

        merge_rough['total'].loc[merge_rough['total'].isnull()] = tot_values

        # Sort
        return merge_rough.sort_values(by=['total', 'survival'], ascending=False).reset_index().rename({'index': 'token'})
    
    
    def get_all_tokens(self, adds, dels, reins, ratio=True):
        """
        """
        # Count token strings.
        adds_total, adds_survival = self.count(adds)
        dels_total, dels_survival = self.count(dels)
        reins_total, reins_survival = self.count(reins)
        
        # The most 100 popluar tokens.
        adds_100 = self.join_and_rank(adds_total, adds_survival)
        dels_100 = self.join_and_rank(dels_total, dels_survival)
        reins_100 = self.join_and_rank(reins_total, reins_survival)

        adds_100.rename({'total': 'adds', 'survival': 'adds_48h'}, inplace=True, axis=1)
        dels_100.rename({'total': 'dels', 'survival': 'dels_48h'}, inplace=True, axis=1)
        reins_100.rename({'total': 'reins', 'survival': 'reins_48h'}, inplace=True, axis=1)
        
        # Outer join.
        adds_dels_rough = adds_100.merge(dels_100, on=['token'], how='outer')
        merge_init = adds_dels_rough.merge(reins_100, on=['token'], how='outer').set_index('token')
        
        # Connect each column to data.
        column_names = list(merge_init.columns)
        datasets_list = [adds_total, adds_survival, dels_total, dels_survival, reins_total, reins_survival]
        actions_datasets = dict(zip(column_names, datasets_list))

        # Fill NaN of columns
        for col_name, data in actions_datasets.items():   
            null_idx = merge_init[merge_init[col_name].isnull()].index
            null_values = data.set_index('token').reindex(null_idx).values.reshape(-1,)

            merge_init[col_name].loc[merge_init[col_name].isnull()] = null_values

        merge_init = merge_init.sort_values(by=list(merge_init.columns), ascending=False).fillna(0)
        merge_init_noratio = merge_init.copy()
        
        # Survival ratio
        ratio_columns = ['adds_48h', 'dels_48h', 'reins_48h']
        for col in ratio_columns:
            merge_init[col] = round(merge_init[col] / merge_init[col[:-4]], 2)
            merge_init[col[:-4]] = merge_init[col[:-4]].astype(int)
            merge_init.rename({col: col+'_ratio'}, axis=1, inplace=True)
        df_merge = merge_init.fillna(0)
        
        if ratio:
            return df_merge.sort_values(by=['adds'], ascending=False)
        else:
            return merge_init_noratio
            
        
    
    
    

        
    
    