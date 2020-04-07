import numpy as np
import pandas as pd


class ConflictManager:

    """In charge of calculating the conflict meassurements, and all the related dataframes
    with intermediate steps.
    Attributes:
        all_content (pd.DataFrame): All content as per received through the Wikiwho Actions API
        conflicts (pd.DataFrame): The actions that have conflicts
        elegible (pd.DataFrame): Only some tokens are elegible to possible have conflicts, the
            dataframe contains all the actions of those elegible tokens
        elegible_actions (pd.DataFrame): Only the actions that are elegible to have conflicts
        revisions (pd.DataFrame): Revisions as per received through the Wikiwho Actions API
    """

    def __init__(self, all_content, revisions,lng, include_stopwords=False):
        self.all_content = all_content
        self.revisions = self.prepare_revisions(revisions)
        self.include_stopwords = include_stopwords
        self.lng = lng

    def calculate(self):

        print('Preparing elegible token actions')
        elegible = self.get_elegible()

        print('Merge elegible actions and revisions')
        elegible = self.merge_actions_and_revisions(
            elegible, self.revisions)

        print('Get the conflicts')
        self.__conflicts = self.__get_conflicts(elegible)
        
        print('Calculate time differences of undos')
        elegible = self.__calculate_time_diffs(elegible)

        print('Get elegible_actions')
        self.__elegible_actions = self.__get_elegible_actions(elegible)

        print('Calculate the token conflict')
        self.elegible = self.calculate_token_conflict_score(
            elegible, self.__conflicts)

        self.conflicts = self.elegible[self.__conflicts]
        self.elegible_actions = self.elegible[self.__elegible_actions]
        self.all_actions = self.__get_all_actions()

        return self.elegible

    def get_conflicting_actions(self, editor):
        return self.elegible[self.__conflicts.shift(-1) & (
            self.elegible.shift(-1)['editor'] == editor)]


    def prepare_revisions(self, revisions):
        revisions = revisions.rename(columns={'o_editor': 'editor'})
        revisions['rev_time'] = pd.to_datetime(revisions['rev_time'])
        return revisions
    
    def __get_all_actions(self):
        all_actions = self.fill_first_insertion(self.all_content)
        if not self.include_stopwords:
            all_actions = self.remove_stopwords(all_actions)

        all_actions = self.wide_to_long(all_actions)
        all_actions = all_actions[all_actions['rev_id'] != -1]
        return self.merge_actions_and_revisions(all_actions, self.revisions)

    def get_elegible(self):
        # by not adding the first revisions (i.e. it remains -1), the merge won't succeed; 
        # therefore the time differences of the first output will be NaN and not taken as 
        # an elegible action. The first deletion is never considered as a conflict, therefore
        # it is not elegible.

        # elegible = self.fill_first_insertion(self.all_content)

        elegible = self.remove_unique_rows(self.all_content)
        if not self.include_stopwords:
            elegible = self.remove_stopwords(elegible)
        elegible = self.wide_to_long(elegible)
        return elegible

    def fill_first_insertion(self, actions):
        """The 'in' column only contains reinsertions, the first insertion is indicated
        with -1. Nevertheless, the first insertion of the token is equal to the original 
        revision id, so here the -1s are replaced by the original revision id"""
        actions.loc[actions['in'] == -1,
                    'in'] = actions.loc[actions['in'] == -1, 'o_rev_id']
        return actions
    
    def remove_unique_rows(self, actions):
        """ A token that just have one row will nor cause any conflict neither the insertions
        or deletions can be undos, so they are removed. In order for a conflict to exist,
        there should be at least three actions, and tokens with on row only have maximum two: 
        the first insertion and a possible deletion.
        """
        return actions[actions.duplicated(subset=['token_id'], keep=False)]

    def remove_stopwords(self, actions):
        """Open a list of stop words and remove from the dataframe the tokens that 
        belong to this list.
        """
        if self.lng == 'en':
            stopwords_fn='data/stopword_list.txt'
        elif self.lng == 'de':
            stopwords_fn='data/stopword_list_de.txt'
            
        stop_words = open(stopwords_fn, 'r').read().split()
        return actions[~actions['token'].isin(stop_words)]

    def wide_to_long(self, actions):
        """ Each row in the actions data frame has an in and out column, i.e. two actions.
        This method transforms those two columns in two rows. The new dataframe will contain 
        a column `action` that indicates if it is an `in` or an `out`, and a column `rev_id` 
        that contains the revision id in which it happens (the revision ids were the values
        orginally present in the `in` and `out` columns)
        """
        return pd.wide_to_long(
            actions.rename(columns={
                'in': 'rev_id_in',
                'out': 'rev_id_out'
            }).reset_index(),
            'rev_id', 'index', 'action', sep='_', suffix='.+').reset_index(
        ).drop(columns='index').sort_values('token_id')

    def merge_actions_and_revisions(self, actions, revisions):
        """ Here the actions are merged with the revisions so that we have information about
        the time and the editor that executed the action in the token. This also returns the
        data sorted by token_id and rev_time, so it can be used to calculate time differences.
        """
        return pd.merge(actions, revisions[['rev_time', 'rev_id', 'editor']],
                        how='left', on='rev_id').sort_values(['token_id', 'rev_time'])

    def __calculate_time_diffs(self, elegible_actions):
        df = elegible_actions

        # first calculate the times for all cases. This will produce some errors because
        # the shift is not aware of the tokens (revision times should belong to the same
        # token). This errors are removed in the next lines
        
        # changed:  instead of shifting by 2, shifting by 1
        df['time_diff'] = df['rev_time'] - df.shift(1)['rev_time']
        
        # the errors are produced in the first two actions (first insertion and deletion) of
        # each token. The first insertion and deletion are guaranteed to exist because duplicates
        # were removed.
        
        to_delete = (
             #First row of each token
             (df['o_rev_id'] == df['rev_id']))
             #Second row of each token
             #(df.shift(1)['o_rev_id'] == df.shift(1)['rev_id']))

        # delete but keep the row
        df.loc[to_delete, 'time_diff'] = np.nan

        # For testing the above
        #if False:
             #this line is equivalent and clearer to the above 3 but much
             #slower)
            #df['time_diff2'] = df.groupby('token_id').apply(
                #lambda group: group['rev_time'] - group.shift(2)['rev_time']).values

             #this is for testing the two methods
            #if (df['time_diff'].fillna(-1) == df['time_diff2'].fillna(-1)).all():
                #print('Group by is equivalent to flat operations')

        return df

    def __get_conflicts(self, df):
        """ This return a selector (boolean vector) of the actions that classify as conflicts, i.e.
        1. insertion-deletion-insertion of the same token, where the editor is the same for the
        insertions but different from the deletions.
        2. delection-insertion-deletion of the same token, where the editor is the same for the
        deletions but different from the insertions.
        """
        # what it should be:
        # the token is the same as the previous
        # out editor is different from in or vice versa
        # changed: we do not consider a conflict only those actions, where the revision is made 
        #by the same user or the first insertion.

        return ((df['token_id'] == df.shift(1)['token_id']) &
                (df['editor'] != df.shift(1)['editor']))

    def __get_elegible_actions(self, df):
        """ Since the difference of time is calculated based on the 2nd previous row 
        (because  we  are looking to undos in the form of insertion-delection-insertion or 
        deletion-insertion-deletion), this means that the first two action per tokens are
        expected to be NaN (as the 2nd previous row does not exist for that token). Similarly,
        this actions should not be elegible as they have no chance of producing conflicts.
        """
        return df['time_diff'].notnull()

    def calculate_token_conflict_score(self, df, conflicts, base=3600):
        """  Although the time difference is a good indicator of conflicts, i.e. undos that take years
        are probably not very relevant, there are two important transformations in order for it to
        make sense, let t be the time difference in seconds:
        1. It needs to be the inverse of the time (i.e. 1/t), so higher value srepresent higher 
        conflicts.
        2. Calculating the log(t, base=3600) soften the curve so that the values are not so extreme. 
        Moreover, it sets 1 hour (3600 secs) as the decisive point in which an undo is more relevant.
        """
        #changed: time_diff is not calculated for the first insertion so we can emit this checking
        df['conflict'] = np.nan
        df.loc[conflicts, ['conflict']] = np.log(
            base) / np.log(df['time_diff'].astype('timedelta64[s]') + 2)

        return df

    def get_page_conflict_score(self):
        """ This calculates a total conflict score for the page. It adds all the conflicts 
        and divide them by the sum of all elegible actions (i.e. actions that have the potential
        of being undos)
        """

        if (self.elegible_actions.shape[0] == 0):
            return 0
        else:
            return (self.elegible.loc[self.__conflicts, 'conflict'].sum() /
                    self.elegible_actions.shape[0])

    #def get_page_conflict_score2(self):
        #return (self.elegible.loc[self.__conflicts, 'conflict'].sum() /
                #len(self.elegible['rev_id'] == self.elegible['o_rev_id']))

    def get_conflict_score_per_editor(self):
        """ This calculates an score per editor. It adds all the conflicts per editor, and 
        divide them by the summ of all elegible actions that belong to each editor( i.e. 
        actions that have the potential of being undos)
        """

        # calculate the number of conflicts per editor
        confs_n = self.conflicts[['editor', 'conflict']].groupby('editor').count().rename(
            columns={'conflict': 'conflict_n'})
        # calculate the accumulated conflict per editor
        confs_ed = self.conflicts[
            ['editor', 'conflict']].groupby('editor').sum()
        # calculate the 'elegible' actions per editor
        actions = self.elegible_actions[
            ['editor', 'action']].groupby('editor').count()

        # join the dataframes
        joined = confs_n.join(confs_ed).join(actions)

        # calculate the score of the editor dividing conflicts / actions
        joined['conflict_score'] = joined['conflict'] / joined['action']
        joined['conflict_ratio'] = joined['conflict_n'] / joined['action']

        # return the result sorted in descending order
        return joined.sort_values('conflict_score', ascending=False)
