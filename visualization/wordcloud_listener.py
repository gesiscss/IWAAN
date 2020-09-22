import copy
import qgrid
import pandas as pd
import matplotlib.pyplot as plt

from IPython.display import display, Markdown as md, clear_output
from ipywidgets import Output, fixed
from .wordclouder import WordClouder
from .editors_listener import remove_stopwords

from metrics.token import TokensManager
from metrics.conflict import ConflictManager


class WCListener():

    def __init__(self, sources, lng, specific_editor=None, conflict_editor=None, max_words=100):
        self.sources = sources
        self.max_words = max_words
        self.lng = lng
        self.specific_editor = specific_editor
        self.conflict_editor = conflict_editor
               

    def listen(self, _range1, _range2, editor, source, action, stopwords):
        # Get source data through ConflictManager.
        if stopwords == "Not included":
            self.source_data = {
                'All Actions': remove_stopwords(self.sources["tokens_source"]["tokens_all"], self.lng),
                'Elegible Actions': remove_stopwords(self.sources["tokens_source"]["elegibles_all"], self.lng),
                'Only Conflicts': remove_stopwords(self.sources["tokens_source"]["conflicts_all"], self.lng)
            }
        else:
            self.source_data = {
                'All Actions': self.sources["tokens_source"]["tokens_all"],
                'Elegible Actions': self.sources["tokens_source"]["elegibles_all"],
                'Only Conflicts': self.sources["tokens_source"]["conflicts_all"]
            }
            
          
        df = self.source_data[source]
            

        df = df[(df.rev_time.dt.date >= _range1) &
                (df.rev_time.dt.date <= _range2)]

        if action == 'Just Insertions':
            df = df[df['action'] == 'in']
        elif action == 'Just Deletions':
            df = df[df['action'] == 'out']

        if editor != 'All':
            df = df[df['name'] == editor]

        if len(df) == 0:
            display(md(f"**There are no words to build the word cloud.**"))
            return 0

        df_in = df[df['action'] == 'in']['token'] + '+'
        df_out = df[df['action'] == 'out']['token'] + '-'
        in_out = pd.concat([df_in, df_out])

        word_counts = in_out.value_counts()[:self.max_words]
        colors = {'+': '#003399', '-': '#CC3300'}

        # Create word cloud
        wc = WordClouder(word_counts, colors, self.max_words)

        try:
            wcr = wc.get_wordcloud()
            display(md(f"**Only top {self.max_words} most frequent conflicting words displayed.**"))

            # Revisions involved
            display(md(f"### The below token conflicts ocurred in a total of {len(df['rev_id'].unique())} revisions:"))

            # Plot
            plt.figure(figsize=(14, 7))
            plt.imshow(wcr, interpolation="bilinear")
            plt.axis("off")
            plt.show()

        except ValueError:
            display(
                md("Cannot create the wordcloud, there were zero conflict tokens."))

class WCActionsListener():
    """
    Class for displaying most frequently changed words.
    ...
    Attributes:
    -----------
    sources (dict): contains two parts: df "tokens_all" (all actions from ConflictManager) and
            dict "tokens_inc_stop" (adds/dels/reins with survival states from TokensManager)
    max_words (int): displaying top max_words frequently changed words. 100 by default.
    lng (str): {'en', 'de'}
    _
    """
    def __init__(self, sources, lng, max_words=100):
        self.max_words = max_words
        self.sources = sources
        self.lng=lng
        
    def _select_token(self, string, range1, range2):
        """
        Called in token_selection_change(). Get string's editing history under
        a given time frame.
        ...
        Attributes:
        -----------
        string (str): string by manul selection.
        range1 (datetime.datetime): date begin.
        range2 (datetime.datetime): date ends.
        ...
        Returns:
        -----------
        pd.DataFrame displaying string's history
        """
        token_source = self.sources["tokens_source"]["tokens_all"]          
        ranged_token = token_source[(token_source['rev_time'].dt.date >= range1)\
                          & (token_source['rev_time'].dt.date <= range2)]
        
        return ranged_token[ranged_token['token'] == string]
            
        
                        
    def revid_selection_change(self, change):
        "Second click."
        with self.out2:
            clear_output()
            selected_df = self.qgrid_selected_token.get_selected_df()
            if len(selected_df) == 0:
                print('Please select a revision!')
            else:
                rev_selected = self.qgrid_selected_token.get_selected_df().reset_index()['rev_id'].iloc[0]
                url = f'https://{self.lng}.wikipedia.org/w/index.php?title=TITLEDOESNTMATTER&diff={rev_selected}&diffmode=source'
                print(url)
                   
    def token_selection_change(self, change):
        "First click."
        with self.out1:
            clear_output()

            # Process the involved dataframe.
            token_selected = self.qgrid_token_obj.get_selected_df().reset_index()['string'].iloc[0]
            selected_token = self._select_token(token_selected, self._range1, self._range2)
            df_selected_token = selected_token.drop(['page_id', 'o_editor', 'token', 'o_rev_id', 'article_title'], axis=1)
            new_cols = ['token_id', 'action', 'rev_time', 'editor', 'rev_id']
            df_selected_token = df_selected_token[new_cols].rename({'editor': 'editor_id'}, axis=1)
            df_selected_token['token_id'] = df_selected_token['token_id'].astype(str)
            df_selected_token['rev_id'] = df_selected_token['rev_id'].astype(str)
            df_selected_token.set_index('token_id', inplace=True)

            qgrid_selected_token = qgrid.show_grid(df_selected_token)
            self.qgrid_selected_token = qgrid_selected_token
            display(md(f'**With string *{token_selected}*, select one revision you want to investigate:**'))
            display(self.qgrid_selected_token)
            
            self.out2 = Output()
            display(self.out2)
            self.qgrid_selected_token.observe(self.revid_selection_change, names=['_selected_rows'])

    
    def listen(self, _range1, _range2, action, stopwords):
        """
        Listener.
        """
        if (len(str(_range1.year)) < 4) | (len(str(_range2.year)) < 4):
            return display(md("Please input the correct year format!"))
                       
       
        # Get source data.
        if stopwords == 'Not included':
            self.token_calculator = TokensManager(remove_stopwords(self.sources["tokens_source"]["tokens_all"], self.lng))
            actions_no_sw = remove_stopwords(self.sources["tokens_inc_stop"], self.lng)
            add_actions = actions_no_sw["adds"]
            del_actions = actions_no_sw["dels"]
            rein_actions = actions_no_sw["reins"]
        else:
            self.token_calculator = TokensManager(self.sources["tokens_source"]["tokens_all"])
            add_actions = self.sources["tokens_inc_stop"]["adds"]
            del_actions = self.sources["tokens_inc_stop"]["dels"]
            rein_actions = self.sources["tokens_inc_stop"]["reins"]
        
        self._range1 = copy.copy(_range1)
        self._range2 = copy.copy(_range2)
        adds = add_actions[(add_actions['rev_time'].dt.date >= _range1) & (add_actions['rev_time'].dt.date <= _range2)]
        dels = del_actions[(del_actions['rev_time'].dt.date >= _range1) & (del_actions['rev_time'].dt.date <= _range2)]
        reins = rein_actions[(rein_actions['rev_time'].dt.date >= _range1) & (rein_actions['rev_time'].dt.date <= _range2)]

        tokens_action_no_ratio = self.token_calculator.get_all_tokens(adds, dels, reins, maxwords=self.max_words, ratio=False)

        symbol_dict = {'adds': '+', 'adds_48h': '!', 'dels': '-', 'dels_48h': '@', 'reins': '*', 'reins_48h': '#'}
        if action == 'All':
            long_list = []
            tokens_for_wc = tokens_action_no_ratio.rename(symbol_dict, axis=1)
            for col in list(tokens_for_wc.columns):
                tokens_for_wc[col].index = tokens_for_wc[col].index + f'{col}'
                long_list.append(tokens_for_wc[col])
            df = pd.concat(long_list)
        else:
            symbol = symbol_dict[action]
            tokens_for_wc = tokens_action_no_ratio.rename({action: symbol}, axis=1)
            tokens_for_wc[symbol].index = tokens_for_wc[symbol].index + symbol    
            df = tokens_for_wc[symbol]

        if len(df) == 0:
            display(md(f"**There are no words to build the word cloud.**"))

        colors = {'+': '#003399', '!': '#0099ff', '-': '#CC3300', 
              '@': '#CC6633', '*': '#00ffcc', '#':'#00ff33'}

        # Create word cloud
        wc = WordClouder(df, colors, 5000)

        try:
            wcr = wc.get_wordcloud()
            display(md(f"**Only top {self.max_words} most frequent words displayed.**"))

            # Plot
            plt.figure(figsize=(14, 7))
            plt.imshow(wcr, interpolation="bilinear")
            plt.axis("off")
            plt.show()

        except ValueError:
            display(
                md("Cannot create the wordcloud, there were zero actions."))

        tokens_action = self.token_calculator.get_all_tokens(adds, dels, reins)
        tokens_action.index = tokens_action.index.rename("string")
               
        tokens_action.reset_index(inplace=True)        
        mask_special = tokens_action["string"] == "<!--"
        tokens_action = tokens_action[~mask_special]
        tokens_action.set_index("string", inplace=True)
        
        if len(tokens_action) != 0:
            qgrid_token_obj = qgrid.show_grid(tokens_action,grid_options={'forceFitColumns':True})
            self.qgrid_token_obj = qgrid_token_obj
            display(md('**Select one string you are interested in:**'))
            display(self.qgrid_token_obj)

            self.out1 = Output()
            display(self.out1)
            self.qgrid_token_obj.observe(self.token_selection_change, names=['_selected_rows'])            
            #return qgrid_token_obj
        else:
            display(md('**There are no words to build the table.**'))
            
            

        
            

        
        
        
            
