import pandas as pd
import numpy as np
import qgrid
import plotly
from plotly import graph_objs
from IPython.display import display, Markdown as md, clear_output, HTML
from ipywidgets import Output
from utils.notebooks import get_previous_notebook

from metrics.conflict import ConflictManager


class ConflictsListener():

    def __init__(self, df, bargap=None):

        # time diff to seconds
        #df['diff_secs'] = df['time_diff'].dt.total_seconds()

        # conflict time diff to seconds 
        #df['diff_secs_confl'] = np.nan
        #df['diff_secs_confl'] = df.loc[~df['conflict'].isnull(), 'diff_secs']

        self.df = df
        self.df_plotted = None
        self.bargap = bargap

    def listen(self, _range1, _range2, granularity, black, red):
        df = self.df

        df = df[(df.year_month.dt.date >= _range1) &
                (df.year_month.dt.date <= _range2)]

        # calculate the aggreated values
        df = df.groupby(pd.Grouper(
            key='year_month', freq=granularity[0])).agg({'conflicts': ['sum'],
                                                       'elegibles': ['sum'],
                                                       'revisions': ['sum'],
                                                       'conflict': ['count', 'sum'],
                                                        'total': ['sum'],
                                                        'total_surv_48h': ['sum'],
                                                        'total_stopword_count': ['sum']}).reset_index()

        df.loc[df[('conflict', 'count')] == 0, ('conflict', 'sum')] = np.nan
        #df.loc[df[('conflicts', 'count')] == 0, ('diff_secs', 'sum')] = np.nan

        self.traces = []
        self.is_norm_scale = True
        df = self.__add_trace(df, black, 'rgba(0, 0, 0, 1)')
        df = self.__add_trace(df, red, 'rgba(255, 0, 0, .8)')

        #np.all(np.array([len(sc.x) == 1 for sc in self.traces]))

        _range = None
        if self.is_norm_scale:
            _range = [0, 1]

        # if red != 'None':
        #     data.append(graph_objs.Scatter(
        #         x=list(df['rev_time']), y=list(df[red]),
        #         name=red,
        #         marker=dict(color='rgba(255, 0, 0, .8)')))

        # if blue != 'None':
        #     data.append(graph_objs.Scatter(
        #         x=list(df['rev_time']), y=list(df[blue]),
        #         name=blue,
        #         marker=dict(color='rgba(0, 128, 43, 1)')))

        # if green != 'None':
        #     data.append(graph_objs.Scatter(
        #         x=list(df['rev_time']), y=list(df[green]),
        #         name=green,
        #         marker=dict(color='rgba(0, 153, 255, .8)')))
        
        if self.bargap == None:
            layout = graph_objs.Layout(hovermode='closest',
                                       xaxis=dict(title=granularity, ticklen=5,
                                                  zeroline=True, gridwidth=2),
                                       yaxis=dict(
                                           ticklen=5, gridwidth=2, range=_range),
                                       legend=dict(x=0.5, y=1.2),
                                       showlegend=True, barmode='group')
        else:
            layout = graph_objs.Layout(hovermode='closest',
                                       xaxis=dict(title=granularity, ticklen=5,
                                                  zeroline=True, gridwidth=2),
                                       yaxis=dict(
                                           ticklen=5, gridwidth=2, range=_range),
                                       legend=dict(x=0.5, y=1.2),
                                       showlegend=True, barmode='group', bargap=self.bargap)

        self.df_plotted = df

        plotly.offline.init_notebook_mode(connected=True)
        plotly.offline.iplot({"data": self.traces, "layout": layout})

    def __add_trace(self, df, metric, color):
        sel = df.index
        if metric == 'None':
            return df
        elif metric == 'Conflict Score':
            df['conflict_score'] = df[
                ('conflict', 'sum')] / df[('elegibles', 'sum')]
            sel = ~df['conflict_score'].isnull()
            y = df.loc[sel, 'conflict_score']
            self.is_norm_scale = False

        elif metric == 'Conflict Ratio':
            df['conflict_ratio'] = df[
                ('conflicts', 'sum')] / df[('elegibles', 'sum')]
            sel = ~(df['conflict_ratio'].isnull() |  (df[('conflict', 'count')] == 0))
            y = df.loc[sel, 'conflict_ratio']

        elif metric == 'Absolute Conflict Score':
            df['absolute_conflict_score'] = df[('conflict', 'sum')]
            sel = ~df['absolute_conflict_score'].isnull() 
            y = df.loc[sel, 'absolute_conflict_score']
            self.is_norm_scale = False

        elif metric == 'Number of Conflicts':
            df['conflict_n'] = df[('conflicts', 'sum')]
            sel = df['conflict_n'] != 0
            y = df.loc[sel, 'conflict_n']
            self.is_norm_scale = False

        elif metric == 'Total':
            df['total_n'] = df[('total', 'sum')]
            sel = df['total_n'] != 0
            y = df.loc[sel, 'total_n']
            self.is_norm_scale = False
            
        elif metric == 'Total_surv_48h':
            df['total_surv_48h_n'] = df[('total_surv_48h', 'sum')]
            sel = df['total_surv_48h_n'] != 0
            y = df.loc[sel, 'total_surv_48h_n']
            self.is_norm_scale = False

        elif metric == 'Total_persistent':
            df['total_persistent_n'] = df[('total_persistent', 'sum')]
            sel = df['total_persistent_n'] != 0
            y = df.loc[sel, 'total_persistent_n']
            self.is_norm_scale = False
            
        elif metric == 'Total_stopword_count':
            df['total_stopword_count_n'] = df[('total_stopword_count', 'sum')]
            sel = df['total_stopword_count_n'] != 0
            y = df.loc[sel, 'total_stopword_count_n']
            self.is_norm_scale = False

        elif metric == 'Total Elegible Actions':
            df['elegibles_n'] = df[('elegibles', 'sum')]
            sel = df['elegibles_n'] != 0
            y = df.loc[sel, 'elegibles_n']
            self.is_norm_scale = False

#         self.traces.append(
#             graph_objs.Scatter(
#                 x=df.loc[sel,'year_month'], y=y,
#                 name=metric,
#                 marker=dict(color=color), mode='markers')
#         )

        self.traces.append(
            graph_objs.Bar(
                x=df.loc[sel,'year_month'], y=y,
                name=metric, marker_color=color)
        )

        return df
    
    
    
class ConflictsActionListener():
    
    def __init__(self, sources, lng):
        self.sources=sources
        self.lng = lng
        
    def listen(self, stopwords):
        
        # Get source data. 
        if stopwords == 'Not included':
            cp_all = self.sources['All content'].copy()
            cp_revisions = self.sources['Revisions'].copy()
            conflict_calculator = ConflictManager(cp_all, cp_revisions, lng=self.lng)
        else:
            cp_all = self.sources['All content'].copy()
            cp_revisions = self.sources['Revisions'].copy()
            conflict_calculator = ConflictManager(cp_all, cp_revisions, lng=self.lng, include_stopwords=True)
            
        conflict_calculator.calculate()
        self.conflict_calculator = conflict_calculator
        clear_output()

        # display the tokens, the difference in seconds and its corresponding conflict score
        conflicts = conflict_calculator.conflicts.copy()
        conflicts['time_diff_secs'] = conflicts['time_diff'].dt.total_seconds()
        self.only_conflicts = conflicts

        if len(conflicts) > 0:
            conflicts_for_grid = conflicts[[
                'action', 'token', 'token_id', 'rev_id', 
                'editor', 'time_diff_secs', 'conflict']].rename(columns={
                'editor': 'editor_id'}).sort_values('conflict', ascending=False)
            conflicts_for_grid['token_id'] = conflicts_for_grid['token_id'].astype(str)
            conflicts_for_grid['rev_id'] = conflicts_for_grid['rev_id'].astype(str)
            conflicts_for_grid.set_index('token_id', inplace=True)
            self.df_for_grid = conflicts_for_grid
            display(qgrid.show_grid(self.df_for_grid))
        else:
            display(md(f'**There are no conflicting tokens in this page.**'))
            display(HTML(f'<a href="{get_previous_notebook()}" target="_blank">Go back to the previous workbook</a>'))
