import pandas as pd
import numpy as np
import plotly
from plotly import graph_objs


class ConflictsListener():

    def __init__(self, df):

        # time diff to seconds
        #df['diff_secs'] = df['time_diff'].dt.total_seconds()

        # conflict time diff to seconds 
        #df['diff_secs_confl'] = np.nan
        #df['diff_secs_confl'] = df.loc[~df['conflict'].isnull(), 'diff_secs']

        self.df = df
        self.df_plotted = None

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

        layout = graph_objs.Layout(hovermode='closest',
                                   xaxis=dict(title=granularity, ticklen=5,
                                              zeroline=True, gridwidth=2),
                                   yaxis=dict(
                                       ticklen=5, gridwidth=2, range=_range),
                                   legend=dict(x=0.5, y=1.2),
                                   showlegend=True, barmode='group', bargap=0.75)

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
