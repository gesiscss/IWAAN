import pandas as pd
import numpy as np
import plotly
from plotly import graph_objs


class ConflictCalculatorListener():

    def __init__(self, df):

        # time diff to seconds
        df['diff_secs'] = df['time_diff'].dt.total_seconds()

        # conflict time diff to seconds 
        df['diff_secs_confl'] = np.nan
        df['diff_secs_confl'] = df.loc[~df['conflict'].isnull(), 'diff_secs']

        self.df = df
        self.df_plotted = None

    def listen(self, _range1, _range2, granularity, black, red):
        df = self.df

        df = df[(df.rev_time.dt.date >= _range1) &
                (df.rev_time.dt.date <= _range2)]

        # calculate the aggreated values
        df = df.groupby(pd.Grouper(
            key='rev_time', freq=granularity[0])).agg({'conflict': ['sum', 'count'],
                                                       'action': ['count'],
                                                       'diff_secs': ['count', 'sum'],
                                                       'diff_secs_confl': ['count', 'sum']}).reset_index()

        df.loc[df[('conflict', 'count')] == 0, ('conflict', 'sum')] = np.nan
        df.loc[df[('diff_secs', 'count')] == 0, ('diff_secs', 'sum')] = np.nan
        df.loc[df[('diff_secs_confl', 'count')] == 0, ('diff_secs_confl', 'sum')] = np.nan

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
                                   showlegend=True, barmode='group', bargap=0.9)

        self.df_plotted = df

        plotly.offline.init_notebook_mode(connected=True)
        plotly.offline.iplot({"data": self.traces, "layout": layout})

    def __add_trace(self, df, metric, color):
        sel = df.index
        if metric == 'None':
            return df
        elif metric == 'Conflict Score':
            df['conflict_score'] = df[
                ('conflict', 'sum')] / df[('diff_secs', 'count')]
            sel = ~df['conflict_score'].isnull()
            y = df.loc[sel, 'conflict_score']
            self.is_norm_scale = False

        elif metric == 'Conflict Ratio':
            df['conflict_ratio'] = df[
                ('conflict', 'count')] / df[('diff_secs', 'count')]
            sel = ~(df['conflict_ratio'].isnull() |  (df[('conflict', 'count')] == 0))
            y = df.loc[sel, 'conflict_ratio']

        elif metric == 'Absolute Conflict Score':
            df['absolute_conflict_score'] = df[('conflict', 'sum')]
            sel = ~df['absolute_conflict_score'].isnull() 
            y = df.loc[sel, 'absolute_conflict_score']
            self.is_norm_scale = False

        elif metric == 'Number of Conflicts':
            df['conflict_n'] = df[('conflict', 'count')]
            sel = df['conflict_n'] != 0
            y = df.loc[sel, 'conflict_n']
            self.is_norm_scale = False

        elif metric == 'Total Elegible Actions':
            df['elegible_n'] = df[('diff_secs', 'count')]
            sel = df['elegible_n'] != 0
            y = df.loc[sel, 'elegible_n']
            self.is_norm_scale = False

        elif metric == 'Total Conflict Time':
            sel = ~df[('diff_secs_confl', 'sum')].isnull()
            y = df.loc[sel, ('diff_secs_confl', 'sum')]
            self.is_norm_scale = False

        elif metric == 'Time per Conflict Action':
            df['time_per_conflict_action'] = df[
                ('diff_secs_confl', 'sum')] / df[('diff_secs_confl', 'count')]
            sel = ~df['time_per_conflict_action'].isnull()
            y = df.loc[sel, 'time_per_conflict_action']
            self.is_norm_scale = False

        elif metric == 'Total Elegible Time':
            sel = ~df[('diff_secs', 'sum')].isnull()
            y = df.loc[sel, ('diff_secs', 'sum')]
            self.is_norm_scale = False

        elif metric == 'Time per Elegible Action':
            df['time_per_elegible_action'] = df[
                ('diff_secs', 'sum')] / df[('diff_secs', 'count')]
            sel = ~df['time_per_elegible_action'].isnull()
            y = df.loc[sel, 'time_per_elegible_action']
            self.is_norm_scale = False

        self.traces.append(
            graph_objs.Bar(
                x=df.loc[sel,'rev_time'], y=y,
                name=metric,
                marker_color=color)
        )

        return df