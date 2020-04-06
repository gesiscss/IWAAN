import datetime

import numpy as np
import pandas as pd
import plotly
from plotly import graph_objs
from IPython.display import display, Markdown as md

class OwnedListener():

    def __init__(self, df, editor):
        self.df = df.sort_values(['token_id', 'rev_time'], ascending=True).set_index('token_id')
        self.editor = editor

        self.days = df.loc[df['o_editor'] == editor, 'rev_time'
            ].dt.to_period('D').unique()
        today = pd.Period(datetime.datetime.today(), freq='D')
        self.days = pd.Series(np.append(self.days, today)).sort_values(ascending=False)
        self.df['rev_time'] = pd.to_datetime(self.df['rev_time']).dt.tz_localize(None)

        if len(self.days) > 0:
            self.days = self.days.dt.to_timestamp('D') + pd.DateOffset(1)

            _all = []
            _abs = []
            df = self.df
            for rev_time in self.days:
                
                df = df[df['rev_time'] <= rev_time]
                last_action = df.groupby('token_id').last()
                surv = last_action[last_action['action'] != 'out']
                _abs.append(sum(surv['o_editor'] == self.editor))
                _all.append(len(surv))

            self.summ = pd.DataFrame({
                'day': self.days,
                'abs': _abs,
                'all': _all
                })
            self.summ['res'] = 100 * self.summ['abs'] / self.summ['all']
        else:
            self.summ = pd.DataFrame([], columns = ['day', 'abs', 'all', 'res'])

    def listen(self, _range1, _range2, granularity, trace):

        df = self.summ

        if len(df) == 0:
            display(md("***It is not possible to plot the tokens owned because this editor has never owned any token.***"))
            return

        df = df[(df.day.dt.date >= _range1) &
                (df.day.dt.date <= _range2 + datetime.timedelta(days=1))].copy()

        self.traces = []
        if trace == 'Tokens Owned':
            _range = None
            df['time'] = df['day'].dt.to_period(granularity[0]).dt.to_timestamp(granularity[0])
            df = df[~df.duplicated(subset='time', keep='first')]
            _y = df['abs']

        elif trace == 'Tokens Owned (%)':
            _range = [0, 100]
            df['time'] = df['day'].dt.to_period(granularity[0]).dt.to_timestamp(granularity[0])
            df = df[~df.duplicated(subset='time', keep='first')]
            _y = df['res']
            
        self.traces.append(
            graph_objs.Scatter(
                x=df['time'], y=_y,
                name=trace,
                marker=dict(color='rgba(255, 0, 0, .5)'))
        )

        layout = graph_objs.Layout(hovermode='closest',
                                   xaxis=dict(title=granularity, ticklen=5,
                                              zeroline=True, gridwidth=2),
                                   yaxis=dict(
                                       ticklen=5, gridwidth=2, range=_range),
                                   legend=dict(x=0.5, y=1.2),
                                   showlegend=True, barmode='group')

        self.df_plotted = df

        plotly.offline.init_notebook_mode(connected=True)
        plotly.offline.iplot({"data": self.traces, "layout": layout})

