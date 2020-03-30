import pandas as pd
import plotly
from plotly import graph_objs


class ActionsListener():

    def __init__(self, df, editor_column='name'):
        self.df = df
        self.editor_column = editor_column
        self.df_plotted = None

    def listen(self, _range1, _range2, editor, granularity,
               black, red, blue, green):
        df = self.df

        df = df[(df.year_month.dt.date >= _range1) &
                (df.year_month.dt.date <= _range2)]

        if editor != 'All':
            df = df[df[self.editor_column] == editor]

        df = df.groupby(pd.Grouper(
            key='year_month', freq=granularity[0]+'S')).sum().reset_index()

        data = [
            graph_objs.Scatter(
                x=df['year_month'], y=df[black],
                name=black,
                marker=dict(color='rgba(0, 0, 0, 1)'))
        ]

        if red != 'None':
            data.append(graph_objs.Scatter(
                x=df['year_month'], y=df[red],
                name=red,
                marker=dict(color='rgba(255, 0, 0, .8)')))

        if blue != 'None':
            data.append(graph_objs.Scatter(
                x=df['year_month'], y=df[blue],
                name=blue,
                marker=dict(color='rgba(0, 153, 255, .8)')))           

        if green != 'None':
            data.append(graph_objs.Scatter(
                x=df['year_month'], y=df[green],
                name=green,
                marker=dict(color='rgba(0, 128, 43, 1)')))

        self.df_plotted = df

        layout = graph_objs.Layout(hovermode='closest',
                                   xaxis=dict(title=granularity, ticklen=5,
                                              zeroline=True, gridwidth=2),
                                   yaxis=dict(title='Actions',
                                              ticklen=5, gridwidth=2),
                                   legend=dict(x=0.5, y=1.2),
                                   showlegend=True, barmode='group')

        plotly.offline.init_notebook_mode(connected=True)        
        plotly.offline.iplot({"data": data, "layout": layout})
        
