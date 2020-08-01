#!/usr/bin/python
# -*- coding: utf-8 -*-

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

db = 'zen'
db_config = {'user': 'my_user',
             'pwd': 'my_user_password',
             'host': 'localhost',
             'port': 5432,
             'db': db}
engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
                                                            db_config['pwd'],
                                                            db_config['host'],
                                                            db_config['port'],
                                                            db_config['db']))

# получаем данные
query = '''
            SELECT * FROM dash_visits
        '''
dash_visits = pd.io.sql.read_sql(query, con = engine)
dash_visits['dt'] = pd.to_datetime(dash_visits['dt'])

query = '''
            SELECT * FROM dash_engagement
        '''
dash_engagement = pd.io.sql.read_sql(query, con = engine)
dash_engagement['dt'] = pd.to_datetime(dash_engagement['dt']).dt.round('min')

# note = '''
#           Этот дашборд показывает: сколько взаимодействий пользователей с карточками происходит в системе с разбивкой по темам карточек, 
#           как много событий генерируют источники с разными темами,
#           насколько хорошо пользователи конвертируются из показов карточек в просмотры статей. 
#           Используйте выбор интервала даты показа, возрастных категорий и тем карточек для управления дашбордом.
#        '''

# задаём лейаут
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets, compress=False)
app.layout = html.Div(children=[      
    # формируем html
    html.H1(children = 'Анализ взаимодействия пользователей с карточками статей в Яндекс.Дзен'),
    html.H5(children = 'Этот дашборд показывает:'),
    html.H5(children = '    * сколько взаимодействий пользователей с карточками происходит в системе с разбивкой по темам карточек,'),
    html.H5(children = '    * как много событий генерируют источники с разными темами,'),
    html.H5(children = '    * насколько хорошо пользователи конвертируются из показов карточек в просмотры статей.'),
    html.Label('Используйте выбор интервала даты показа, возрастных категорий и тем карточек для управления дашбордом.'),
    #html.Label(note),
    html.Br(),
    html.Div([
        
        html.Div([
            # выбор временного периода
            html.Label('Дата:'),
            dcc.DatePickerRange(
                start_date = dash_visits['dt'].min(),
                end_date = dash_visits['dt'].max(),
                display_format = 'YYYY-MM-DD',
                id = 'dt_selector',       
                ),
            html.Br(),
            html.Br(),
            html.Label('Возрастные категории:'),
            dcc.Dropdown(
                options = [{'label': x, 'value': x} for x in dash_visits['age_segment'].unique()],
                value = dash_visits['age_segment'].unique().tolist(),
                multi = True,
                id = 'age-dropdown'
                ),                               
            ], className = 'six columns'),
        
        html.Div([
            html.Label('Темы карточек:'),
            dcc.Dropdown(
                options = [{'label': x, 'value': x} for x in dash_visits['item_topic'].unique()],
                value = dash_visits['item_topic'].unique().tolist(),
                multi = True,
                id = 'item-topic-dropdown'
                ),
            ], className = 'six columns'),

        ], className = 'row'),

    html.Br(),

    html.Div([
        
        html.Div([
            html.H5('История событий по темам карточек:'),
            dcc.Graph(style = {'height': '50vw'},
                id = 'history-absolute-visits'
                ),  
            ], className = 'six columns'),            
        
        html.Div([
            html.H5('Разбивка событий по темам источников:'),
            dcc.Graph(style = {'height': '25vw'},
                id = 'pie-visits'
                ),
            html.H5('Глубина взаимодействия'),
            html.Label('среднее количество пользователей в минуту:'),
            dcc.Graph(style = {'height': '25vw'},
                id = 'engagement-graph'
                ),
            ], className = 'six columns'),        
        
        ], className = 'row'),
    html.Br(),
    html.Br(),
    html.Br(),
    html.Br(),

    ])

# описываем логику дашборда
@app.callback(
    [Output('history-absolute-visits', 'figure'),
     Output('pie-visits', 'figure'),
     Output('engagement-graph', 'figure'),
    ],
    [Input('item-topic-dropdown', 'value'),
     Input('age-dropdown', 'value'),
     Input('dt_selector', 'start_date'),
     Input('dt_selector', 'end_date'),
    ])
def update_figures(selected_item_topics, selected_ages, start_date, end_date):

    # График истории событий по темам карточек
    items = (dash_visits
        .query('item_topic.isin(@selected_item_topics) and \
            dt >= @start_date and dt <= @end_date and \
            age_segment.isin(@selected_ages)')
        .groupby(['item_topic', 'dt'], as_index=False)
        .agg({'visits': 'sum'})
        .sort_values('visits', ascending=False)
        )
    data_by_item_topic = []
    for item_topic in items['item_topic'].unique():
        data_by_item_topic += [go.Scatter(x = items.query('item_topic == @item_topic')['dt'],
                                   y = items.query('item_topic == @item_topic')['visits'],
                                   mode = 'lines',
                                   stackgroup = 'one',
                                   name = item_topic)]

    # График разбивки событий по темам источников
    report = (dash_visits
        .query('item_topic.isin(@selected_item_topics) and \
            dt >= @start_date and dt <= @end_date and \
            age_segment.isin(@selected_ages)')
        .groupby('source_topic', as_index=False)
        .agg({'visits': 'sum'})
        )
    data_by_source_topic = [go.Pie(labels = report['source_topic'],
        values = report['visits'], 
        showlegend = False,
        textposition = 'outside',
        texttemplate = '%{label} %{percent:.1%}',
        title_position = 'top center'
        )]

    # График средней глубины взаимодействия
    report = (dash_engagement
        .query('item_topic.isin(@selected_item_topics) and \
            dt >= @start_date and dt <= @end_date and \
            age_segment.isin(@selected_ages)')
        .groupby('event', as_index=False)
        .agg({'unique_users': 'mean'})
        .rename(columns={'unique_users': 'avg_unique_users'})
        .sort_values('avg_unique_users', ascending=False)
        )
    data_by_event = [go.Bar(x = report['event'],
        y = report['avg_unique_users'],
        text = report['avg_unique_users'].round(1),
        textposition = 'auto',
        width = 0.5)]

    # формируем результат для отображения
    return (
            {
            'data': data_by_item_topic,
            'layout': go.Layout(xaxis = {'title': 'Дата'},
                                yaxis = {'title': 'Количество визитов'}
                                )
            },
            {
            'data': data_by_source_topic, 
            'layout': go.Layout(xaxis = {'title': 'Тема источника'},
                                yaxis = {'title': 'Количество визитов'},
                                #font_size = 11,
                                #height = 500,
                                )
                },             
            {
            'data': data_by_event, 
            'layout': go.Layout(xaxis = {'title': 'Событие'},
                                yaxis = {'title': f'Среднее количество пользователей'}
                                )
            },
   )  

if __name__ == '__main__':
    app.run_server(debug = True, host='0.0.0.0')