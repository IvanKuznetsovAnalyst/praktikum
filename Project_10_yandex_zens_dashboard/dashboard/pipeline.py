#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import getopt
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

if __name__ == "__main__":

    # input params setup
    unixOptions = "s:e:"  
    gnuOptions = ["start_dt=", "end_dt="] 

    fullCmdArguments = sys.argv
    argumentList = fullCmdArguments[1:]    #excluding script name

    try:  
        arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
    except getopt.error as err:  
        # output error, and return with an error code
        print (str(err))
        sys.exit(2)

    start_dt = ''
    end_dt = ''   
    for currentArgument, currentValue in arguments:  
        if currentArgument in ("-s", "--start_dt"):
            start_dt = currentValue                                   
        elif currentArgument in ("-e", "--end_dt"):
            end_dt = currentValue
    if start_dt == '' or end_dt == '':
        print('''
Не заданы входные параметры - даты начала и окончания периода
Вызовите эту программу так:
python3 pipeline.py --start_dt='2019-09-24 18:00:00' --end_dt='2019-09-24 19:00:00'
            ''')
        sys.exit(2)

    db = 'zen'
    db_in_dump = 'zen.dump'
    db_out_dump = 'zen_modified.dump'


    commands = [
    'sudo service postgresql start',
    f'cp {db_in_dump} /tmp/{db_in_dump}',
    f'''sudo -u postgres psql -d {db} -c 'drop table if exists log_raw;' ''',
    f'sudo -u postgres pg_restore -d {db} /tmp/{db_in_dump}',
    ]

    for command in commands:
        print(f'\n-----Begin: {command}\n')
        os.system(command)

    db_config = {'user': 'my_user',
                 'pwd': 'my_user_password',
                 'host': 'localhost',
                 'port': 5432,
                 'db': db}   
    connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_config['user'], 
                                                             db_config['pwd'], 
                                                             db_config['host'], 
                                                             db_config['port'], 
                                                             db_config['db'])
    engine = create_engine(connection_string)
    query = f'''SELECT event_id,        
                     age_segment,
                     event,
                     item_id,    
                     item_topic,
                     item_type,    
                     source_id,     
                     source_topic,    
                     source_type,     
                     date_trunc('minute',TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'Etc/UTC') as dt,    
                     user_id 
                FROM log_raw 
                WHERE (TO_TIMESTAMP(ts / 1000) AT TIME ZONE 'Etc/UTC') BETWEEN '{start_dt}'::TIMESTAMP AND '{end_dt}'::TIMESTAMP;
            '''
    log_raw = pd.io.sql.read_sql(query, con = engine, index_col = 'event_id')
    log_raw['dt'] = pd.to_datetime(log_raw['dt']).dt.round('min')
    dash_visits = (log_raw
        .groupby(['item_topic', 'source_topic', 'age_segment', 'dt'], as_index=False)
        .agg({'user_id': 'count'})
        .fillna(0)
        )
    dash_engagement = (log_raw
        .groupby(['dt', 'item_topic', 'event', 'age_segment'], as_index=False)
        .agg({'user_id': 'nunique'})
        .fillna(0)
        )
    dash_visits = dash_visits.rename(columns = {'user_id': 'visits'})
    dash_engagement = dash_engagement.rename(columns = {'user_id': 'unique_users'})
    tables = {'dash_visits': dash_visits, 
              'dash_engagement': dash_engagement}
    for table_name, table_data in tables.items():   
        query = f'''
                  DELETE FROM {table_name} WHERE dt BETWEEN '{start_dt}'::TIMESTAMP AND '{end_dt}'::TIMESTAMP;
                '''
        engine.execute(query)
        table_data.to_sql(name = table_name, con = engine, if_exists = 'append', index = False)


    os.system(f'sudo -u postgres pg_dump -Fc {db}> /tmp/{db_out_dump}')
    print('В таблице log_raw {} записей'.format(log_raw.shape[0]))
    print('В таблице dash_visits {} записей'.format(dash_visits.shape[0]))
    print('В таблице dash_engagement {} записей'.format(dash_engagement.shape[0]))
    print(f'Выгружен дамп базы данных zen в файл: /tmp/{db_out_dump}')
    print('Работа пайплайна закончена')