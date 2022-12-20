# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests
import numpy as np
from db_utils import get_connection, load_connection

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Считаем метрики за вчерашний день и записываем их в таблицу
# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'o-ts',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 11, 13),
}

connection_load = load_connection()

connection = get_connection()


# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_daily_tsepulina():

    @task
    #для каждого юзера посчитаем число просмотров и лайков контента
    def extract_feed_actions():
        q_1 = """
                SELECT user_id as user, 
                        countIf(action ='like') as likes,
                        countIf(action = 'view') as views,
                        toDate(time) as event_date,
                        if(gender = 1, 'male', 'female') as gender,
                        os,
                        multiIf(age<=18, '0-18', age>18 and age<=25, '19-25', age>25 and age<=35, '26-35', age>35 and age<=45, '36-45', '46+') age
                FROM {db}.feed_actions
                WHERE event_date = yesterday()
                GROUP BY user_id, event_date, gender, os, age"""

        feed_actions = ph.read_clickhouse(q_1, connection=connection)
        return feed_actions

    @task
    #для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему
    def extract_messages_actions():
        q_2 = """
                SELECT user, users_sent, messages_sent, users_received, messages_received,
                        event_date, os, gender, age
                FROM 
                (SELECT toDate(time) as event_date,
                        user_id as user, 
                        count(distinct reciever_id) users_sent,
                        count(reciever_id) messages_sent,
                        os,
                        if(gender = 1, 'male', 'female') as gender,
                        multiIf(age<=18, '0-18', age>18 and age<=25, '19-25', age>25 and age<=35, '26-35', age>35 and age<=45, '36-45', '46+') age   
                FROM {db}.message_actions
                WHERE event_date = yesterday()
                GROUP BY event_date, user, os, gender, age) t1
                full OUTER JOIN
                (SELECT toDate(time) as event_date,
                        reciever_id as user,
                        count(distinct user_id) users_received,
                        count(user_id) messages_received,
                        os,
                        if(gender = 1, 'male', 'female') as gender,
                        multiIf(age<=18, '0-18', age>18 and age<=25, '19-25', age>25 and age<=35, '26-35', age>35 and age<=45, '36-45', '46+') age
                FROM {db}.message_actions
                WHERE toDate(time) = yesterday()
                GROUP BY  event_date, user, os, gender, age) t2
                using user
            """

        messages_actions = ph.read_clickhouse(q_2, connection=connection)
        return messages_actions
    
    
    @task
    #объединяем две таблицы в одну
    def merged_tables(feed_actions, messages_actions):
        df_merged = pd.merge(feed_actions, messages_actions, how='outer').fillna(0)
        return df_merged
    
    
    @task
    #делаем срез по OS
    def transform_os(df_merged):
        df_merged_os = df_merged.groupby(['os']) \
               .aggregate({'event_date': 'max',
                          'likes':'sum', 
                          'views': 'sum', 
                          'users_sent': 'sum', 
                          'messages_sent':'sum',
                          'users_received': 'sum',
                          'messages_received': 'sum'
                         }) \
                .reset_index() \
                .rename(columns = {'os':'dimension_value'})               
        df_merged_os.insert(0,'dimension', 'os')
        df_merged_os = df_merged_os.reindex(columns=['event_date', 'dimension','dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent'])
        return df_merged_os
    
    @task
    #делаем срез по возрасту
    def transform_age(df_merged):
        df_merged_age = df_merged.groupby(['age']) \
               .aggregate({'event_date': 'max',
                          'likes':'sum', 
                          'views': 'sum', 
                          'users_sent': 'sum', 
                          'messages_sent':'sum',
                          'users_received': 'sum',
                          'messages_received': 'sum'
                         }) \
                .reset_index() \
                .rename(columns = {'age':'dimension_value'})
        df_merged_age['dimension'] = 'age' 
        df_merged_age = df_merged_age.reindex(columns=['event_date', 'dimension','dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent'])
        return df_merged_age
        
    @task
    #делаем срез по полу
    def transform_gender(df_merged):
        df_merged_gender = df_merged.groupby(['gender']) \
               .aggregate({'event_date': 'max',
                          'likes':'sum', 
                          'views': 'sum', 
                          'users_sent': 'sum', 
                          'messages_sent':'sum',
                          'users_received': 'sum',
                          'messages_received': 'sum'
                         }) \
                .reset_index() \
                .rename(columns = {'gender':'dimension_value'})
        df_merged_gender['dimension'] = 'gender' 
        df_merged_gender = df_merged_gender.reindex(columns=['event_date', 'dimension','dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent'])
        return df_merged_gender
        
    @task
    #записываем полученные данные в отдельную таблицу
    def load(df_merged_os, df_merged_age, df_merged_gender):
        db_data = pd.concat([df_merged_os, df_merged_age, df_merged_gender])
        for col, t in db_data.dtypes.items():
            if t == np.float64:
                db_data[col] = db_data[col].astype(int)
        q_3 = """
                CREATE TABLE IF NOT EXISTS test.Tsepulina
                    (event_date Date,
                    dimension VARCHAR,
                    dimension_value VARCHAR,
                    views Int64,
                    likes Int64,                       
                    messages_received Int64,           
                    messages_sent Int64,                
                    users_received Int64,              
                    users_sent Int64)                  
                ENGINE = MergeTree
                ORDER By event_date
            """
        ph.execute(q_3, connection_load)
        ph.to_clickhouse(db_data, table='Tsepulina', connection=connection_load, index=False)
        
        
    

    feed_actions = extract_feed_actions()
    messages_actions = extract_messages_actions()
    df_merged = merged_tables(feed_actions, messages_actions)
    df_merged_os = transform_os(df_merged)
    df_merged_age = transform_age(df_merged)
    df_merged_gender = transform_gender(df_merged)
    load(df_merged_os, df_merged_age, df_merged_gender)

dag_daily_tsepulina = dag_daily_tsepulina()
