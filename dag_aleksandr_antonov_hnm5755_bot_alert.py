# coding=utf-8
import telegram
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter
import seaborn as sns
import io
import pandas as pd
import requests
from datetime import datetime, timedelta
import pandahouse
import sys
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context



# The Getch class, since a folder is required in the Airflow repository, which exists only in analyst_simulator and locally on my PC

class Getch:
    def __init__(self, query, db='simulator_20250820'):
        self.connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': db,
        }
        self.query = query
        self.df = self.getchdf

    @property
    def getchdf(self):
        try:
            return pandahouse.read_clickhouse(self.query, connection=self.connection)
        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)        

# Default parameters that are passed to all tasks
default_args = {
    'owner': 'aleksandr_antonov_hnm5755',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 10, 7),
}

# DAG run interval = 15 min.
schedule_interval = '*/15 * * * *'

def check_anomaly(df, metric, a=4, n=4):
    # the check_anomaly function suggests an algorithm for detecting anomalies
    # by comparing the current value with the value from 15 minutes ago.
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
        
    return is_alert, df

 
my_token = '1111111111111:AAFHHxLQAz3O6dfgfZndfg65v11P5DYod5fgdfQ'
bot = telegram.Bot(token=my_token)
chat_id = 427611111
    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_aleksandr_antonov_hnm5755_bot_alert():

    @task
    def extract_metrics():    
    # for easier visualization, we add the columns 'date' and 'hm' to the query.
        data = Getch("""
                     SELECT
                        ts,
                        date,
                        hm,
                        views,
                        likes,
                        likes / views as ctr,
                        uniq_users_feed,
                        messages_sent,
                        uniq_users_messenger
                    FROM
                        (
                        SELECT
                            toStartOfFifteenMinutes(time) as ts,
                            toDate(ts) as date,
                            formatDateTime(ts, '%R') as hm,
                            countIf(action='view') as views,
                            countIf(action='like') as likes,
                            uniqExact(user_id) as uniq_users_feed
                        FROM simulator_20250820.feed_actions
                        WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm
                        ORDER BY ts
                        ) AS fa
                        LEFT JOIN
                        (
                        SELECT  
                            toStartOfFifteenMinutes(time) as ts,
                            toDate(ts) as date,
                            formatDateTime(ts, '%R') as hm,
                            count() AS messages_sent,
                            uniqExact(user_id) as uniq_users_messenger
                        FROM simulator_20250820.message_actions
                        WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date, hm
                        ORDER BY ts 
                        ) AS ma
                        USING ts
                        """).df
        return data
       
    @task
    def check_and_alert(data):
        metrics_list = ['views', 'likes', 'ctr', 'uniq_users_feed', 'messages_sent', 'uniq_users_messenger']
        for metric in metrics_list:
            print(metric)
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly(df, metric)

            if is_alert == 1:   # If 'or True' is set, the metrics will be sent even without anomalies. 
                                # this should be removed before deploying to production.
                msg = '''Metric {metric}:\nCurrent value = {current_val:.2f}\nDifference from previous value {last_val_diff:.2%}\n[Open the dashboard] https://superset.lab.karpov.courses/superset/dashboard/7566/'''.format(metric=metric,
                                                                                                                                                current_val=df[metric].iloc[-1],
                                                                                                                                                last_val_diff= abs(1 - (df[metric].iloc[-1]/df[metric].iloc[-2]))) 

                sns.set(rc={'figure.figsize': (16, 10)}) # plot size
                plt.tight_layout()

                ax = sns.lineplot(x=df['ts'], y=df[metric], label='metric') #  specify the column names in the DataFrame for the x and y axes.
                ax = sns.lineplot(x=df['ts'], y=df['up'], label='up')
                ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')                 

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                ax.set(xlabel='time')
                ax.set(ylabel=metric)

                ax.set_title(metric)
                ax.set(ylim=(0, None))

                # Create a file object
                plot_object = io.BytesIO()
                ax.figure.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()

                # Send an alert
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    df = extract_metrics()
    check_and_alert(df)


dag_aleksandr_antonov_hnm5755_bot_alert = dag_aleksandr_antonov_hnm5755_bot_alert()









