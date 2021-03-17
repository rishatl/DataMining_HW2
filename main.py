import requests
import collections
import time
import configparser
import psycopg2
from psycopg2 import OperationalError
import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2020, 2, 13),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'depends_on_past': False,
}


def take_posts(token, version, domain):
    count = 100
    offset = 0
    all_posts = []

    while offset < 200:
        response = requests.get("https://api.vk.com/method/wall.get",
                                params={
                                    'access_token': token,
                                    'v': version,
                                    'domain': domain,
                                    'count': count,
                                    'offset': offset
                                }
                                )
        data = response.json()['response']['items']
        offset += 100
        all_posts.extend(data)
        time.sleep(0.5)
    return all_posts


def file_writer(data):
    with open('all_posts.txt', 'w') as file:
        i = 1
        for post in data:
            file.write(post['text'] + '\n')
            try:
                file.write(post['copy_history'][0]['text'] + '\n')
            except Exception:
                pass
            file.write('\n')
            i += 1


def most_common_words(connection):
    file = open('all_posts.txt')
    text = file.read()
    stop_symbols = r'.,:\!/?*-_•–—0123456789&"'
    wordcount = {}
    for word in text.lower().split():
        if word not in stop_symbols:
            if word not in wordcount:
                wordcount[word] = 1
            else:
                wordcount[word] += 1

    n_print = 100
    print("\nOK. The {} most common words are as follows\n".format(n_print))
    word_counter = collections.Counter(wordcount)
    for word, count in word_counter.most_common(n_print):
        save(connection, word, count)
        print(word, ": ", count)

    file.close()


sql = 'INSERT INTO "words" values (%s, %s)'


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(database=db_name,
                                      user=db_user,
                                      password=db_password,
                                      host=db_host,
                                      port=db_port)
    except OperationalError as e:
        print("The error '{e}' occurred")
    return connection


def save(connection, word, count):
    cursor = connection.cursor()
    cursor.execute(sql, (word, count))
    connection.commit()


def main():
    config = configparser.ConfigParser()
    config.sections()
    config.read('configuration.ini')
    token = config['VK']['token']
    version = config['VK']['version']
    domain = config['VK']['domain']
    db_name = config['DB']['db_name']
    db_user = config['DB']['db_user']
    db_password = config['DB']['db_password']
    db_host = config['DB']['db_host']
    db_port = config['DB']['db_port']
    connection = create_connection(db_name, db_user, db_password, db_host, db_port)
    data = take_posts(token, version, domain)
    file_writer(data)
    most_common_words(connection)


if __name__ == '__main__':
    main()


with DAG(dag_id='dag', default_args=args, schedule_interval=None) as dag:
    parse_vk_wall = PythonOperator(
        task_id='most_common_words',
        python_callable=main,
        dag=dag
    )
