import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import re
import vk_api
import re
import psycopg2
from nltk.corpus import stopwords

token = "1c61300a1c61300a1c61300a341c17ab9211c611c61300a7c56eca7d3df2758b4efc6b3"
database = "postgres"
user = "postgres"
password = "postgres"
host = "database-1.c1xvb10kjfue.us-east-1.rds.amazonaws.com"
port = "5432"

args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2021, 3, 15),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'depends_on_past': False,
}

def parse():
    api = getApi(token=token)
    group_id = "-35488145"
    first_100_posts = get_100_posts(group_id=group_id, api=api, offset=0)
    second_100_posts = get_100_posts(group_id=group_id, api=api, offset=100)
    posts = first_100_posts + second_100_posts
    word_statistic = {}
    for item in posts:
        words = item.get('text').split(' ')
        for word in words:
            word = word.lower()
            if re.match("[a-zA-Zа-яА-ЯёЁ#_]+", word) is not None:
                word = re.match("[a-zA-Zа-яА-ЯёЁ#_]+", word).group()
                if not word in stopwords.words('russian'):
                    if word_statistic.get(word) is None:
                        word_statistic[word] = 1
                    else:
                        word_statistic[word] = word_statistic[word] + 1
    word_statistic = {k: v for k, v in sorted(word_statistic.items(),
                                   key=lambda item: item[1],
                                   reverse=True)}

    saveToDataBase(word_statistic)

def getApi(token):
    session = vk_api.VkApi(token=token)
    api = session.get_api()
    return api

def get_100_posts(group_id, api, offset):
    posts = api.wall.get(owner_id=group_id, count=100, offset=offset).get('items')
    return posts

def deleteTable(cursor):
    cursor.execute("DROP TABLE IF EXISTS words_statistic")

def createTable(cursor):
    cursor.execute("""create table words_statistic (
                            word  varchar not null,
                            count integer not null
                        );""")

def addWordToTable(cursor, key, value):
    cursor.execute("INSERT INTO words_statistic (word, count) VALUES (\'{}\', {})".format(str(key), value))


def saveToDataBase(word_statistic):
    conn = psycopg2.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cursor = conn.cursor()
    deleteTable(cursor)
    createTable(cursor)
    for (key, value) in word_statistic.items():
        addWordToTable(cursor, key, value)
    conn.commit()
    conn.close()


with DAG(dag_id='alba_dag', default_args=args, schedule_interval=None) as dag:
    parse_vk_wall = PythonOperator(
        task_id='alba_task',
        python_callable=parse,
        dag=dag
    )
