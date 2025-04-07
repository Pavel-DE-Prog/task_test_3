import logging
import os
import json
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime, timedelta

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Определение аргументов по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'email_on_failure': True,  # Включаем уведомления об ошибках
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Функция для загрузки конфигурации из переменных окружения
def load_db_config():
    """Загружает конфигурацию подключения к базам данных из JSON файла."""
    config_path = r'C:\\Users\\Lenovo\\Desktop\\Ariflow_ETL_Papline_DAG\\dags\\db_config.json'
    with open(config_path) as f:
        config = json.load(f)

    project_connections = {key: os.getenv(value) for key, value in config['projects'].items()}
    project_connections['analytics'] = os.getenv(config['analytics'])

    logger.info("Подключения получены: %s", project_connections)
    return project_connections

# Проверка возможности подключения
def check_connection(conn_id):
    """Проверяет возможность подключения к базе данных."""
    hook = PostgresHook(postgres_conn_id=conn_id)
    try:
        hook.get_conn()
        logger.info("Подключение %s успешно", conn_id)
    except Exception as e:
        logger.error("Ошибка подключения %s: %s", conn_id, e)
        raise

# Создание таблицы analytics_sessions
def create_analytics_table():
    """Создает таблицу analytics_sessions в аналитической базе данных."""
    connections = load_db_config()
    hook = PostgresHook(postgres_conn_id=connections['analytics'])

    create_table_query = """
    CREATE TABLE IF NOT EXISTS analytics_sessions (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        active BOOLEAN NOT NULL,
        page_name VARCHAR(255),
        last_activity_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        project VARCHAR(255),
        events_count INTEGER,
        transactions_sum NUMERIC,
        first_successful_transaction_time TIMESTAMP,
        first_successful_transaction_usd NUMERIC
    );
    """

    try:
        hook.run(create_table_query)
        logger.info("Таблица analytics_sessions успешно создана.")
    except Exception as e:
        logger.error("Ошибка при создании таблицы: %s", e)
        raise

# Извлечение и обогащение данных
def extract_and_enrich_data(conn_id, **kwargs):
    """Извлекает и обогащает данные из базы данных."""
    hook = PostgresHook(postgres_conn_id=conn_id)

    query = """
    SELECT 
        us.user_id, 
        COUNT(e.id) AS events_count,
        COALESCE(SUM(t.amount * er.exchange_rate), 0) AS transactions_sum,
        MIN(t.created_at) AS first_successful_transaction_time,
        COALESCE(MIN(t.amount * er.exchange_rate), 0) AS first_successful_transaction_usd
    FROM user_sessions us
    LEFT JOIN events e ON us.id = e.session_id
    LEFT JOIN transactions t ON us.user_id = t.user_id AND DATE(us.created_at) = DATE(t.created_at) 
    LEFT JOIN exchange_rates er ON t.currency = er.currency_from AND DATE(t.created_at) = DATE(er.currency_date)
    WHERE us.created_at > NOW() - INTERVAL '10 MINUTES'
    GROUP BY us.user_id;
    """

    try:
        df_enriched = hook.get_pandas_df(query)
        if df_enriched.empty:
            logger.info("Нет данных для загрузки")
            return
        kwargs['ti'].xcom_push(key='enriched_data', value=df_enriched.to_json())
        logger.info("Обогащенные данные извлечены: %d строк.", df_enriched.shape[0])
    except Exception as e:
        logger.error("Ошибка при извлечении и обогащении данных: %s", e)
        raise

# Функция загрузки данных
def load_data(**kwargs):
    """Загружает обогащенные данные в таблицу analytics_sessions."""
    ti = kwargs['ti']
    enriched_data_json = ti.xcom_pull(key='enriched_data', task_ids='extract_and_enrich_data')

    if enriched_data_json is None:
        logger.info("Нет обогащенных данных для загрузки")
        return

    df = pd.read_json(enriched_data_json)

    if df.empty:
        logger.info("Нет обогащенных данных для загрузки")
        return

    connections = load_db_config()
    hook_analytics = PostgresHook(postgres_conn_id=connections['analytics'])
    engine = hook_analytics.get_sqlalchemy_engine()

    enriched_data_records = df.to_dict(orient='records')

    if not enriched_data_records:
        logger.info("Нет данных для загрузки")
        return

    try:
        with engine.connect() as conn:
            conn.execute(
                hook_analytics.get_table('analytics_sessions').insert(),
                enriched_data_records
            )
        logger.info("Успешно загружено %d записей в таблицу analytics_sessions", len(df))
    except Exception as e:
        logger.error("Ошибка загрузки данных: %s", e)
        raise

# Функция для создания задач проверки соединений
def create_connection_check_tasks():
    """Создает задачи для проверки соединений с базами данных."""
    connections = load_db_config()
    tasks = []

    for project_name, conn_id in connections.items():
        task = PythonOperator(
            task_id=f'check_connection_{project_name}',
            python_callable=check_connection,
            op_kwargs={'conn_id': conn_id}
        )
        tasks.append(task)

    return tasks

# Функция для создания задач извлечения данных
def create_extract_tasks():
    """Создает задачи для извлечения данных из баз данных всех проектов."""
    connections = load_db_config()
    tasks = []

    for project_name, conn_id in connections.items():
        if project_name.startswith("project_"):  # Проверяем только проектные подключения
            task = PythonOperator(
                task_id=f'extract_data_{project_name}',
                python_callable=extract_and_enrich_data,
                op_kwargs={'conn_id': conn_id}  # Передаем id подключения
            )
            tasks.append(task)

    return tasks

# Определение DAG для ETL процесса
with DAG('etl_sessions', default_args=default_args, schedule='*/10 * * * *', catchup=False) as dag:
    create_table_task = PythonOperator(
        task_id='create_analytics_table',
        python_callable=create_analytics_table
    )

    connection_checks = create_connection_check_tasks()

    extract_tasks = create_extract_tasks()  # Создаем задачи извлечения

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    # Определение порядка выполнения задач
    for connection_check in connection_checks:
        connection_check >> create_table_task

    create_table_task >> extract_tasks >> load_task  # Вызовите все задачи извлечения параллельно