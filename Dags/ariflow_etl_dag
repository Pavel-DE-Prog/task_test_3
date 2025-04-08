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
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_db_config():
    """Загружает конфигурацию подключения к базам данных из JSON файла."""
    config_path = os.path.join(os.path.dirname(__file__), 'db_config.json')
    with open(config_path) as f:
        config = json.load(f)

    project_connections = {key: os.getenv(value) for key, value in config['projects'].items()}
    project_connections['analytics'] = os.getenv(config['analytics'])

    logger.info("Подключения получены: %s", project_connections)
    return project_connections

def check_connection(conn_id):
    """Проверяет возможность подключения к базе данных."""
    hook = PostgresHook(postgres_conn_id=conn_id)
    try:
        hook.get_conn()
        logger.info("Подключение %s успешно", conn_id)
    except Exception as e:
        logger.error("Ошибка подключения %s: %s", conn_id, e)
        raise

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

def extract_data_from_project(project_conn_id, session_start_time, **kwargs):
    """Извлекает данные из таблиц user_sessions, pages и events в временную таблицу."""
    hook = PostgresHook(postgres_conn_id=project_conn_id)

    query = f"""
    CREATE TEMP TABLE temp_user_data AS
    SELECT 
        us.user_id, 
        COUNT(e.id) AS events_count,
        us.created_at AS session_created_at,
        p.page_name,
        us.active
    FROM user_sessions us
    LEFT JOIN events e ON us.id = e.session_id
    LEFT JOIN pages p ON p.id = us.page_id
    WHERE us.created_at > %s
    GROUP BY us.user_id, p.page_name, us.active, us.created_at;
    """

    try:
        hook.run(query, parameters=(session_start_time,))
        logger.info("Данные извлечены из проекта %s", project_conn_id)
    except Exception as e:
        logger.error("Ошибка извлечения данных из %s: %s", project_conn_id, e)
        raise

def enrich_and_load_data(**kwargs):
    """Обогащает и загружает данные в таблицу analytics_sessions."""
    connections = load_db_config()
    hook_analytics = PostgresHook(postgres_conn_id=connections['analytics'])

    enriched_data = []

    # Перебираем проекты (предполагаем, что временные таблицы уже созданы для каждого проекта)
    for proj in ['project_a', 'project_b', 'project_c']:
        project_conn_id = os.getenv(proj)
        hook_project = PostgresHook(postgres_conn_id=project_conn_id)

        # Извлечение данных из временной таблицы
        transactions_query = """
        SELECT 
            user_id, 
            events_count, 
            session_created_at, 
            page_name, 
            active 
        FROM temp_user_data;
        """
        
        df_project = hook_project.get_pandas_df(transactions_query)

        for index, row in df_project.iterrows():
            user_id = row['user_id']
            session_created_at = row['session_created_at']
            active = row['active']
            page_name = row['page_name']
            events_count = row['events_count']

            # Запрос на извлечение транзакций
            transactions_query = """
            SELECT 
                COALESCE(SUM(t.amount * er.exchange_rate), 0) AS transactions_sum,
                MIN(t.created_at) AS first_successful_transaction_time,
                COALESCE(MIN(t.amount * er.exchange_rate), 0) AS first_successful_transaction_usd
            FROM transactions t
            LEFT JOIN exchange_rates er ON t.currency = er.currency_from
            WHERE t.user_id = %s AND DATE(t.created_at) = DATE(%s) AND t.success = TRUE;
            """

            # Получаем данные по транзакциям
            transactions_data = hook_analytics.get_pandas_df(transactions_query, parameters=(user_id, session_created_at))

            if not transactions_data.empty:
                transactions_sum = transactions_data['transactions_sum'].values[0]
                first_successful_transaction_time = transactions_data['first_successful_transaction_time'].values[0]
                first_successful_transaction_usd = transactions_data['first_successful_transaction_usd'].values[0]
            else:
                transactions_sum = 0
                first_successful_transaction_time = None
                first_successful_transaction_usd = 0
            
            enriched_data.append({
                'user_id': user_id,
                'active': active,
                'page_name': page_name,
                'last_activity_at': session_created_at,
                'events_count': events_count,
                'transactions_sum': transactions_sum,
                'first_successful_transaction_time': first_successful_transaction_time,
                'first_successful_transaction_usd': first_successful_transaction_usd,
                'project': proj
            })

    # Преобразование в DataFrame
    df_enriched = pd.DataFrame(enriched_data)

    # Загрузка данных в таблицу analytics_sessions
    if not df_enriched.empty:
        df_enriched.to_sql(
            'analytics_sessions',
            con=hook_analytics.get_sqlalchemy_engine(),
            if_exists='append',
            index=False,
            chunksize=500
        )
        logger.info("Успешно загружено %d записей в таблицу analytics_sessions", len(df_enriched))
    else:
        logger.info("Нет данных для загрузки в analytics_sessions")

# Определение DAG для ETL процесса
with DAG('etl_sessions', default_args=default_args, schedule='*/10 * * * *', catchup=False) as dag:
    create_table_task = PythonOperator(
        task_id='create_analytics_table',
        python_callable=create_analytics_table
    )

    connection_checks = [PythonOperator(
        task_id=f'check_connection_{proj}',
        python_callable=check_connection,
        op_kwargs={'conn_id': os.getenv(conn)},
    ) for proj, conn in load_db_config()['projects'].items()]

    extract_tasks = [PythonOperator(
        task_id=f'extract_data_{proj}',
        python_callable=extract_data_from_project,
        op_kwargs={'project_conn_id': os.getenv(conn), 
                    'session_start_time': datetime.now() - timedelta(minutes=10)},
    ) for proj, conn in load_db_config()['projects'].items()]

    enrich_load_task = PythonOperator(
        task_id='enrich_and_load_data',
        python_callable=enrich_and_load_data
    )

    # Установка зависимостей между задачами
    for check_task in connection_checks:
        check_task >> create_table_task

    create_table_task >> extract_tasks >> enrich_load_task
