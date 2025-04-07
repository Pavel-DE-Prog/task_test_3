from airflow.hooks.postgres_hook import PostgresHook
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)


def check_connection(conn_id):
    """Проверяет подключение к базе данных по заданному идентификатору соединения.

    Args:
        conn_id (str): Идентификатор соединения Airflow для подключения к базе данных.

    Returns:
        bool: True, если подключение успешно, иначе False.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)

    try:
        with hook.get_conn() as connection:  # Пытаемся получить соединение
            logging.info(f"Successfully connected using {conn_id}.")
            return True
    except Exception as e:
        logging.error(f"Failed to connect using {conn_id}: {e}")
        return False


if __name__ == "__main__":
    connection_id = "PROJECT_A_CONN"  # Пример идентификатора соединения
    success = check_connection(connection_id)

    if success:
        print("Connection successful.")
    else:
        print("Connection failed.")