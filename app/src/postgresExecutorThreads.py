from typing import List, Optional, Dict, Any
from psycopg2 import pool
import psycopg2.extras
import threading
import time
from src.logger import logger
from src.databaseSettings import settings
from src.parseScripts import scripts

class PostgresExecutorThreads:
    """Класс для параллельного выполнения SQL в PostgreSQL"""
    def __init__(self, 
                 host: str = settings.DATABASE_HOST, 
                 port: int = settings.DATABASE_PORT, 
                 database: str = settings.DATABASE_DB, 
                 user: str = settings.DATABASE_USER, 
                 password: str = settings.DATABASE_PASSWORD,
                 min_connections: int = settings.DATABASE_CONNECTIONS_MIN,
                 max_connections: int = settings.DATABASE_CONNECTIONS_MAX):
        """
        Инициализация подключения к PostgreSQL
        Args:
            host: Хост БД
            port: Порт БД
            database: Имя базы данных
            user: Имя пользователя
            password: Пароль
            min_connections: Минимальное количество соединений в пуле
            max_connections: Максимальное количество соединений в пуле
        """
        self.db_params = settings.DATABASE_PARAMS
        
        # Создаем пул соединений
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=settings.DATABASE_CONNECTIONS_MIN,
                maxconn=settings.DATABASE_CONNECTIONS_MAX,
                **self.db_params
            )
            logger.info("Пул соединений с PostgreSQL создан успешно")
        except Exception as e:
            logger.error(f"Ошибка создания пула соединений: {e}")
            raise
    
    def execute_sql(self, 
                    sql_script: str, 
                    thread_name: str,
                    params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Выполнение SQL-скрипта в отдельном потоке
        
        Args:
            sql_script: SQL-скрипт для выполнения
            thread_name: Имя потока (для логирования)
            params: Параметры для запроса
            
        Returns:
            Словарь с результатами выполнения
        """
        connection = None
        cursor = None
        result = {
            'thread_name': thread_name,
            'success': False,
            'execution_time': 0,
            'rows_affected': 0,
            'error': None,
            'data': None
        }
        
        start_time = time.time()
        
        try:
            # Получаем соединение из пула
            connection = self.connection_pool.getconn()
            cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            logger.info(f"[{thread_name}] Начало выполнения SQL")
            
            # Выполняем SQL
            if params:
                cursor.execute(sql_script, params)
            else:
                cursor.execute(sql_script)
            
            # Если это SELECT запрос - получаем данные
            if sql_script.strip().upper().startswith('SELECT'):
                result['data'] = cursor.fetchall()
                result['rows_affected'] = len(result['data'])
            else:
                # Для INSERT/UPDATE/DELETE получаем количество измененных строк
                result['rows_affected'] = cursor.rowcount
            
            logger.info(f"[{thread_name}] Получено\Обработано {result['rows_affected']} строк")

            connection.commit()
            
            result['success'] = True
            
        except Exception as e:
            logger.error(f"[{thread_name}] Ошибка при выполнении SQL: {e}")
            result['error'] = str(e)
            if connection:
                connection.rollback()
                
        finally:
            # Закрываем курсор и возвращаем соединение в пул
            if cursor:
                cursor.close()
            if connection:
                self.connection_pool.putconn(connection)
            
            result['execution_time'] = time.time() - start_time
            logger.info(f"[{thread_name}] Выполнение завершено за {result['execution_time']:.2f} сек")
        
        return result
    
    def execute_in_threads( self, 
                            sql_scripts: dict[str, dict[str, str]]) ->  List[Dict[str, Any]]:
        """
        Параллельное выполнение нескольких SQL-скриптов
        
        Args:
            -| sql_scripts: Список объектов SQL-скриптов
            -| "Проверка подключения":
            -----| sql: |
            --------|- SELECT 1 as VALUE
            -----| name: "Проверка подключения"
            -----| type: SELECT
            
        Returns:
            Список результатов выполнения каждого потока
        """

        threads = []
        results = []
        
        def thread_wrapper(sql: str, name: str, result_list: list):
            """Обертка для потока"""
            result = self.execute_sql(sql, name)
            result_list.append(result)
        
        # # Создаем и запускаем потоки
        logger.info(f"Запуск {len(sql_scripts)} потоков...")
        for i, (key, item) in enumerate(sql_scripts.items()):
            thread = threading.Thread(
                target=thread_wrapper,
                args=(item.get('sql',''), item.get('name',key), results),
                name=item.get('name',key)
            )
            threads.append(thread)
            thread.start()

        # # Ждем завершения всех потоков
        for thread in threads:
            thread.join()
        
        logger.info("Все потоки завершены")
        return results
    
    def close(self):
        """Закрытие пула соединений"""
        if hasattr(self, 'connection_pool'):
            self.connection_pool.closeall()
            logger.info("Пул соединений закрыт")

class PostgresRunThreads:

    @staticmethod
    def info():
        return """
Выполнение SQL запросов
1. Установить параметры подключения к БД. Файл .env
2. Заполнить перечень запросов, которые необходимо выполнить. файл scripts.yml

Пример заполнения файла scripts.yml
  scripts:
    "Проверка подключения":
      sql: |
        SELECT 1 
        as VALUE
      name: "Проверка подключения"
      type: command

    "Выполнение запроса":
      sql: |
        SELECT 1 
        as VALUE
      name: "Выполнение запроса"
      type: command

"""

    @staticmethod
    def run():
                
        executor = None
        
        try:
            # Создаем экземпляр исполнителя
            logger.info("=" * 50)
            logger.info("Начало параллельного выполнения SQL-скриптов")
            logger.info("=" * 50)
            
            executor = PostgresExecutorThreads(**settings.DATABASE_PARAMS)
            
            # Выполняем SQL в трех потоках
            results = executor.execute_in_threads(scripts.scripts)
            
            # # Выводим результаты
            logger.info("")
            logger.info("=" * 50)
            logger.info("РЕЗУЛЬТАТЫ ВЫПОЛНЕНИЯ:")
            logger.info("=" * 50)
            
            success_count = 0
            for result in results:
                status = "УСПЕХ" if result['success'] else "ОШИБКА"
                logger.info("")
                logger.info(f"Поток: {result['thread_name']}")
                logger.info(f"Статус: {status}")
                logger.info(f"Время выполнения: {result['execution_time']:.2f} сек")
                logger.info(f"Строк обработано: {result['rows_affected']}")
                
                if result['success']:
                    success_count += 1
                else:
                    logger.error(f"Ошибка: {result['error']}")
            
            logger.info("")
            logger.info("=" * 50)
            logger.info(f"ИТОГО: {success_count}/{len(results)} потоков выполнены успешно")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            
        finally:
            # Закрываем соединения
            if executor:
                executor.close()
