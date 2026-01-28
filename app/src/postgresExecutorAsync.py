from datetime import datetime
import json
from typing import List, Optional, Dict, Any, AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict
from src.databaseSettings import settings
import asyncio
import asyncpg
from src.logger import logger
from src.parseScripts import scripts

@dataclass
class QueryResult:
    """Результат выполнения запроса"""
    query_name: str
    success: bool
    execution_time: float
    rows_affected: int = 0
    error: Optional[str] = None
    data: Optional[List[Dict[str, Any]]] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертация в словарь"""
        result = asdict(self)
        if self.started_at:
            result['started_at'] = self.started_at.isoformat()
        if self.completed_at:
            result['completed_at'] = self.completed_at.isoformat()
        return result
    
class PostgresExecutorAsync:
    """
    Асинхронный исполнитель SQL запросов с использованием пула соединений.
    Реализует паттерны: Connection Pool, Repository, async/await.
    """

    
    def __init__(
        self,
        host: str = settings.DATABASE_HOST, 
        port: int = settings.DATABASE_PORT, 
        database: str = settings.DATABASE_DB, 
        user: str = settings.DATABASE_USER, 
        password: str = settings.DATABASE_PASSWORD,
        min_connections: int = settings.DATABASE_CONNECTIONS_MIN,
        max_connections: int = settings.DATABASE_CONNECTIONS_MAX,
        connection_timeout: int = settings.DATABASE_CONNECTION_TIMEOUT,
        command_timeout: int = settings.DATABASE_COMMAND_TIMEOUT,
        concurrent_max: int = settings.ASYNC_CONCURRENT_MAX
    ):
        """
        Инициализация асинхронного исполнителя
        
        Args:
            host: Хост БД
            port: Порт БД
            database: Имя базы данных
            user: Имя пользователя
            password: Пароль
            min_connections: Минимальное количество соединений в пуле
            max_connections: Максимальное количество соединений в пуле
            connection_timeout: Таймаут подключения (секунды)
            command_timeout: Таймаут выполнения команд (секунды)
            concurrent_max: Мкасимальное количество одновременных запусков
        """
        self.dsn = settings.DATABASE_URL
        self.db_params = settings.DATABASE_PARAMS

        self.min_connections = min_connections or settings.DATABASE_CONNECTIONS_MIN
        self.max_connections = max_connections or settings.DATABASE_CONNECTIONS_MAX
        self.connection_timeout = connection_timeout or settings.DATABASE_CONNECTION_TIMEOUT
        self.command_timeout = command_timeout or settings.DATABASE_COMMAND_TIMEOUT
        self.concurrent_max = concurrent_max or settings.ASYNC_CONCURRENT_MAX

        self.connection_pool: Optional[asyncpg.pool.Pool] = None
        
    async def initialize(self) -> None:
        """Инициализация пула соединений (должна быть вызвана до использования)"""
        try:
            self.connection_pool = await asyncpg.create_pool(
                dsn=self.dsn,
                min_size=self.min_connections,
                max_size=self.max_connections,
                timeout=self.connection_timeout,
                command_timeout=self.command_timeout,
                max_inactive_connection_lifetime=300,  # Закрывать неактивные соединения через 5 минут
                init=self._init_connection,
            )
            logger.info("Асинхронный пул соединений с PostgreSQL создан успешно")
        except Exception as e:
            logger.error(f"Ошибка создания пула соединений: {e}")
            raise
        
    async def _init_connection(self, connection: asyncpg.Connection) -> None:
        """Инициализация соединения (можно добавить настройки)"""
        await connection.set_type_codec(
            'json',
            encoder=lambda v: json.dumps(v) if v else 'null',
            decoder=lambda v: json.loads(v) if v else None,
            schema='pg_catalog'
        )
        await connection.execute("SET TIME ZONE 'UTC'")

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """
        Контекстный менеджер для получения соединения из пула.
        
        Использование:
            async with executor.get_connection() as conn:
                result = await conn.fetch("SELECT * FROM table")
        """
        if not self.connection_pool:
            raise RuntimeError("Пул соединений не инициализирован. Вызовите initialize()")
        
        async with self.connection_pool.acquire() as connection:
            try:
                yield connection
            except Exception as e:
                await connection.rollback()
                raise
            else:
                await connection.commit()
    
    async def execute_query(
        self,
        sql: str,
        query_name: str = "Unnamed Query",
        params: Optional[List[Any]] = None,
        timeout: Optional[int] = None
    ) -> QueryResult:
        """
        Выполнение одного SQL запроса
        
        Args:
            sql: SQL запрос
            query_name: Имя запроса для логирования
            params: Параметры запроса
            timeout: Таймаут выполнения (секунды)
            
        Returns:
            QueryResult с результатами выполнения
        """
        result = QueryResult(
            query_name=query_name,
            success=False,
            execution_time=0,
            started_at=datetime.utcnow()
        )
        
        start_time = asyncio.get_event_loop().time()
        
        try:
            if not self.connection_pool:
                raise RuntimeError("Пул соединений не инициализирован.")

            async with self.connection_pool.acquire() as connection:
                logger.info(f"[{query_name}] Начало выполнения SQL")
                
                # Определяем тип запроса
                is_select = sql.strip().upper().startswith('SELECT')
                is_with = sql.strip().upper().startswith('WITH')
                
                if is_select or (is_with and 'SELECT' in sql.strip().upper()):
                    # Для запросов, возвращающих данные
                    if params:
                        rows = await connection.fetch(sql, *params)
                    else:
                        rows = await connection.fetch(sql)
                    
                    result.data = [dict(row) for row in rows]
                    result.rows_affected = len(result.data)
                    logger.info(f"[{query_name}] Получено {result.rows_affected} строк")
                else:
                    async with connection.transaction():
                        if params:
                            status = await connection.execute(sql, *params)
                        else:
                            status = await connection.execute(sql)
                        if status:
                            # Формат: "INSERT 0 1", "UPDATE 5", "DELETE 3"
                            parts = status.split()
                            if len(parts) >= 2:
                                try:
                                    # Для INSERT формат: "INSERT 0 1" - последнее число это количество
                                    if parts[0].upper() == 'INSERT':
                                        result.rows_affected = int(parts[-1]) if parts[-1].isdigit() else 0
                                    # Для UPDATE/DELETE формат: "UPDATE 5" или "DELETE 3"
                                    else:
                                        result.rows_affected = int(parts[-1]) if parts[-1].isdigit() else 0
                                except (ValueError, IndexError):
                                    result.rows_affected = 0
                        logger.info(f"[{query_name}] Обработано {result.rows_affected} строк")

                result.success = True

        except asyncio.TimeoutError:
            error_msg = f"Таймаут выполнения запроса ({timeout or self.command_timeout} сек)"
            logger.error(f"[{query_name}] {error_msg}")
            result.error = error_msg
        except Exception as e:
            logger.error(f"[{query_name}] Ошибка при выполнении SQL: {e}")
            result.error = str(e)
        finally:
            result.execution_time = asyncio.get_event_loop().time() - start_time
            result.completed_at = datetime.utcnow()
            logger.info(f"[{query_name}] Выполнение завершено за {result.execution_time:.2f} сек")
        
        return result
    
    async def execute_queries_concurrently(
        self,
        queries: Dict[str, Dict[str, Any]],
        max_concurrent: Optional[int] = None
    ) -> List[QueryResult]:
        """
        Параллельное выполнение нескольких SQL запросов
        
        Args:
            queries: Словарь запросов
            max_concurrent: Максимальное количество одновременно выполняемых запросов
            
        Returns:
            Список результатов выполнения
        """
        if not queries:
            logger.warning("Нет запросов для выполнения")
            return []
        
        logger.info(f"Запуск {len(queries)} асинхронных запросов...")
        
        # Создаем задачи для всех запросов
        tasks = []
        for key, query_data in queries.items():
            task = self.execute_query(
                sql=query_data.get('sql', ''),
                query_name=query_data.get('name', key),
                params=query_data.get('params'),
                timeout=query_data.get('timeout')
            )
            tasks.append(task)
        
        # Используем семафор для ограничения одновременных запросов
        max_concurrent_on = max_concurrent or self.max_connections
        if max_concurrent:
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def limited_task(task: asyncio.Task) -> QueryResult:
                async with semaphore:
                    return await task
                
            limited_tasks = [limited_task(asyncio.create_task(t)) for t in tasks]
            results = await asyncio.gather(*limited_tasks, return_exceptions=True)
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обрабатываем исключения
        processed_results = []
        for i, result in enumerate(results):
            query_name = list(queries.keys())[i]
            if isinstance(result, Exception):
                processed_results.append(QueryResult(
                    query_name=query_name,
                    success=False,
                    execution_time=0,
                    error=str(result),
                    started_at=datetime.utcnow(),
                    completed_at=datetime.utcnow()
                ))
            else:
                processed_results.append(result)
        
        logger.info("Все асинхронные запросы завершены")
        return processed_results
        
    async def execute_transaction(
        self,
        queries: List[Dict[str, Any]]
    ) -> List[QueryResult]:
        """
        Выполнение нескольких запросов в одной транзакции
        
        Args:
            queries: Список запросов с параметрами
            
        Returns:
            Список результатов
        """
        results = []
        
        try:
            async with self.get_connection() as conn:
                async with conn.transaction():
                    for i, query_data in enumerate(queries):
                        sql = query_data.get('sql', '')
                        query_name = query_data.get('name', f"Transaction Query {i}")
                        params = query_data.get('params')
                        
                        start_time = asyncio.get_event_loop().time()
                        try:
                            is_select = sql.strip().upper().startswith('SELECT')
                            
                            if is_select:
                                rows = await conn.fetch(sql, *params) if params else await conn.fetch(sql)
                                data = [dict(row) for row in rows]
                                rows_affected = len(data)
                            else:
                                await conn.execute(sql, *params) if params else await conn.execute(sql)
                                rows_affected = 0
                                data = None
                            
                            results.append(QueryResult(
                                query_name=query_name,
                                success=True,
                                execution_time=asyncio.get_event_loop().time() - start_time,
                                rows_affected=rows_affected,
                                data=data,
                                started_at=datetime.utcnow(),
                                completed_at=datetime.utcnow()
                            ))
                            
                        except Exception as e:
                            results.append(QueryResult(
                                query_name=query_name,
                                success=False,
                                execution_time=asyncio.get_event_loop().time() - start_time,
                                error=str(e),
                                started_at=datetime.utcnow(),
                                completed_at=datetime.utcnow()
                            ))
                            raise  # Откатываем всю транзакцию
                            
        except Exception as e:
            logger.error(f"Ошибка в транзакции: {e}")
            
        return results
    
    async def close(self) -> None:
        """Закрытие пула соединений"""
        if self.connection_pool:
            await self.connection_pool.close()
            logger.info("Асинхронный пул соединений закрыт")

class PostgresRunAsync:
    """Асинхронный раннер для выполнения SQL запросов"""
    
    @staticmethod
    async def run_async(
        database_params: Optional[Dict[str, Any]] = None,
        scripts_data: Optional[Dict[str, Dict[str, Any]]] = None,
        max_concurrent: Optional[int] = None
    ) -> List[QueryResult]:
        """
        Асинхронное выполнение SQL запросов
        
        Args:
            database_params: Параметры подключения к БД
            scripts_data: Словарь с SQL скриптами
            max_concurrent: Максимальное количество одновременных запросов
            
        Returns:
            Список результатов
        """
        executor = None
        
        try:
            # Логирование начала выполнения
            logger.info("=" * 50)
            logger.info("Начало асинхронного выполнения SQL-скриптов")
            logger.info("=" * 50)
            
            # Используем настройки по умолчанию или переданные параметры
            db_params = database_params or settings.DATABASE_PARAMS
            scripts_to_run = scripts_data or scripts.scripts
            
            # Создаем и инициализируем исполнитель
            executor = PostgresExecutorAsync(**db_params)
            await executor.initialize()
            
            # Выполняем запросы параллельно
            results = await executor.execute_queries_concurrently(
                queries=scripts_to_run,
                max_concurrent=max_concurrent
            )
            
            # Логирование результатов
            await PostgresRunAsync._log_results(results)
            
            return results
            
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            raise
        finally:
            # Закрываем соединения
            if executor:
                await executor.close()
    
    @staticmethod
    async def _log_results(results: List[QueryResult]) -> None:
        """Логирование результатов выполнения"""
        logger.info("")
        logger.info("=" * 50)
        logger.info("РЕЗУЛЬТАТЫ ВЫПОЛНЕНИЯ:")
        logger.info("=" * 50)
        
        success_count = 0
        for result in results:
            if result != None:
                status = "УСПЕХ" if result.success else "ОШИБКА"
                logger.info("")
                logger.info(f"Запрос: {result.query_name}")
                logger.info(f"Статус: {status}")
                logger.info(f"Время выполнения: {result.execution_time:.2f} сек")
                logger.info(f"Строк обработано: {result.rows_affected}")
                
                if result.success:
                    success_count += 1
                else:
                    logger.error(f"Ошибка: {result.error}")
            else:
                logger.error(f"Ошибка: результат выполнения не определен")
        
        logger.info("")
        logger.info("=" * 50)
        logger.info(f"ИТОГО: {success_count}/{len(results)} запросов выполнены успешно")
        logger.info("=" * 50)
    
    @staticmethod
    def info() -> str:
        return """
Асинхронное выполнение SQL запросов
1. Установить параметры подключения к БД. Файл .env
2. Заполнить перечень запросов, которые необходимо выполнить. файл scripts.yml

Пример заполнения файла scripts.yml:
scripts:
  "Проверка подключения":
    sql: |
      SELECT 1 as VALUE
    name: "Проверка подключения"
    type: command
    timeout: 30  # опционально, таймаут в секундах

  "Выполнение запроса":
    sql: |
      SELECT * FROM users WHERE active = $1
    name: "Выполнение запроса"
    type: select
    params: [true]  # параметры для запроса

Для запуска используйте:
    results = await PostgresAsyncRunner.run_async()
"""

    # Статический метод запуска
    @staticmethod
    def run():

        async def _run() -> List[QueryResult]:
            return await PostgresRunAsync.run_async()
        
        asyncio.run(_run())


    # # Синхронная обертка для обратной совместимости
    # @staticmethod
    # def run_sync_wrapper(
    #     database_params: Optional[Dict[str, Any]] = None,
    #     scripts_data: Optional[Dict[str, Dict[str, Any]]] = None,
    #     max_concurrent: Optional[int] = None
    # ) -> List[Dict[str, Any]]:
    #     """
    #     Синхронная обертка для запуска асинхронного выполнения
        
    #     Args:
    #         database_params: Параметры подключения к БД
    #         scripts_data: Словарь с SQL скриптами
    #         max_concurrent: Максимальное количество одновременных запросов
            
    #     Returns:
    #         Список результатов в виде словарей
    #     """
    #     async def _run() -> List[QueryResult]:
    #         return await PostgresRunAsync.run_async(
    #             database_params=database_params,
    #             scripts_data=scripts_data,
    #             max_concurrent=max_concurrent
    #         )
        
    #     # Запускаем асинхронный код
    #     results = asyncio.run(_run())
        
    #     # Конвертируем в словари для обратной совместимости
    #     return [result.to_dict() for result in results]