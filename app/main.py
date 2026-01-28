from src.postgresExecutorThreads import PostgresRunThreads
from src.postgresExecutorAsync import PostgresRunAsync

if __name__ == "__main__":

    # Запуск основной версии
    # PostgresRunThreads.run()

    PostgresRunAsync.run()
    