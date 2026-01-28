# Запуск sql-скриптов в потоке

## Подготовка

### Требуется подготовить .env файл по примеру ./app/example.env

``` environment
# ./app/.env

# Database PostgreSQL
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_DB=test
DATABASE_USER=test
DATABASE_PASSWORD=test

# Ограничение по количеству подключений
DATABASE_CONNECTIONS_MIN=1
DATABASE_CONNECTIONS_MAX=10

# Расположение файла с sql-скриптами
SCRIPT_FILE_PATH=/app/config/example.scripts.yml
# Расположение файла логов
LOG_FILE=/app/logs/sqlexecute.log
```

### Требуется подготовить файл со скриптами по прмиеру ./app/config/example.scripts.yml

``` ansible
# scripts.yml

scripts:

    "Проверка подключения":
      sql: |
        SELECT 1 
        as VALUE
      type: command

    "Последние пять дней":
      sql: SELECT 1 as VALUE
```

## Запуск выполнения скриптов

### Запуск

``` command
docker compose up -d

docker-compose up -d
```

### Просмотр исполнения посредством просмотра логов docker compose

``` command
docker compose logs -f
```

### Просмотр исполнения посредством просмотра файла логов

Открыть файл ./app/logs/sqlexecute.log
