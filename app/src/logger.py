
import logging
import os

LOG_FILE = os.getenv('LOG_FILE','/app/logs/sqlexecute.log')

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    
    handlers=[
        logging.FileHandler(
            filename=LOG_FILE,
            encoding='utf-8',
            mode='a'),  # Запись в файл
        logging.StreamHandler()  # Дополнительно вывод в консоль (опционально)
    ]
)

logger = logging.getLogger(__name__)