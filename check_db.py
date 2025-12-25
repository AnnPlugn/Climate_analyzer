"""
Скрипт для проверки подключения к PostgreSQL базе данных
"""
import os
import sys
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# Настройки подключения
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def check_connection(max_retries=10, retry_delay=2):
    """
    Проверка подключения к базе данных с повторными попытками
    
    Args:
        max_retries: максимальное количество попыток
        retry_delay: задержка между попытками в секундах
    """
    print("=" * 60)
    print("Проверка подключения к PostgreSQL")
    print("=" * 60)
    print(f"Хост: {POSTGRES_HOST}")
    print(f"Порт: {POSTGRES_PORT}")
    print(f"База данных: {POSTGRES_DB}")
    print(f"Пользователь: {POSTGRES_USER}")
    print("=" * 60)
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"\nПопытка подключения {attempt}/{max_retries}...")
            
            # Создаем подключение
            engine = create_engine(
                DATABASE_URL,
                pool_pre_ping=True,
                connect_args={"connect_timeout": 5}
            )
            
            # Пытаемся выполнить простой запрос
            with engine.connect() as conn:
                result = conn.execute(text("SELECT version();"))
                version = result.fetchone()[0]
                print(f"✓ Подключение успешно!")
                print(f"  Версия PostgreSQL: {version}")
                
                # Проверяем существование базы данных
                result = conn.execute(text("SELECT current_database();"))
                db_name = result.fetchone()[0]
                print(f"  Текущая база данных: {db_name}")
                
                # Проверяем таблицы
                result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    ORDER BY table_name;
                """))
                tables = [row[0] for row in result.fetchall()]
                
                if tables:
                    print(f"  Найдено таблиц: {len(tables)}")
                    for table in tables:
                        # Проверяем количество записей
                        count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table};"))
                        count = count_result.fetchone()[0]
                        print(f"    - {table}: {count} записей")
                else:
                    print("  Таблицы не найдены (база данных пуста)")
                
                print("\n" + "=" * 60)
                print("✓ База данных работает корректно!")
                print("=" * 60)
                return True
                
        except OperationalError as e:
            error_msg = str(e)
            print(f"✗ Ошибка подключения: {error_msg}")
            
            if attempt < max_retries:
                print(f"  Повторная попытка через {retry_delay} секунд...")
                time.sleep(retry_delay)
            else:
                print("\n" + "=" * 60)
                print("✗ Не удалось подключиться к базе данных")
                print("=" * 60)
                print("\nВозможные причины:")
                print("1. PostgreSQL не запущен")
                print("2. Неправильные учетные данные")
                print("3. База данных не существует")
                print("4. Проблемы с сетью/портом")
                print("\nПроверьте:")
                print(f"  - Запущен ли контейнер: docker ps | grep postgres")
                print(f"  - Логи контейнера: docker logs <container_name>")
                print(f"  - Переменные окружения в .env файле")
                return False
                
        except Exception as e:
            print(f"✗ Неожиданная ошибка: {e}")
            return False
    
    return False

if __name__ == "__main__":
    success = check_connection()
    sys.exit(0 if success else 1)

