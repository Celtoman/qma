"""
Query Monitoring Analyzer 4.3
Скрипт для анализа поисковых запросов из Яндекс.Вебмастера
"""

import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
import sys
import argparse
from typing import Optional, Set, Tuple
from collections import Counter
from tqdm import tqdm

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('qma.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Определение рабочей директории
SCRIPT_DIR = Path(__file__).parent.resolve()


def get_excel_files() -> list[str]:
    """Получить список Excel файлов в текущей директории."""
    try:
        return [f.name for f in SCRIPT_DIR.glob('*.xlsx') if not f.name.startswith('~$')]
    except Exception as e:
        logger.error(f"Ошибка при поиске Excel файлов: {e}")
        return []


def select_file() -> Optional[str]:
    """Выбрать Excel файл из списка доступных."""
    excel_files = get_excel_files()
    
    if not excel_files:
        print('Excel файлы не найдены в текущей директории.')
        input("\nНажмите Enter для завершения...")
        return None
    
    print("\nДоступные Excel файлы:")
    for i, file in enumerate(excel_files, 1):
        print(f"[{i}] {file}")
    
    while True:
        try:
            choice = int(input("\nВыберите номер файла: "))
            if 1 <= choice <= len(excel_files):
                selected_file = excel_files[choice - 1]
                logger.info(f"Выбран файл: {selected_file}")
                return selected_file
            print("Неверный номер. Попробуйте еще раз.")
        except ValueError:
            print("Введите число.")
        except KeyboardInterrupt:
            print("\n\nОперация прервана пользователем.")
            sys.exit(0)


def load_urls_from_file() -> Optional[Set[str]]:
    """Загрузить список URL из файла urls.txt."""
    urls_file = SCRIPT_DIR / 'urls.txt'
    
    try:
        if urls_file.exists():
            with open(urls_file, 'r', encoding='utf-8') as file:
                urls = {line.strip() for line in file if line.strip()}
            
            if urls:
                print(f"\nЗагружено {len(urls)} URL-адресов из файла urls.txt")
                logger.info(f"Загружено {len(urls)} URL из urls.txt")
                return urls
            else:
                logger.warning("Файл urls.txt пуст")
        return None
    except Exception as e:
        logger.error(f"Ошибка при чтении файла urls.txt: {e}")
        print(f"\nОшибка при чтении файла urls.txt: {e}")
        return None


def determine_report_type(df: pd.DataFrame) -> Optional[int]:
    """
    Определить тип отчёта.
    
    Returns:
        1 - отчёт по запросам (с demand)
        None - неподдерживаемый формат
    """
    columns = df.columns.tolist()
    has_demand = any(col.endswith('_demand') for col in columns)
    
    if has_demand:
        return 1
    
    return None


def load_data(file_path: Path) -> Optional[pd.DataFrame]:
    """Загрузить данные из Excel файла."""
    try:
        if not file_path.exists():
            logger.error(f'Файл {file_path} не найден')
            print(f'Файл {file_path} не найден.')
            input("\nНажмите Enter для завершения...")
            return None
        
        df = pd.read_excel(file_path)
        logger.info(f"Данные успешно загружены из {file_path.name}. Строк: {len(df)}")
        return df
        
    except PermissionError:
        logger.error(f"Нет доступа к файлу {file_path}. Возможно, файл открыт в Excel.")
        print(f"\nОШИБКА: Нет доступа к файлу {file_path.name}")
        print("Закройте файл в Excel и попробуйте снова.")
        input("\nНажмите Enter для завершения...")
        return None
    except Exception as e:
        logger.error(f"Ошибка при чтении файла {file_path}: {e}")
        print(f"\nОшибКА при чтении файла: {e}")
        input("\nНажмите Enter для завершения...")
        return None


def add_word_count_column(df: pd.DataFrame) -> pd.DataFrame:
    """Добавить столбец с количеством слов в запросе."""
    try:
        df['Число слов в запросе'] = df['Query'].apply(lambda x: len(str(x).split()))
        return df
    except Exception as e:
        logger.error(f"Ошибка при подсчете слов: {e}")
        df['Число слов в запросе'] = 0
        return df


def create_output_file_name(domain: str) -> str:
    """Создать имя выходного файла."""
    current_date = datetime.now().strftime('%Y-%m-%d')
    return f"{domain}-webmaster-{current_date}.xlsx"


def create_word_count_df(df: pd.DataFrame) -> pd.DataFrame:
    """Создать DataFrame со статистикой по словам (оптимизированная версия)."""
    try:
        # Объединение всех запросов и разбиение на слова (векторизованная операция)
        all_words = ' '.join(df['Query'].astype(str)).split()
        
        # Использование Counter для быстрого подсчёта
        word_count = Counter(all_words)
        
        word_count_df = pd.DataFrame(
            word_count.items(), 
            columns=['Слово', 'Количество']
        )
        logger.info(f"Создана статистика по {len(word_count_df)} уникальным словам")
        return word_count_df
    except Exception as e:
        logger.error(f"Ошибка при создании статистики слов: {e}")
        return pd.DataFrame(columns=['Слово', 'Количество'])


def filter_by_urls(df: pd.DataFrame, urls_set: Optional[Set[str]], site_url: str) -> pd.DataFrame:
    """Фильтровать данные по списку URL."""
    if urls_set is None:
        return df
    
    try:
        df_urls = {site_url + url for url in df['Url']}
        urls_intersection = df_urls.intersection(urls_set)
        
        if not urls_intersection:
            logger.warning("Ни один URL из файла urls.txt не найден в данных")
            print("\nВНИМАНИЕ: Ни один URL из файла urls.txt не найден в данных!")
            return df
        
        filtered_df = df[df['Полный URL'].isin(urls_intersection)]
        print(f"\nОтфильтровано записей: {len(filtered_df)} из {len(df)}")
        logger.info(f"Отфильтровано {len(filtered_df)} записей из {len(df)}")
        return filtered_df
    except Exception as e:
        logger.error(f"Ошибка при фильтрации по URL: {e}")
        return df


def filter_queries_with_urls(df: pd.DataFrame, domain: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Фильтровать запросы, содержащие URL или домены.
    
    Returns:
        Tuple: (чистый DataFrame без URL, DataFrame с отфильтрованными запросами)
    """
    try:
        url_pattern = r'(?:https?:\/\/)?(?:[\w-]+\.)+[\w-]+(?:\/[\w-]+)*\/?'
        domain_pattern = domain.replace('.', r'\.').lower()
        
        mask = (
            df['Query'].str.contains(url_pattern, case=False, na=False, regex=True) | 
            df['Query'].str.contains(domain_pattern, case=False, na=False, regex=True)
        )
        
        filtered_df = df[mask].copy()
        clean_df = df[~mask].copy()
        
        logger.info(f"Отфильтровано {len(filtered_df)} запросов с URL/доменами")
        return clean_df, filtered_df
    except Exception as e:
        logger.error(f"Ошибка при фильтрации запросов с URL: {e}")
        return df, pd.DataFrame()


def safe_mean(series_df: pd.DataFrame, round_digits: int = 1) -> pd.Series:
    """Безопасное вычисление среднего значения с округлением."""
    try:
        return (
            pd.to_numeric(series_df.mean(axis=1), errors='coerce')
            .round(round_digits)
            .fillna(0)
        )
    except Exception as e:
        logger.error(f"Ошибка при вычислении среднего: {e}")
        return pd.Series([0] * len(series_df))


def process_query_report(df: pd.DataFrame, site_url: str, domain: str, urls_set: Optional[Set[str]]) -> Optional[Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]]:
    """
    Обработать отчёт по поисковым запросам.
    
    Returns:
        Tuple: (основной DataFrame, отфильтрованные запросы, статистика слов)
    """
    try:
        print("\nОбнаружен отчет по поисковым запросам. Обработка...")
        logger.info("Начало обработки отчёта по запросам")
        
        # Добавление количества слов
        df = add_word_count_column(df)
        
        # Получение списков столбцов по типам
        demand_columns = [col for col in df.columns if col.endswith('_demand')]
        shows_columns = [col for col in df.columns if col.endswith('_shows')]
        position_columns = [col for col in df.columns if col.endswith('_position')]
        clicks_columns = [col for col in df.columns if col.endswith('_clicks')]
        
        if not demand_columns:
            logger.error("Не найдены столбцы с частотностью (_demand)")
            print("\nОШИБКА: Неверный формат файла. Не найдены столбцы с частотностью.")
            return None
        
        # Расчёт суммарных показателей
        df['Сум. частотность'] = df[demand_columns].sum(axis=1)
        df['Сум. показов'] = df[shows_columns].sum(axis=1)
        df['Сум. кликов'] = df[clicks_columns].sum(axis=1)
        
        # Расчёт средних показателей
        df['position'] = safe_mean(df[position_columns], 1)
        df['Demand'] = safe_mean(df[demand_columns], 0)
        df['Shows'] = safe_mean(df[shows_columns], 0)
        df['Ср. число кликов'] = safe_mean(df[clicks_columns], 0)
        
        # Расчёт CTR (векторизованная операция)
        df['CTR'] = ((df['Сум. кликов'] / df['Сум. показов'].replace(0, 1)) * 100).round(1)
        df.loc[df['Сум. показов'] == 0, 'CTR'] = 0
        
        # Создание полного URL
        df['Полный URL'] = site_url + df['Url']
        
        # Формирование результата
        result_df = df.loc[df['Сум. частотность'] >= 0].sort_values(
            by='Сум. частотность', 
            ascending=False
        )
        
        result_df = result_df[[
            'Query', 'Url', 'Полный URL', 'Число слов в запросе',
            'position', 'Demand', 'Shows', 'Сум. показов', 'Сум. частотность',
            'Ср. число кликов', 'Сум. кликов', 'CTR'
        ]]
        
        result_df = result_df.rename(columns={'Url': 'Относительный URL'})
        
        # Фильтрация запросов с URL
        clean_df, queries_with_urls = filter_queries_with_urls(result_df, domain)
        
        # Фильтрация по списку URL (если есть)
        if urls_set:
            clean_df = filter_by_urls(clean_df, urls_set, site_url)
        
        # Создание статистики слов
        word_count_df = create_word_count_df(clean_df)
        word_count_df = word_count_df.sort_values(by='Количество', ascending=False)
        
        logger.info("Обработка отчёта завершена успешно")
        return clean_df, queries_with_urls, word_count_df
        
    except Exception as e:
        logger.error(f"Ошибка при обработке отчёта: {e}")
        print(f"\nОШИБКА при обработке данных: {e}")
        return None


def save_results(output_file: Path, clean_df: pd.DataFrame, queries_with_urls: pd.DataFrame, word_count_df: pd.DataFrame) -> bool:
    """Сохранить результаты в Excel файл."""
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            clean_df.to_excel(writer, index=False, sheet_name='Семантическое ядро')
            
            if not queries_with_urls.empty:
                queries_with_urls.to_excel(writer, index=False, sheet_name='Отфильтровано')
            
            word_count_df.to_excel(writer, index=False, sheet_name='Статистика слов')
        
        logger.info(f"Результаты сохранены в файл {output_file.name}")
        return True
        
    except PermissionError:
        logger.error(f"Нет доступа для записи в файл {output_file}")
        print(f"\nОШИБКА: Не удалось сохранить файл {output_file.name}")
        print("Возможно, файл открыт в Excel. Закройте его и попробуйте снова.")
        return False
    except Exception as e:
        logger.error(f"Ошибка при сохранении файла: {e}")
        print(f"\nОШИБКА при сохранении файла: {e}")
        return False


def parse_arguments() -> argparse.Namespace:
    """Парсинг аргументов командной строки."""
    parser = argparse.ArgumentParser(
        description='Query Monitoring Analyzer - анализ поисковых запросов из Яндекс.Вебмастера',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  python "QMA 4.2.py"                                    # Интерактивный режим
  python "QMA 4.2.py" --file report.xlsx --site https://example.com
  python "QMA 4.2.py" -f report.xlsx -s https://example.com --no-wait
        """
    )
    
    parser.add_argument(
        '-f', '--file',
        type=str,
        help='Путь к Excel файлу с отчётом из Яндекс.Вебмастера'
    )
    
    parser.add_argument(
        '-s', '--site',
        type=str,
        help='Адрес сайта в формате https://site.ru'
    )
    
    parser.add_argument(
        '--no-wait',
        action='store_true',
        help='Не ждать нажатия Enter в конце (для автоматизации)'
    )
    
    parser.add_argument(
        '-v', '--version',
        action='version',
        version='Query Monitoring Analyzer 4.3'
    )
    
    return parser.parse_args()


def main() -> None:
    """Главная функция программы."""
    args = None
    try:
        # Парсинг CLI аргументов
        args = parse_arguments()
        
        logger.info("=" * 50)
        logger.info("Запуск Query Monitoring Analyzer 4.3")
        logger.info("=" * 50)
        
        # Выбор файла (CLI или интерактивно)
        if args.file:
            input_file_name = args.file
            input_file = Path(input_file_name)
            if not input_file.exists():
                print(f"ОШИБКА: Файл {input_file_name} не найден.")
                return
            logger.info(f"Используется файл из CLI: {input_file_name}")
        else:
            input_file_name = select_file()
            if input_file_name is None:
                return
            input_file = SCRIPT_DIR / input_file_name
        
        # Загрузка данных с прогресс-баром
        print("\nЗагрузка данных...")
        df = load_data(input_file)
        if df is None:
            return
        
        # Определение типа отчёта
        mode = determine_report_type(df)
        if mode != 1:
            logger.warning("Неподдерживаемый тип отчёта")
            print("\nОШИБКА: Это не семантический отчёт.")
            print("Скрипт поддерживает только отчёты по поисковым запросам из Яндекс.Вебмастера.")
            if not args.no_wait:
                input("\nНажмите Enter для завершения...")
            return
        
        # Получение адреса сайта (CLI или интерактивно)
        if args.site:
            site_url = args.site
            logger.info(f"Используется адрес сайта из CLI: {site_url}")
        else:
            site_url = input("\nПожалуйста, введите адрес сайта в формате https://site.ru: ").strip()
        
        if not site_url:
            print("\nОШИБКА: Адрес сайта не может быть пустым.")
            return
        
        domain = site_url.split('//')[-1].split('/')[0]
        logger.info(f"Анализ для домена: {domain}")
        
        # Загрузка списка URL для фильтрации
        urls_set = load_urls_from_file()
        
        # Обработка отчёта с прогресс-баром
        print("\nОбработка данных...")
        with tqdm(total=100, desc="Прогресс", bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt}') as pbar:
            pbar.update(20)
            result = process_query_report(df, site_url, domain, urls_set)
            pbar.update(60)
            
            if result is None:
                return
            
            clean_df, queries_with_urls, word_count_df = result
            pbar.update(20)
        
        # Создание имени выходного файла
        output_file_name = create_output_file_name(domain)
        output_file = SCRIPT_DIR / output_file_name if not args.file else Path(input_file_name).parent / output_file_name
        
        # Сохранение результатов
        print("\nСохранение результатов...")
        if save_results(output_file, clean_df, queries_with_urls, word_count_df):
            print("\n" + "=" * 50)
            print(f"[OK] Результат сохранен в файл {output_file_name}")
            print(f"[OK] Обработано поисковых запросов: {len(clean_df)}")
            print(f"[OK] Отфильтровано запросов с URL: {len(queries_with_urls)}")
            print("=" * 50)
        
        logger.info("Программа завершена успешно")
        
    except SystemExit:
        # Нормальный выход (например, при --help или --version)
        raise
    except KeyboardInterrupt:
        logger.info("Программа прервана пользователем")
        print("\n\nРабота программы прервана пользователем.")
    except Exception as e:
        logger.error(f"Непредвиденная ошибка: {e}", exc_info=True)
        print(f"\nНепредвиденная ошибка: {e}")
    finally:
        if args and not args.no_wait:
            input("\nНажмите Enter для завершения...")


if __name__ == "__main__":
    main()
