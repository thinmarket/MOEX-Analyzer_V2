import os
import json
from datetime import datetime
import logging
import pandas as pd

from moexparser2 import MOEXDataCollector
from analiz import TickerAnalyzer, load_trade_files_from_folder, load_trades_from_file
from plot_report import plot_report
from report_generator import ReportGenerator
from rank_candidates import run_ranking

# --- Настройки ---
CONFIG = {
    "tickers_to_process": [],
    "raw_data_folder": "ANALIZ_final/moex_data",
    "analysis_folder": "ANALIZ_final/analysis_results",
    "reports_folder": "ANALIZ_final/final_reports"
}

# --- Настройка логирования ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ANALIZ_final/automation.log"),
        logging.StreamHandler()
    ]
)

def create_folders():
    """Создает необходимые папки, если их нет."""
    for folder in [
        CONFIG["raw_data_folder"],
        CONFIG["analysis_folder"],
        CONFIG["reports_folder"]
    ]:
        os.makedirs(folder, exist_ok=True)

def step_1_collect_data():
    """Шаг 1: Сбор данных с Московской биржи."""
    logging.info("--- Шаг 1: Начало сбора данных ---")
    collector = MOEXDataCollector(data_folder=CONFIG["raw_data_folder"])

    tickers_to_process = CONFIG["tickers_to_process"]

    if not tickers_to_process:
        logging.info("Список тикеров не указан. Собираем данные по всем инструментам.")
        try:
            instruments = collector.get_instruments_list()
            all_instruments = instruments['shares'] + instruments['futures']
            total = len(all_instruments)
            logging.info(f"Найдено {total} инструментов.")

            for i, instrument in enumerate(all_instruments):
                ticker = instrument['ticker']
                market_type = 'shares' if instrument in instruments['shares'] else 'futures'
                logging.info(f"({i+1}/{total}) Сбор данных для: {ticker}")
                data = collector.get_trades_data(ticker, market_type)
                if data:
                    collector.save_data(data, ticker, 'trades', market_type)
            logging.info("Сбор данных по всем инструментам завершен.")
        except Exception as e:
            logging.error(f"Ошибка при сборе данных по всем инструментам: {e}", exc_info=True)
            return False
    else:
        logging.info(f"Будут обработаны тикеры: {tickers_to_process}")
        try:
            instruments = collector.get_instruments_list()
            ticker_map = {inst['ticker']: ('shares' if inst in instruments['shares'] else 'futures')
                          for inst in instruments['shares'] + instruments['futures']}

            for ticker in tickers_to_process:
                if ticker in ticker_map:
                    market_type = ticker_map[ticker]
                    logging.info(f"Сбор данных для: {ticker}")
                    data = collector.get_trades_data(ticker, market_type)
                    if data:
                        collector.save_data(data, ticker, 'trades', market_type)
                else:
                    logging.warning(f"Тикер {ticker} не найден в списке инструментов.")
        except Exception as e:
            logging.error(f"Ошибка при сборе данных по указанным тикерам: {e}", exc_info=True)
            return False

    logging.info("--- Шаг 1: Сбор данных завершен ---")
    return True

def step_2_run_analysis():
    """Шаг 2: Запуск анализа на основе собранных данных."""
    logging.info("--- Шаг 2: Начало анализа данных ---")

    trades_folder = os.path.join(CONFIG["raw_data_folder"], 'trades')
    trade_files = load_trade_files_from_folder(trades_folder)

    if not trade_files:
        logging.error("Не найдено файлов для анализа. Проверьте папку с данными.")
        return False

    logging.info(f"Найдено {len(trade_files)} файлов для анализа.")

    def json_serializer(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        raise TypeError(f"Объект типа {type(obj)} не сериализуем")

    for i, file_path in enumerate(trade_files):
        ticker = os.path.basename(file_path).split('_')[0]
        logging.info(f"({i+1}/{len(trade_files)}) Анализ файла: {os.path.basename(file_path)}")

        try:
            # Загружаем данные только для одного тикера
            ticker_df = load_trades_from_file(file_path)
            if ticker_df is None or ticker_df.empty:
                logging.warning(f"Файл {os.path.basename(file_path)} пуст или содержит ошибки. Пропускаем.")
                continue

            analyzer = TickerAnalyzer(ticker_df)
            report = analyzer.run_full_analysis()

            today = datetime.now().strftime('%Y-%m-%d')
            dated_analysis_folder = os.path.join(CONFIG["analysis_folder"], today)
            os.makedirs(dated_analysis_folder, exist_ok=True)
            output_filename = os.path.join(dated_analysis_folder, f'analysis_{ticker}.json')

            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=4, default=json_serializer)
            logging.info(f"Анализ для {ticker} сохранен в {output_filename}")

        except Exception as e:
            logging.error(f"Ошибка при анализе файла {os.path.basename(file_path)}: {e}", exc_info=True)

    logging.info("--- Шаг 2: Анализ данных завершен ---")
    return True

def step_3_generate_reports():
    """Шаг 3: Генерация графических и текстовых отчетов."""
    logging.info("--- Шаг 3: Начало генерации отчетов ---")

    today = datetime.now().strftime('%Y-%m-%d')
    today_folder = os.path.join(CONFIG["analysis_folder"], today)
    if os.path.exists(today_folder):
        analysis_files = [
            os.path.join(today_folder, f)
            for f in os.listdir(today_folder)
            if f.startswith('analysis_') and f.endswith('.json')
        ]
    else:
        analysis_files = []

    if not analysis_files:
        logging.warning("Не найдено файлов для анализа. Пропускаем генерацию отчетов.")
        return False

    logging.info(f"Найдено {len(analysis_files)} файлов для генерации отчетов.")

    for i, json_path in enumerate(analysis_files):
        ticker = os.path.basename(json_path).replace('analysis_', '').replace('.json', '')
        logging.info(f"({i+1}/{len(analysis_files)}) Генерация отчета для: {ticker}")

        try:
            plot_report(json_path, output_folder=CONFIG["analysis_folder"])
            logging.info(f"График для {ticker} сохранен.")

            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            generator = ReportGenerator(data)
            text_report = generator.generate_full_report()

            today = datetime.now().strftime('%Y-%m-%d')
            dated_reports_folder = os.path.join(CONFIG["reports_folder"], today)
            os.makedirs(dated_reports_folder, exist_ok=True)
            report_filename = os.path.join(dated_reports_folder, f'report_{ticker}.txt')
            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write(text_report)
            logging.info(f"Текстовый отчет для {ticker} сохранен в {report_filename}")

        except Exception as e:
            logging.error(f"Ошибка при генерации отчета для {ticker}: {e}", exc_info=True)

    logging.info("--- Шаг 3: Генерация отчетов завершена ---")
    return True

def step_4_rank_candidates():
    """Шаг 4: Ранжирование кандидатов на основе отчетов."""
    logging.info("--- Шаг 4: Начало ранжирования кандидатов ---")
    try:
        from datetime import datetime
        today = datetime.now().strftime('%Y-%m-%d')
        today_reports_folder = os.path.join(CONFIG["reports_folder"], today)
        run_ranking(today_reports_folder)
        logging.info("--- Шаг 4: Ранжирование кандидатов завершено ---")
        return True
    except Exception as e:
        logging.error(f"Ошибка при ранжировании кандидатов: {e}", exc_info=True)
        return False

def main():
    """Главная функция для запуска всех шагов автоматизации."""
    logging.info("=== Запуск автоматического анализа ===")

    create_folders()

    if step_1_collect_data():
        if step_2_run_analysis():
            if step_3_generate_reports():
                step_4_rank_candidates()

    logging.info("=== Автоматический анализ завершен ===")

if __name__ == "__main__":
    main()
