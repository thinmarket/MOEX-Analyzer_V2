import os
import re
import logging

class ReportAnalyzer:
    """
    Анализирует текстовый отчет и выставляет скоринговые баллы для лонга и шорта.
    """
    def __init__(self, report_text, ticker):
        self.text = report_text
        self.ticker = ticker
        self.long_score = 0
        self.short_score = 0
        self.long_reasons = []
        self.short_reasons = []

    def analyze(self):
        """
        Применяет набор правил для оценки отчета.
        """
        # --- Сигналы в ЛОНГ ---
        if re.search(r"рынок был под контролем быков", self.text, re.I):
            self.long_score += 2
            self.long_reasons.append("Бычий сентимент")
        if re.search(r"цена закрытия .*? находится выше vwap", self.text, re.I):
            self.long_score += 1
            self.long_reasons.append("Цена выше VWAP")
        if re.search(r"активный откуп на лоях", self.text, re.I):
            self.long_score += 2
            self.long_reasons.append("Активный откуп")
        if re.search(r"кульминация продаж", self.text, re.I):
            self.long_score += 3
            self.long_reasons.append("Кульминация продаж")
        if re.search(r"закол уровня poc.*?силе покупателей", self.text, re.I):
            self.long_score += 2
            self.long_reasons.append("Ложный пробой POC (сила покупателей)")

        # --- Сигналы в ШОРТ ---
        if re.search(r"медведи доминировали", self.text, re.I):
            self.short_score += 2
            self.short_reasons.append("Медвежий сентимент")
        if re.search(r"цена закрытия .*? находится ниже vwap", self.text, re.I):
            self.short_score += 1
            self.short_reasons.append("Цена ниже VWAP")
        if re.search(r"разгрузка на хаях", self.text, re.I):
            self.short_score += 2
            self.short_reasons.append("Разгрузка на хаях")
        if re.search(r"кульминация покупок", self.text, re.I):
            self.short_score += 3
            self.short_reasons.append("Кульминация покупок")
        if re.search(r"закол уровня poc.*?слабость покупателей", self.text, re.I):
            self.short_score += 2
            self.short_reasons.append("Ложный пробой POC (слабость покупателей)")

def rank_reports(reports_folder: str) -> tuple[list, list]:
    """
    Читает все отчеты из папки, анализирует и возвращает отсортированные списки кандидатов.
    """
    candidates = []
    logging.info(f"Анализ отчетов из папки: {reports_folder}")
    if not os.path.isdir(reports_folder):
        logging.error(f"Папка с отчетами не найдена: {reports_folder}")
        return [], []

    for filename in os.listdir(reports_folder):
        if filename.endswith(".txt"):
            ticker = filename.replace("report_", "").replace(".txt", "")
            file_path = os.path.join(reports_folder, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    report_text = f.read()

                analyzer = ReportAnalyzer(report_text, ticker)
                analyzer.analyze()
                candidates.append({
                    "ticker": ticker,
                    "long_score": analyzer.long_score,
                    "short_score": analyzer.short_score,
                    "long_reasons": analyzer.long_reasons,
                    "short_reasons": analyzer.short_reasons
                })
            except Exception as e:
                logging.error(f"Не удалось проанализировать отчет {filename}: {e}")

    # Сортировка кандидатов
    long_candidates = sorted([c for c in candidates if c['long_score'] > 0], key=lambda x: x['long_score'], reverse=True)
    short_candidates = sorted([c for c in candidates if c['short_score'] > 0], key=lambda x: x['short_score'], reverse=True)

    return long_candidates, short_candidates

def print_summary(long_candidates: list, short_candidates: list):
    """
    Печатает итоговый рейтинг кандидатов в консоль.
    """
    print("\n" + "="*50)
    print("ИТОГОВЫЙ РЕЙТИНГ КАНДИДАТОВ")
    print("="*50 + "\n")

    print("--- ТОП-5 кандидатов в ЛОНГ ---")
    if not long_candidates:
        print("Подходящих кандидатов в лонг не найдено.")
    else:
        for i, candidate in enumerate(long_candidates[:5]):
            print(f"{i+1}. {candidate['ticker']} (Рейтинг: {candidate['long_score']})")
            if candidate['long_reasons']:
                print(f"   - Основания: {', '.join(candidate['long_reasons'])}")

    print("\n" + "--- ТОП-5 кандидатов в ШОРТ ---")
    if not short_candidates:
        print("Подходящих кандидатов в шорт не найдено.")
    else:
        for i, candidate in enumerate(short_candidates[:5]):
            print(f"{i+1}. {candidate['ticker']} (Рейтинг: {candidate['short_score']})")
            if candidate['short_reasons']:
                print(f"   - Основания: {', '.join(candidate['short_reasons'])}")

def run_ranking(reports_folder: str):
    """
    Главная функция для запуска ранжирования.
    """
    long_candidates, short_candidates = rank_reports(reports_folder)
    print_summary(long_candidates, short_candidates)

if __name__ == '__main__':
    # Для самостоятельного запуска укажите путь к папке с отчетами
    # например, 'final_reports'
    if not os.path.isdir('ANALIZ_final/final_reports'):
        print("Папка 'ANALIZ_final/final_reports' не найдена. Запустите сначала основной скрипт.")
    else:
        run_ranking('ANALIZ_final/final_reports')
