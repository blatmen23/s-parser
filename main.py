from data_parser import DataParser
from database_manager import DatabaseManager
from data_analyzer import DataAnalyzer
from telegram_reporter import TelegramReporter

import time

from config import *


def exception_way():
    tg_reporter.send_error_message("⚠️ Возникла непредвиденная ошибка, отчёта сегодня не будет 😔")
    db_manager.rollback_tables()
    exit()


parser = DataParser()
db_manager = DatabaseManager(host=MYSQL_HOST,
                             port=MYSQL_PORT,
                             user=MYSQL_USERNAME,
                             password=MYSQL_PASSWORD,
                             database=MYSQL_DB_NAME)
analyzer = DataAnalyzer()
tg_reporter = TelegramReporter(tg_token=TG_TOKEN,
                               chat_id=TG_CHAT)

db_manager.connect()
prepare_tables_result = db_manager.prepare_tables()

if prepare_tables_result == 'exception':
    exception_way()

start_time = int(time.time())
data = parser.parse_data()  # генератор
for chunk_data in data:
    if chunk_data == 'exception':
        exception_way()
    db_manager.save_data(chunk_data)

end_time = int(time.time())

table_differences = db_manager.get_different_tables()
db_manager.archive_data(table_differences['left_students'])

differences_file_path, report_file_path = analyzer.get_report(differences=table_differences,
                                                              start_time=start_time,
                                                              end_time=end_time,
                                                              total_students=db_manager.get_total_students(),
                                                              total_groups=db_manager.get_total_groups(),
                                                              )

tg_reporter.send_document(report_file_path, "Отчет в .txt формате")
tg_reporter.send_document(differences_file_path, "Отчет в .json формате")

db_manager.connection_close()
