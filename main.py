from data_parser import DataParser
from database_manager import DatabaseManager
from data_analyzer import DataAnalyzer
from telegram_reporter import TelegramReporter

import time

from config import *


def exception_way():
    tg_reporter.send_error_message(TG_CHAT_ADMIN, "<b>⚠️ Возникла непредвиденная ошибка, отчёта сегодня не будет 😔</b>")
    tg_reporter.send_error_message(TG_CHAT_CHANNEL, "<b>⚠️ Возникла непредвиденная ошибка, отчёта сегодня не будет 😔</b>")
    db_manager.rollback_tables()
    db_manager.connection_close()
    exit()


parser = DataParser()
db_manager = DatabaseManager(host=MYSQL_HOST,
                             port=MYSQL_PORT,
                             user=MYSQL_USERNAME,
                             password=MYSQL_PASSWORD,
                             database=MYSQL_DB_NAME)
analyzer = DataAnalyzer()
tg_reporter = TelegramReporter(tg_token=TG_TOKEN)

db_manager.connect()
db_manager.prepare_database()
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

tg_reporter.send_document(TG_CHAT_ADMIN, report_file_path, "Отчет в .txt формате")
tg_reporter.send_document(TG_CHAT_ADMIN, differences_file_path, "Отчет в .json формате")

tg_reporter.send_document(TG_CHAT_CHANNEL, report_file_path, """
<b>🔄 База студентов была обновлена.</b>

📎 Отчёт в прикреплённом файле

<i>📌 Инструкция <a href="https://t.me/c/2191201472/8">«как понимать отчёт»</a></i>""")


db_manager.connection_close()
