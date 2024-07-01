from data_parser import DataParser
from database_manager import DatabaseManager
from data_analyzer import DataAnalyzer

import time

from config import *

parser = DataParser()
db_manager = DatabaseManager(host=MYSQL_HOST,
                             port=MYSQL_PORT,
                             user=MYSQL_USERNAME,
                             password=MYSQL_PASSWORD,
                             database=MYSQL_DB_NAME)
analyzer = DataAnalyzer()

db_manager.connect()

# db_manager.prepare_tables()
start_time = int(time.time())
# data = parser.parse_data() # генератор
end_time = int(time.time())
# for chunk_data in data:
#     db_manager.save_data(chunk_data)

table_differences = db_manager.get_different_tables()
db_manager.archive_data(table_differences['left_students'])

analyzer.get_report(differences=table_differences,
                    start_time=start_time,
                    end_time=end_time,
                    total_students=db_manager.get_total_students(),
                    total_groups=db_manager.get_total_groups(),
                    )

db_manager.close()
