from data_parser import DataParser
from database_manager import DatabaseManager
from data_analyzer import DataAnalyzer
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
#
# for chunk_data in parser.parse_data():
#     db_manager.save_data(chunk_data)

table_differences = db_manager.get_different_tables()
db_manager.archive_data(table_differences['left_students'])
analyzer.save_differences(table_differences)

db_manager.close()
