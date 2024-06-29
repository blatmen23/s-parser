from data_parser import DataParser
from database_manager import DatabaseManager

from config import *

parser = DataParser()
db_manager = DatabaseManager(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USERNAME, password=MYSQL_PASSWORD, database=MYSQL_DB_NAME)

db_manager.connect()
db_manager.prepare_tables()

for chunk_data in parser.parse_data():
    print(chunk_data)
    db_manager.save_data(chunk_data)

# db_manager.close()