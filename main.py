from data_parser import DataParser
from database_manager import DatabaseManager

from config import *

parser = DataParser()
db_manager = DatabaseManager(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USERNAME, password=MYSQL_PASSWORD, database=MYSQL_DB_NAME)

db_manager.connect()
db_manager.create_tables()

for chunk in parser.parse_data():
    print(chunk)

# db_manager.close()