import pymysql.cursors

class DatabaseManager(object):
    connection = None

    def __init__(self, host: str, port: int, user: str, password:str, database: str):
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.database = database

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                cursorclass=pymysql.cursors.DictCursor)
            print(f"Успешное подключение к базе данных")
        except Exception as ex:
            print(f"Не удалось подключиться к базе данных: {ex}")
            raise ex

    def prepare_tables(self):
        with self.connection.cursor() as cursor:
            # создаст таблицу Institutes, если её не существует
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS Institutes (
                institute_id INT UNIQUE PRIMARY KEY,
                institute_name VARCHAR(255) NOT NULL,
                institute_num INT NOT NULL);""")
            print("Приведена таблица Institutes")

            # создаст таблицу Courses, если её не существует
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS Courses (
                course_id INT UNIQUE PRIMARY KEY,
                course_name VARCHAR(16) NOT NULL);""")
            print("Приведена таблица Courses")

            # создаст таблицу StudentGroups, если её не существует
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS StudentGroups (
                group_id INT UNIQUE,
                group_name VARCHAR(16) NOT NULL,
                institute INT,
                course INT,
                PRIMARY KEY (group_id),
                FOREIGN KEY (institute) REFERENCES Institutes (institute_id),
                FOREIGN KEY (course) REFERENCES Courses (course_id));""")
            print("Приведена таблица StudentGroups")

            # создаст таблицу Students, если её не существует
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS Students (
                student_id INT UNIQUE,
                student_name VARCHAR(255) NOT NULL,
                student_group INT,
                leader BOOLEAN NOT NULL,
                PRIMARY KEY (student_id),
                FOREIGN KEY (student_group) REFERENCES StudentGroups (group_id));""")
            print("Приведена таблица Students")

            # создаст таблицу Students_tmp, если её не существует
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS Students (
                student_id INT UNIQUE,
                student_name VARCHAR(255) NOT NULL,
                student_group INT,
                leader BOOLEAN NOT NULL,
                PRIMARY KEY (student_id),
                FOREIGN KEY (student_group) REFERENCES StudentGroups (group_id));""")
            print("Приведена таблица Students")

    def save_data(self, chunk_data):
        with self.connection.cursor() as cursor:
            cursor.execute(f"""REPLACE INTO Courses (course_id, course_name) VALUES (%s, %s);""",
                           (chunk_data['course'][0], chunk_data['course'][1]))
            cursor.execute(f"""REPLACE INTO Institutes (institute_id, institute_name, institute_num) VALUES (%s, %s, %s);""",
                           (chunk_data['institute'][0], chunk_data['institute'][1], chunk_data['institute_num']))
            self.connection.commit()
            # cursor.execute(f"""REPLACE INTO StudentGroups (group_id, group_name, institute, course)
            #                     VALUES ({chunk_data['group'][0]}, {chunk_data['group'][1]}, {chunk_data['institute'][0]}, {chunk_data['course'][0]});""")
            # self.connection.commit()

    def close(self):
        self.connection.close()

