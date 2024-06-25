import pymysql.cursors

class DatabaseManager(object):
    def __init__(self, host: str, port: int, user: str, password:str, database: str):
        self.host = host
        self.user = user
        self.port = port
        self.password = password
        self.database = database
        self.connection = None

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

    def create_tables(self):
        with self.connection.cursor() as cursor:
            # создаст таблицу institutes, если её не существует
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS institutes (
                institute_id INT PRIMARY KEY,
                institute_name VARCHAR(255) NOT NULL,
                institute_num INT NOT NULL);""")
            print("Обновлена таблица institutes")

            # создаст таблицу courses, если её не существует
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS courses (
                course_id INT PRIMARY KEY,
                course_name INT NOT NULL);""")
            print("Обновлена таблица courses")

            # создаст таблицу groups, если её не существует
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS students_groups (
                group_id INT PRIMARY KEY,
                group_name INT NOT NULL,
                course_id INT NOT NULL,
                institute_id INT NOT NULL,
                FOREIGN KEY(course_id) REFERENCES courses(course_id), 
                FOREIGN KEY(institute_id) REFERENCES institutes(institute_id));""")
            print("Обновлена таблица groups")

            # создаст таблицу students, если её не существует
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS students (
                student_id INT PRIMARY KEY,
                student_name VARCHAR(255) NOT NULL,
                group_id INT NOT NULL,
                course_id INT NOT NULL,
                institute_id INT NOT NULL,
                FOREIGN KEY(group_id) REFERENCES students_groups(group_id),
                FOREIGN KEY(course_id) REFERENCES courses(course_id), 
                FOREIGN KEY(institute_id) REFERENCES institutes(institute_id));""")
            print("Обновлена таблица students")

            # создаст таблицу students_tmp, если её не существует
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS students_tmp LIKE s_parser.students;""")
            print("Обновлена таблица students_tmp")


    def close(self):
        self.connection.close()

