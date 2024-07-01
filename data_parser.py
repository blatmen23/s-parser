import datetime
import requests
from bs4 import BeautifulSoup

class DataParser(object):
    courses = None
    groups = None
    students = None
    leader = False
    institutes = {
        1: (1, "Институт авиации, наземного транспорта и энергетики"),
        2: (2, "Факультет физико-математический"),
        3: (3, "Институт автоматики и электронного приборостроения"),
        4: (4, "Институт компьютерных технологий и защиты информации"),
        5: (5, "Институт радиоэлектроники, фотоники и цифровых технологий"),
        6: (28, "Институт инженерной экономики и предпринимательства")
    }

    def parse_data(self):
        print(f"{datetime.datetime.now().time()} > Начало парсинга")
        # перебираем институты
        for institute in list(self.institutes.keys()):
            url = f"https://old.kai.ru/info/students/brs.php?p_fac={self.institutes[institute][0]}"
            page = requests.get(url)
            soup = BeautifulSoup(page.content, 'lxml')
            # достаём курсы этого института
            tags = soup.find("select", {"name": "p_kurs"}).find_all("option")
            self.courses = [(tag["value"], tag.text) for tag in tags]

            # перебираем все курсы института
            for course in self.courses:
                url = f"https://old.kai.ru/info/students/brs.php?p_fac={self.institutes[institute][0]}&p_kurs={course[0]}"
                page = requests.get(url)
                soup = BeautifulSoup(page.content, 'lxml')
                # достаём группы этого курса, этого института
                tags = soup.find("select", {"name": "p_group"}).find_all("option")
                self.groups = [(tag["value"], tag.text) for tag in tags]
                # перебираем все группы, этого курса, этого института
                for group in self.groups:
                    url = f"https://old.kai.ru/info/students/brs.php?p_fac={self.institutes[institute][0]}&p_kurs={course[0]}&p_group={group[0]}"
                    page = requests.get(url)
                    soup = BeautifulSoup(page.content, 'lxml')
                    # достаём студентов этой группы, этого курса, этого института
                    tags = soup.find("select", {"name": "p_stud"}).find_all("option")
                    self.students = [(tag["value"], tag.text) for tag in tags]
                    for student in self.students:
                        if student[0] == "" or student[1] == "":
                            continue
                        yield {'institute': self.institutes[institute],
                               'institute_num': institute,
                               'course': course,
                               'group': group,
                               'student': student,
                               'leader': self.leader}
        print(f"{datetime.datetime.now().time()} > конец парсинга.")

    # def parse_elders(self):
    #     for id in range(0, 30000):
    #         url = f"https://kai.ru/infoClick/-/info/group?id={id}"
    #         page = requests.get(url)
    #         soup = BeautifulSoup(page.content, 'lxml')
    #         if soup.find("div", {"class": "alert-info"}):
    #             print(str(id) + " alert-info")
    #             continue
    #
    #         students_row = soup.find("tbody").find_all("tr")
    #         for student_row in students_row:
    #             student_columns = student_row.find_all("td")
    #             student_name_column = student_columns[1]
    #             if student_name_column.find("span"):
    #                 print(id, " ".join(student_name_column.text.split()[:-1:]))
