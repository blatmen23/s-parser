import requests
from bs4 import BeautifulSoup

from config import *


class DataParser(object):
    def __init__(self):
        self.institutes = {
            1: (1, "Институт авиации, наземного транспорта и энергетики"),
            2: (2, "Факультет физико-математический"),
            3: (3, "Институт автоматики и электронного приборостроения"),
            4: (4, "Институт компьютерных технологий и защиты информации"),
            5: (5, "Институт радиоэлектроники, фотоники и цифровых технологий"),
            6: (28, "Институт инженерной экономики и предпринимательства")
        }
        self.courses = None
        self.groups = None
        self.students = None

    def parse_data(self):
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
                        # print(self.institutes[institute][1], course[1], group[1], student[1])
