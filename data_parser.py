import datetime
import requests
from bs4 import BeautifulSoup


class StudentsParser(object):
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
        print(f"{datetime.datetime.now().time()} > Начало парсинга студентов")
        try:
            # перебираем институты
            for institute in list(self.institutes.keys()):
                url = f"https://old.kai.ru/info/students/brs.php?p_fac={self.institutes[institute][0]}"
                page = requests.get(url, timeout=10)
                soup = BeautifulSoup(page.content, 'lxml')
                # достаём курсы этого института
                tags = soup.find("select", {"name": "p_kurs"}).find_all("option")
                self.courses = [(tag["value"], tag.text) for tag in tags]

                # перебираем все курсы института
                for course in self.courses:
                    url = f"https://old.kai.ru/info/students/brs.php?p_fac={self.institutes[institute][0]}&p_kurs={course[0]}"
                    page = requests.get(url, timeout=10)
                    soup = BeautifulSoup(page.content, 'lxml')
                    # достаём группы этого курса, этого института
                    tags = soup.find("select", {"name": "p_group"}).find_all("option")
                    self.groups = [(tag["value"], tag.text) for tag in tags]
                    # перебираем все группы, этого курса, этого института
                    for group in self.groups:
                        url = f"https://old.kai.ru/info/students/brs.php?p_fac={self.institutes[institute][0]}&p_kurs={course[0]}&p_group={group[0]}"
                        page = requests.get(url, timeout=10)
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
        except requests.exceptions.ConnectTimeout:
            print(f"{datetime.datetime.now().time()} > requests.exceptions.ConnectTimeout")
            print(f"{datetime.datetime.now().time()} > конец парсинга студентов.")
            yield "exception"
            # raise ex
        except Exception as ex:
            print(f"{datetime.datetime.now().time()} > {ex}")
            print(f"{datetime.datetime.now().time()} > конец парсинга студентов.")
            yield "exception"
            # raise ex

        print(f"{datetime.datetime.now().time()} > конец парсинга студентов.")


class LeadersParser(object):

    def __init__(self, groups):
        self.groups = groups

    def _get_groups_data(self):
        groups_data = list()
        for digit in range(1, 11):
            url = f'https://kai.ru/raspisanie?p_p_id=pubStudentSchedule_WAR_publicStudentSchedule10&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=getGroupsURL&p_p_cacheability=cacheLevelPage&p_p_col_id=column-1&p_p_col_count=1&query={digit}'
            groups_data = groups_data + requests.get(url, timeout=10).json()
        return groups_data

    def _get_leader_from_group(self, groups_data):
        leaders_data = list()
        for group_data in groups_data:
            if group_data['group'] not in self.groups:
                continue
            url = f"https://kai.ru/infoClick/-/info/group?id={group_data['id']}&name={group_data['group']}"
            page = requests.get(url, timeout=10)
            soup = BeautifulSoup(page.content, 'lxml')
            try:
                # Получаем строку с элементом, содержащим старосту
                row_with_leader = soup.select_one("tbody tr td:has(span.label-info)")

                # Если найдена строка, выводим ее
                if row_with_leader:
                    leader_name_td = soup.find('span', class_='label label-info').find_parent('td')
                    student_order = leader_name_td.find_parent('tr').find('td').text
                    student_name = leader_name_td.contents[0].strip()
                    leaders_data.append({
                        "student_name": student_name,
                        "group_name": group_data['group'],
                    })
                    print(f"{student_order} {student_name} в {group_data['group']} ~ {group_data['id']}")
            except:
                print(f"Не удалось получить старосту в {group_data['group']} ~ {group_data['id']}")
        return leaders_data

    def parse_leaders(self):
        print(f"{datetime.datetime.now().time()} > Начало парсинга старост")
        try:
            groups_dict = self._get_groups_data()
            leaders = self._get_leader_from_group(groups_data=groups_dict)

            for leader in leaders:
                yield leader
        except requests.exceptions.ConnectTimeout:
            print(f"{datetime.datetime.now().time()} > requests.exceptions.ConnectTimeout")
            print(f"{datetime.datetime.now().time()} > конец парсинга старост.")
            yield "exception"
            # raise ex
        except Exception as ex:
            print(f"{datetime.datetime.now().time()} > {ex}")
            print(f"{datetime.datetime.now().time()} > конец парсинга старост.")
            yield "exception"
            # raise ex

        print(f"{datetime.datetime.now().time()} > конец парсинга старост.")
