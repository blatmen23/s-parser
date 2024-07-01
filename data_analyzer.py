import json
from datetime import date


class DataAnalyzer(object):

    def _save_differences(self, difference: dict):
        file_name = f"table_differences_{date.today()}.json"
        file_path = f"differences files/{file_name}"
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(difference, file, ensure_ascii=False)
        print(f"difference объект сохранён как {file_name}")

    def _save_report(self, report: str):
        file_name = f"report_{date.today()}.txt"
        file_path = f"reports/{file_name}"
        with open(file_path, "w", encoding="utf-8") as file:
            file.write(report)
        print(f"report объект сохранён как {file_name}")

    def _get_time_difference(self, start_time: int, end_time: int):
        time_difference = end_time - start_time
        hours = str(time_difference // (60*60)).rjust(2, '0')
        minutes = str(time_difference // 60).zfill(2)
        seconds = str(time_difference % 60).zfill(2)
        return f"{hours}:{minutes}:{seconds}"

    def get_report(self, differences: dict, start_time: int, end_time: int, total_students: int, total_groups: int):
        self._save_differences(differences)

        all_new_groups = "\n".join([str(group["group_id"]) + " - " + str(group["group_name"]) for group in differences["new_groups"]])
        all_deleted_groups = "\n".join([str(group["group_id"]) + " - " + str(group["group_name"]) for group in differences["deleted_groups"]])

        all_entered_students = "\n".join([str(student["student_id"]) + " - " + str(student["student_name"]) for student in differences["entered_students"]])
        all_left_students = "\n".join([str(student["student_id"]) + " - " + str(student["student_name"]) for student in differences["left_students"]])

        all_leaders_status_changes = "\n".join([str(student["student_id"]) + " - " + str(student["student_name"] + " - " + str(student["status"])) for student in differences["leader_status"]])
        all_group_changes = "\n".join([str(student["student_id"]) + " - " + str(student["student_name"] + " - " + str(student["last_group_name"])) + " -> " + str(student["new_group_name"]) for student in differences["group_change"]])

        report_content = (f'База студентов обновлена: {date.today()}\n'
                          f'Затраченное время: {self._get_time_difference(start_time, end_time)}\n'
                          f'Найдено групп: {total_groups}\n'
                          f'Новые группы: {len(differences["new_groups"])}\n'
                          f'{all_new_groups}\n'
                          f'Не найденные группы: {len(differences["deleted_groups"])}\n'
                          f'{all_deleted_groups}\n'
                          f'Найдено студентов: {total_students}\n'
                          f'Новые студенты: {len(differences["entered_students"])}\n'
                          f'{all_entered_students}\n'
                          f'Не найденные студенты: {len(differences["left_students"])}\n'
                          f'{all_left_students}\n'
                          f'Изменения статусов старост: {len(differences["leader_status"])}\n'
                          f'{all_leaders_status_changes}\n'
                          f'Студенты изменившие группу: {len(differences["group_change"])}\n'
                          f'{all_group_changes}\n')

        self._save_report(report_content)
        print(report_content)
