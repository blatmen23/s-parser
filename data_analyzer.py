import json
from datetime import date


class DataAnalyzer(object):

    def _save_report_json(self, difference: dict):
        file_name = f"report_{date.today()}.json"
        file_path = f"reports/json/{file_name}"
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(difference, file, ensure_ascii=False)
        print(f"difference объект сохранён как {file_name}")
        return file_path

    def _save_report_txt(self, report: str):
        file_name = f"report_{date.today()}.txt"
        file_path = f"reports/txt/{file_name}"
        with open(file_path, "w", encoding="utf-8") as file:
            file.write(report)
        print(f"report объект сохранён как {file_name}")
        return file_path

    def _get_time_difference(self, start_time: int, end_time: int):
        time_difference = end_time - start_time
        hours = str(time_difference // (60*60)).rjust(2, '0')
        minutes = str(time_difference // 60).zfill(2)
        seconds = str(time_difference % 60).zfill(2)
        return f"{hours}:{minutes}:{seconds}"

    def get_report_txt(self, differences: dict, start_time: int, end_time: int, total_students: int, total_groups: int):
        all_new_groups = "\n".join([str(group["group_name"]) for group in differences["new_groups"]])
        all_deleted_groups = "\n".join([str(group["group_name"]) for group in differences["deleted_groups"]])

        all_entered_students = "\n".join([str(student["student_name"]) + " - " + str(student["group_name"]) for student in differences["entered_students"]])
        all_left_students = "\n".join([str(student["student_name"]) + " - " + str(student["group_name"]) for student in differences["left_students"]])

        all_leaders_status_changes = "\n".join([str(student["student_name"] + " - " + str(student["group_name"]) + " - " + ("повышение" if str(student["status"]) == "promotion" else ("понижение" if str(student["status"]) == "demotion" else ""))) for student in differences["leader_status"]])
        all_group_changes = "\n".join([str(student["student_name"] + " - " + str(student["last_group_name"])) + " -> " + str(student["new_group_name"]) for student in differences["group_changes"]])

        report_content = (f'База студентов обновлена: {date.today()}\n'
                          f'Затраченное время: {self._get_time_difference(start_time, end_time)}\n'
                          f'Найдено групп: {total_groups}\n'
                          f'Новые группы: {len(differences["new_groups"])}\n'
                          f'{all_new_groups}\n\n'
                          f'Не найденные группы: {len(differences["deleted_groups"])}\n'
                          f'{all_deleted_groups}\n\n'
                          f'Найдено студентов: {total_students}\n'
                          f'Новые студенты: {len(differences["entered_students"])}\n'
                          f'{all_entered_students}\n\n'
                          f'Не найденные студенты: {len(differences["left_students"])}\n'
                          f'{all_left_students}\n\n'
                          f'Изменения статусов старост: {len(differences["leader_status"])}\n'
                          f'{all_leaders_status_changes}\n\n'
                          f'Студенты изменившие группу: {len(differences["group_changes"])}\n'
                          f'{all_group_changes}\n\n')

        # differences_file_path = self._save_report_json(differences)
        report_txt_file_path = self._save_report_txt(report_content)
        return report_txt_file_path

    def get_report_json(self, differences: dict, start_time: int, end_time: int, total_students: int, total_groups: int):
        report_content = dict(differences)
        report_content["today"] = str(date.today())
        report_content["time_difference"] = self._get_time_difference(start_time, end_time)
        report_content["total_groups"] = total_groups
        report_content["total_new_groups"] = len(differences["new_groups"])
        report_content["total_deleted_groups"] = len(differences["deleted_groups"])
        report_content["total_students"] = total_students
        report_content["total_new_students"] = len(differences["entered_students"])
        report_content["total_deleted_students"] = len(differences["left_students"])
        report_content["total_leader_status"] = len(differences["leader_status"])
        report_content["total_group_changes"] = len(differences["group_changes"])

        report_txt_file_path = self._save_report_json(report_content)
        return report_txt_file_path

