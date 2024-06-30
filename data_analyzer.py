import json
from datetime import date


class DataAnalyzer(object):

    def save_differences(self, difference):
        file_name = f"table_differences_{date.today()}.json"
        file_path = f"differences files/{file_name}"
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(difference, file, ensure_ascii=False)
        print(f"difference объект сохранён как {file_name}")

    def get_report(self):
        ...
