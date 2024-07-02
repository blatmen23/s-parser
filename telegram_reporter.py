import requests


class TelegramReporter(object):
    def __init__(self, tg_token, chat_id):
        self.tg_token = tg_token
        self.chat_id = chat_id

    def send_document(self, file_path, caption):
        try:
            file = {'document': open(file_path, 'rb')}
            data = {'chat_id': self.chat_id,
                    'caption': caption}
            url = f'https://api.telegram.org/bot{self.tg_token}/sendDocument'
            response = requests.post(url, files=file, data=data)
            if response:
                print(f"{file_path} отправлен в телеграм")
        except Exception as ex:
            print(f"Не удалось отправить {file_path}: {ex}")

    def send_error_message(self, text):
        try:
            data = {'chat_id': self.chat_id,
                    'text': text}
            url = f'https://api.telegram.org/bot{self.tg_token}/sendMessage'
            response = requests.post(url, data)
            if response:
                print(f"Сообщение об ошибке отправлено в телеграм")
        except Exception as ex:
            print(f"Не удалось отправить сообщение об ошибке: {ex}")
