import requests

class TelegramReporter(object):
    def __init__(self, tg_token, chat_id):
        self.tg_token = tg_token
        self.chat_id = chat_id

    def send_document(self, file_path, caption):
        file = {'document': open(file_path, 'rb')}
        data = {'chat_id': self.chat_id,
                'caption': caption}
        url = f'https://api.telegram.org/bot{self.tg_token}/sendPhoto'
        response = requests.post(url, files=file, data=data)


