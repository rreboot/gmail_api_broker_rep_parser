import base64
from datetime import datetime
import os
import pickle
from pathlib import Path
import pdb
import re

from lxml import html
from apiclient import errors
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd

from structures import PortfolioTableRecord

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
BASE_PATH = os.path.abspath(os.path.dirname(__file__))
ATT_DIR = Path(BASE_PATH, 'attachments')


def get_service():
    """Получение доступа к GmailAPI
    Создает файл или получает данные из файла token.pickle с токеном
    доступа.
    При первом запуске открывает окно с запросом доступа к аккаунту.
    """
    creds = None

    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('gmail', 'v1', credentials=creds)


def messages_list(service, user_id, query=''):
    """Получение списка сообщений из почтового ящика.

    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      query: String used to filter messages returned.
      Eg.- 'from:user@some_domain.com' for Messages from a particular sender.

    Returns:
      List of Messages that match the criteria of the query. Note that the
      returned list contains Message IDs, you must use get with the
      appropriate ID to get the details of a Message.
    """
    try:
        response = service.users().messages().list(userId=user_id,
                                                   q=query).execute()
        messages = []
        if 'messages' in response:
            messages.extend(response['messages'])

        while 'nextPageToken' in response:
            page_token = response['nextPageToken']
            response = service.users().messages().list(
                userId=user_id, q=query, pageToken=page_token).execute()
            messages.extend(response['messages'])

        return messages
    except errors.HttpError as error:
        print(f'An error occurred: {error}')


def html_attachment_id(service, user_id, msg_id):
    """Get list of html attachment ids from Message.

    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      msg_id: ID of Message containing attachment.
    """

    try:
        message = service.users().messages().get(
            userId=user_id, id=msg_id).execute()

        for part in message['payload']['parts']:
            if 'html' in part['filename'].lower():

                return part['filename'], part['body']['attachmentId']

    except errors.HttpError as error:
        print(f'An error occurred: {error}')


def save_attachments():
    """Сохранение вложений в папку ATT_DIR"""
    service = get_service()

    if not ATT_DIR.exists():
        os.mkdir(ATT_DIR)

    broker_msg_list = messages_list(service, 'me',
                                    query='from:broker_rep@sberbank.ru')
    for msg in broker_msg_list:
        msg_id = msg['id']
        msg_attachment = html_attachment_id(service, 'me', msg_id)
        attachment_name = msg_attachment[0]
        attachment_id = msg_attachment[1]

        if Path(ATT_DIR, attachment_name).exists():
            print(f'attachment {attachment_name} exists')
            continue

        attachment = service.users().messages().attachments().get(
            id=attachment_id, userId='me', messageId=msg_id)
        data = attachment.execute()['data']
        with open(ATT_DIR / attachment_name, 'wb') as f:
            print(f'attachment {attachment_name} saved')
            f.write(base64.urlsafe_b64decode(data))


def parse_attachments():
    att_list = [str(x) for x in ATT_DIR.glob('*.html')]
    glob_portfolio_table = []

    for file in att_list:
        with open(file, 'r', errors='ignore') as f:
            content = f.read()
        content = content[:content.find('</html>') + 7]
        content = content.replace('<br>', r'\n')

        tree = html.fromstring(content)

        header = tree.xpath('//h3/text()')[0]
        period = re.findall(r'(\d+\.\d+\.\d+) по (\d+\.\d+\.\d+)', header)[0]
        period_from = period[0]
        period_to = period[1]

        # Парсинг таблицы Портфель ценных бумаг
        h_portfolio = tree.xpath(
            '//p[contains(text(), "Портфель Ценных Бумаг")]')
        if h_portfolio:
            portfolio_table = h_portfolio[0].getnext()
            rows = portfolio_table.xpath(
                './/tr[not(@bgcolor) and count(td) > 1]')

            for row in rows:
                cells = row.xpath('.//td/text()') + [period_from, period_to]
                record = PortfolioTableRecord(*cells)
                glob_portfolio_table.append(record.toTuple())

    df = pd.DataFrame(glob_portfolio_table)
    print(df)

# Примерный алгоритм:
# Получение вложения
# Очистка содержимого
# Парсинг таблицы
# Сохранение таблицы в БД?


if __name__ == '__main__':
    # save_attachments()
    parse_attachments()
