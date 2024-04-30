import os
import time

import requests
import ydb

from db import driver_config, select_tokens, set_tokens
from hh_logging import logger

BASE_URL = 'https://api.hh.ru'
ENV_USER_AGENT = os.getenv('ENV_USER_AGENT')
ENV_CLIENT_ID = os.getenv('ENV_CLIENT_ID')
ENV_CLIENT_SECRET = os.getenv('ENV_CLIENT_SECRET')

headers = {
    'User-Agent': ENV_USER_AGENT,
}

params = {
    'grant_type': 'refresh_token',
    'refresh_token': '',
    'client_id': ENV_CLIENT_ID,
    'client_secret': ENV_CLIENT_SECRET
}


def main(event, context):
    with ydb.Driver(driver_config) as driver:
        try:
            driver.wait(timeout=15)
            session = driver.table_client.session().create()

            tokens = select_tokens(session)
            headers['Authorization'] = 'Bearer ' + tokens['access_token']

            response = requests.get(f'{BASE_URL}/resumes/mine', headers=headers)
            if response.status_code == 403:
                del headers['Authorization']
                params['refresh_token'] = tokens['refresh_token']

                response = requests.post(f'{BASE_URL}/token', headers=headers, params=params)
                if response.status_code != 200:
                    logger.error(f'Не удалось обновить токен с кодом {response.status_code}')
                    logger.error(response.text)
                    exit(1)
                new_tokens = response.json()
                print(new_tokens)

                expires = int(time.time() + int(new_tokens['expires_in']))
                set_tokens(session, new_tokens['access_token'], new_tokens['refresh_token'], expires)

                tokens = select_tokens(session)
                headers['Authorization'] = 'Bearer ' + tokens['access_token']
            elif response.status_code != 200:
                logger.error(f'Неизвестная ошибка с кодом {response.status_code}')
                logger.error(response.text)
                exit(1)
            response = requests.get(f'{BASE_URL}/resumes/mine', headers=headers)
            print(f'Answer code {response.status_code}')

            logger.info("My log message", extra={"my-key": "my-value"})
        except TimeoutError:
            print("Connect failed to YDB")
            print("Last reported errors by discovery:")
            print(driver.discovery_debug_details())
            exit(1)

    return 'Ok'
