from datetime import datetime, timedelta

import ydb

from db import driver_config, select_tokens, set_tokens
from hh_logging import logger


def main(event, context):
    with ydb.Driver(driver_config) as driver:
        try:
            driver.wait(timeout=15)
            session = driver.table_client.session().create()
            # resumes = select_resumes(session)

            expires = datetime.now() + timedelta(seconds=int(1209599))
            set_tokens(session, 'access_token1', 'refresh_token1',
                       expires.replace(microsecond=0).isoformat() + 'Z')
            tokens = select_tokens(session)
            print(tokens)
            logger.info("My log message", extra={"my-key": "my-value"})
        except TimeoutError:
            print("Connect failed to YDB")
            print("Last reported errors by discovery:")
            print(driver.discovery_debug_details())
            exit(1)

    return 'Ok'
