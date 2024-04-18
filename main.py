import ydb

from db import select_resumes, driver_config
from hh_logging import logger


def main(event, context):
    with ydb.Driver(driver_config) as driver:
        try:
            driver.wait(timeout=15)
            session = driver.table_client.session().create()
            resumes = select_resumes(session)
            print(resumes)
            logger.info("My log message", extra={"my-key": "my-value"})
        except TimeoutError:
            print("Connect failed to YDB")
            print("Last reported errors by discovery:")
            print(driver.discovery_debug_details())
            exit(1)

    return 'Ok'
