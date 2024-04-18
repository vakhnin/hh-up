import os

import ydb

driver_config = ydb.DriverConfig(
    os.getenv('ENV_ENDPOINT'),
    os.getenv('ENV_DATABASE'),
    credentials=ydb.credentials_from_env_variables(),
    root_certificates=ydb.load_ydb_root_certificate(),
)


def select_resumes(session):
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        """
        SELECT
            resume_id,
            name,
            describe
        FROM resumes_table;
        """,
        commit_tx=True,
    )

    result = []
    for row in result_sets[0].rows:
        result.append({
            'resume_id': row.resume_id,
            'name': row.name,
            'describe': row.describe,
        })
    return result
