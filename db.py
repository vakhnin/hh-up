import os
from datetime import datetime

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


def select_tokens(session):
    result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
        """
        SELECT
            access_token,
            refresh_token,
            expires
        FROM token_table
        WHERE id = 1;
        """,
        commit_tx=True,
    )

    row = result_sets[0].rows[0]
    return {
        'access_token': row.access_token,
        'refresh_token': row.refresh_token,
        'expires': datetime.fromtimestamp(row.expires).strftime("%m/%d/%Y, %H:%M:%S"),
    }


def set_tokens(session, access_token, refresh_token, expires):
    query = f"""
        DECLARE $access_token AS Utf8;
        DECLARE $refresh_token AS Utf8;
        DECLARE $expires AS DateTime;
        REPLACE INTO token_table (id, access_token, refresh_token, expires)
        VALUES
            (
            1,
            $access_token,
            $refresh_token,
            $expires
            );
        """
    prepared_query = session.prepare(query)
    session.transaction(ydb.SerializableReadWrite()).execute(
        prepared_query,
        {
            '$access_token': access_token,
            '$refresh_token': refresh_token,
            '$expires': expires,
        },
        commit_tx=True,
    )
