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
        'expires': row.expires,
    }


def set_tokens(session, access_token, refresh_token, expires):
    query = f"""
        REPLACE INTO token_table (id, access_token, refresh_token, expires)
        VALUES
            (
            1,
            "{access_token}",
            "{refresh_token}",
            CAST("{expires}" AS Datetime)
            );
        """
    print(query)
    session.transaction(ydb.SerializableReadWrite()).execute(
        query,
        commit_tx=True,
    )
