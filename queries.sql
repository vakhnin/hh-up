DELETE
FROM resumes_table
WHERE
  resume_id = <resume_id>;

REPLACE INTO token_table (id, access_token, refresh_token, expires)
VALUES
  (
  1,
  "access_token1",
  "refresh_token1",
  CAST("2024-04-14T12:00:00Z" AS Datetime)
  );
