-- move_data.sql
-- Upsert from ACCOUNT_RAW into OMEGA and write simple audit (optional)

USE DATABASE IDENTIFIER(CONCAT(CURRENT_DATABASE())) ; -- ensures we use the repo-provided DB when running
USE SCHEMA IDENTIFIER(CONCAT(CURRENT_SCHEMA())) ;

-- MERGE source ACCOUNT_RAW into target OMEGA
MERGE INTO OMEGA tgt
USING (
  SELECT
    ACCOUNT_ID,
    NAME,
    BALANCE,
    UPDATED_AT,
    MD5( NVL(TO_VARCHAR(NAME), '') || '|' || NVL(TO_VARCHAR(BALANCE), '') || '|' || NVL(TO_VARCHAR(UPDATED_AT), '') ) AS data_hash
  FROM ACCOUNT_RAW
) src
ON tgt.ACCOUNT_ID = src.ACCOUNT_ID
WHEN MATCHED AND tgt.data_hash IS DISTINCT FROM src.data_hash THEN
  UPDATE SET
    NAME = src.NAME,
    BALANCE = src.BALANCE,
    UPDATED_AT = src.UPDATED_AT,
    data_hash = src.data_hash
WHEN NOT MATCHED THEN
  INSERT (ACCOUNT_ID, NAME, BALANCE, UPDATED_AT, data_hash)
  VALUES (src.ACCOUNT_ID, src.NAME, src.BALANCE, src.UPDATED_AT, src.data_hash);

-- Optional: create simple audit table if not exists and insert changed rows (keeps small change history)
CREATE TABLE IF NOT EXISTS OMEGA_AUDIT (
  AUDIT_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  ACCOUNT_ID NUMBER,
  ACTION VARCHAR,
  NAME VARCHAR,
  BALANCE NUMBER(18,2),
  UPDATED_AT TIMESTAMP_NTZ
);

-- Insert audit rows (UPSERTed rows)
INSERT INTO OMEGA_AUDIT (ACCOUNT_ID, ACTION, NAME, BALANCE, UPDATED_AT)
SELECT src.ACCOUNT_ID,
       CASE WHEN tgt.ACCOUNT_ID IS NULL THEN 'INSERT' ELSE 'UPDATE' END,
       src.NAME, src.BALANCE, src.UPDATED_AT
FROM (
  SELECT
    ACCOUNT_ID,
    NAME,
    BALANCE,
    UPDATED_AT,
    MD5( NVL(TO_VARCHAR(NAME), '') || '|' || NVL(TO_VARCHAR(BALANCE), '') || '|' || NVL(TO_VARCHAR(UPDATED_AT), '') ) AS data_hash
  FROM ACCOUNT_RAW
) src
LEFT JOIN OMEGA tgt
  ON src.ACCOUNT_ID = tgt.ACCOUNT_ID
WHERE tgt.ACCOUNT_ID IS NULL OR tgt.data_hash IS DISTINCT FROM src.data_hash;

