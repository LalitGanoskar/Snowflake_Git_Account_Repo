--Create or replace the task
CREATE OR REPLACE TASK SNOW_GIT_DB.SNOW_GIT_SCHEMA.refresh_account_view_task
  WAREHOUSE = COMPUTE_WH
AS
CALL refresh_account_view();
