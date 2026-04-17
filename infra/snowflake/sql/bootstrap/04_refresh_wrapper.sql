-- Create the public Snowflake refresh wrapper and triggered manifest-processing task.
--
-- Required session variables:
--   set database_name = 'EDGARTOOLS_DEV';
--   set source_schema_name = 'EDGARTOOLS_SOURCE';
--   set gold_schema_name = 'EDGARTOOLS_GOLD';
--   set deployer_role_name = 'EDGARTOOLS_DEV_DEPLOYER';
--   set refresh_warehouse_name = 'EDGARTOOLS_DEV_REFRESH_WH';
--   set manifest_stream_name = 'SNOWFLAKE_RUN_MANIFEST_STREAM';
--   set source_load_procedure_name = 'LOAD_EXPORTS_FOR_RUN';
--   set refresh_procedure_name = 'REFRESH_AFTER_LOAD';
--   set stream_processor_procedure_name = 'PROCESS_RUN_MANIFEST_STREAM';
--   set manifest_task_name = 'SNOWFLAKE_RUN_MANIFEST_TASK';

USE ROLE IDENTIFIER($deployer_role_name);
USE DATABASE IDENTIFIER($database_name);
USE SCHEMA IDENTIFIER($gold_schema_name);

CREATE OR REPLACE PROCEDURE IDENTIFIER($refresh_procedure_name)(workflow_name STRING, run_id STRING)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  UPDATE EDGARTOOLS_SOURCE.SNOWFLAKE_REFRESH_STATUS
  SET
    refresh_status = 'succeeded',
    status = 'succeeded',
    error_message = NULL,
    last_successful_refresh_at = CURRENT_TIMESTAMP(),
    updated_at = CURRENT_TIMESTAMP()
  WHERE source_workflow = :workflow_name
    AND run_id = :run_id
    AND source_load_status = 'succeeded';

  RETURN OBJECT_CONSTRUCT(
    'status', 'succeeded',
    'workflow_name', :workflow_name,
    'run_id', :run_id
  );
END;
$$;

CREATE OR REPLACE PROCEDURE IDENTIFIER($stream_processor_procedure_name)()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  FOR manifest_record IN (
    SELECT DISTINCT workflow_name, run_id
    FROM EDGARTOOLS_SOURCE.SNOWFLAKE_RUN_MANIFEST_STREAM
    WHERE METADATA$ACTION = 'INSERT'
  ) DO
    CALL EDGARTOOLS_SOURCE.LOAD_EXPORTS_FOR_RUN(manifest_record.workflow_name, manifest_record.run_id);
    CALL EDGARTOOLS_GOLD.REFRESH_AFTER_LOAD(manifest_record.workflow_name, manifest_record.run_id);
  END FOR;

  RETURN OBJECT_CONSTRUCT('status', 'succeeded');
END;
$$;

BEGIN
  EXECUTE IMMEDIATE
    'CREATE OR REPLACE TASK ' || $manifest_task_name || '
       WAREHOUSE = ' || $refresh_warehouse_name || '
       WHEN SYSTEM$STREAM_HAS_DATA(''' || $database_name || '.' || $source_schema_name || '.' || $manifest_stream_name || ''')
       AS
       CALL ' || $database_name || '.' || $gold_schema_name || '.' || $stream_processor_procedure_name || '()';
END;

BEGIN
  EXECUTE IMMEDIATE
    'ALTER TASK ' || $manifest_task_name || ' RESUME';
END;
