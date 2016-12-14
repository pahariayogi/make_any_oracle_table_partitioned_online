--
--
/******************************************************************************
 * Description: This script grants the permissions required for deploying
 *              partitioning
 *              NOTE: Run script as 'SYS AS SYSDBA'.
 *
 ******************************************************************************/

ACCEPT SCHEMA_NAME DEFAULT 'AUCORE' PROMPT 'ENTER SCHEMA NAME :'

revoke SELECT on DBA_REDEFINITION_ERRORS from &SCHEMA_NAME;

revoke EXECUTE on DBMS_REDEFINITION from &SCHEMA_NAME;

--
--

