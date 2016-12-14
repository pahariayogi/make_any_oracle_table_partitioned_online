--
--
/******************************************************************************
 * Description: This script grants the permissions required for deploying
 *              partitioning
 *              NOTE: Run script as 'SYS AS SYSDBA'.
 *
 ******************************************************************************/

ACCEPT SCHEMA_NAME DEFAULT 'AUCORE' PROMPT 'ENTER SCHEMA NAME :'

grant CREATE ANY TABLE, ALTER ANY TABLE, DROP ANY TABLE, LOCK ANY TABLE, SELECT ANY TABLE, CREATE ANY TRIGGER, CREATE ANY INDEX to &SCHEMA_NAME;

grant SELECT on DBA_REDEFINITION_ERRORS to &SCHEMA_NAME;

grant EXECUTE on DBMS_REDEFINITION to &SCHEMA_NAME;

--
--

