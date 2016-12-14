--
--
/******************************************************************************
 * Description: To empty or truncate a sys audit table  
 *              
 *              NOTE: Run script as schema owner
 *
 ******************************************************************************/


-- purge system audit events
TRUNCATE TABLE SYSTEM_AUDIT_LOG_PARAMETERS;
ALTER TABLE SYSTEM_AUDIT_LOG_PARAMETERS disable constraints SYALP_SYAL_FK;
TRUNCATE TABLE SYSTEM_AUDIT_LOGS;
ALTER TABLE SYSTEM_AUDIT_LOG_PARAMETERS enable constraints SYALP_SYAL_FK;

 
--
--

