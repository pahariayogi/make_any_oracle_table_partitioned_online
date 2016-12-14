CREATE OR REPLACE PACKAGE BODY PKG_PARTITION_MIGRATION AS
--
--
/******************************************************************************
 * Description: This package contains procedures and functions to add partitioning to a table
 * Author: Ypahariya
 * Features:
 * 			1. Facilitates partitioning of any regular db tables ONLINE using DBMS_REDEFINITION
 * 			2. Supports RANGE PARTITIONING ONLY as of now
 *			3. Takes care of Global Index rebuild post partitioning
 *			
 * Needs bug fixes and further improvement:
 * 			1. To fix vpd policy issue - Fixed
 *			2. Hard coding of columns 'CREATED_DATETIME' to be removed and make it generic for any column - Fixed
 * 			3. To support REFERENCE PARTITION
 *			4. To support Sub-partitions
 *			5. To support other partition types (LIST, INTERVAL etc)
 *			6. To support other partitions schemes in  (like Daily, Weekly, Qtry, Yearly etc)	- Daily, Monthly supported
 *			7. To allow conversion of monthly partitons to daily or vise versa - Supported
 ******************************************************************************/

 PROCEDURE pr_partition_table (
    iv_table_name            VARCHAR2,
    iv_table_name_short      VARCHAR2,
    in_num_of_partitions     NUMBER,
	iv_part_column			 VARCHAR2,
	iv_part_scheme			 VARCHAR2		-- Like 'MONTHLY', 'DAILY'
  ) IS

	lv_table_sql                   varchar2(32767) := NULL;
	lv_partition_name 		varchar2(20)     := NULL;
	lv_high_value      		varchar2(20)    := NULL;
	ln_num_table_check             NUMBER          := NULL;
	ld_current_date                DATE            := NULL;
	ld_temp_date                   DATE            := NULL;
  
	/* CURSOR cur_global_indexes(iv_table_name VARCHAR2) IS
	SELECT index_name
	FROM user_indexes ind, user_part_tables par_tab
	WHERE ind.table_name = par_tab.table_name
	AND par_tab.table_name = iv_table_name
	AND ind.PARTITIONED = 'NO';
	rec_global_indexes   cur_global_indexes%ROWTYPE;
	*/
	TYPE values_t IS TABLE OF VARCHAR2(100);
	cur_unique_values   values_t;
  BEGIN

    ld_current_date := sysdate;
 
    DBMS_OUTPUT.put_line('Create interim table: ' || to_char(sysdate, 'DD-MM-YYYY hh24:Mi:SS'));
    pr_create_temp_part_tables(iv_table_name, iv_table_name_short);

    -- Get DDL for <iv_table_name_short>_T and change name to <iv_table_name_short>_P
    lv_table_sql := replace(dbms_metadata.get_ddl('TABLE',iv_table_name_short || '_TEMP'),'"'||user||'"."'|| iv_table_name_short || '_TEMP"',iv_table_name_short || '_PART' );

	--DBMS_OUTPUT.put_line(lv_table_sql);	--debug

    -- Remove last char ";"
    lv_table_sql := substr(lv_table_sql, 0, length(lv_table_sql) -1);
                                                  
    lv_table_sql := lv_table_sql || ' PARTITION BY RANGE(' || iv_part_column || ') ( ';
    
    -- Create partitions for past dates for existing data. Start from the inception to current date
	CASE 
	WHEN upper(iv_part_scheme) = 'MONTHLY' THEN
	 EXECUTE IMMEDIATE 'SELECT UNIQUE to_char(' || iv_part_column || ',''YYYY-MM'')    unique_month FROM ' || iv_table_name || ' ORDER BY to_char(' || iv_part_column || ',''YYYY-MM'')' BULK COLLECT INTO cur_unique_values;
	WHEN upper(iv_part_scheme) = 'DAILY' THEN
	 EXECUTE IMMEDIATE 'SELECT UNIQUE to_char(' || iv_part_column || ',''DD-MM-YYYY'') unique_days  FROM ' || iv_table_name || ' ORDER BY to_char(' || iv_part_column || ',''DD-MM-YYYY'')' BULK COLLECT INTO cur_unique_values;
	 --EXECUTE IMMEDIATE 'SELECT UNIQUE to_char(' || iv_part_column || ',''DD-MON-YYYY'') unique_days  FROM ' || iv_table_name || ' ORDER BY ' || iv_part_column BULK COLLECT INTO cur_unique_values;
	END CASE;

	FOR indx IN 1 .. cur_unique_values.COUNT
	   LOOP
			CASE 
			WHEN upper(iv_part_scheme) = 'MONTHLY' THEN
			  ld_current_date := to_date(cur_unique_values(indx) || '-01','YYYY-MM-DD');
			  lv_high_value 	:= to_char(add_months( ld_current_date,1),'DD-MON-YYYY');
			  lv_partition_name := to_char(ld_current_date, 'MONYYYY');
			  DBMS_OUTPUT.put_line (ld_current_date || ' , ' || lv_high_value || ' , ' || lv_partition_name);
			WHEN upper(iv_part_scheme) = 'DAILY' THEN
			  ld_current_date := to_date(cur_unique_values(indx),'DD-MM-YYYY');
			  lv_high_value 	:= to_char(ld_current_date+1,'DD-MON-YYYY');
			  lv_partition_name := to_char(ld_current_date, 'DDMONYYYY');
			END CASE;

			  lv_table_sql := lv_table_sql || 'PARTITION "' || iv_table_name_short || lv_partition_name || '" VALUES LESS THAN (TO_DATE('' ' ||lv_high_value|| ' '', ''DD-MON-YYYY''))';
			
			  lv_table_sql := lv_table_sql || ',';
	   END LOOP;
	   DBMS_OUTPUT.put_line (lv_table_sql);

    -- Create partitions for future dates. Start from the current date to (current date + in_num_of_partitions)
	ld_current_date := sysdate;
    FOR i IN 1..in_num_of_partitions LOOP
			CASE 
			WHEN upper(iv_part_scheme) = 'MONTHLY' THEN
			  ld_current_date := '01-' || to_char(add_months(sysdate,i+1),'MON-YYYY');
			  lv_partition_name := to_char(ld_current_date-1, 'MONYYYY');
			  lv_high_value 	:= to_char(ld_current_date, 'DD-MON-YYYY');
			WHEN upper(iv_part_scheme) = 'DAILY' THEN
			  ld_current_date := ld_current_date+1;
			  lv_high_value 	:= to_char(ld_current_date+1,'DD-MON-YYYY');
			  lv_partition_name := to_char(ld_current_date, 'DDMONYYYY');
			  DBMS_OUTPUT.put_line (ld_current_date || ' , ' || lv_high_value || ' , ' || lv_partition_name);
			END CASE;
  
      lv_table_sql := lv_table_sql || 'PARTITION "' || iv_table_name_short || lv_partition_name || '" VALUES LESS THAN (TO_DATE('' ' ||lv_high_value|| ' '', ''DD-MON-YYYY''))';
    
      --need to support 'daily' partition as well
      --ld_current_date := ld_current_date + 1;
      lv_table_sql := lv_table_sql || ',';
    END LOOP;

    -- Add overflow partition to end of list of date specific partitions to create
    lv_table_sql := lv_table_sql || 'PARTITION OVERFLOW VALUES LESS THAN (MAXVALUE))';

    DBMS_OUTPUT.put_line('Create ' || iv_table_name_short || '_PART:' || to_char(sysdate, 'DD-MM-YYYY hh24:Mi:SS'));
	
	DBMS_OUTPUT.put_line(lv_table_sql);	--debug
    EXECUTE IMMEDIATE lv_table_sql;

	pr_copy_vpd_policies(iv_table_name, iv_table_name_short || '_PART');	--disabling policies, if any, on original table and copying it to interim table to get back in the end.

    pr_run_redefinition(iv_table_name, iv_table_name_short);
 
	-- The following step can be performed manually post partitioning
    --pr_drop_temp_part_tables(iv_table_name_short);
    
    -- I dont think we need index rebuild?
    /*DBMS_OUTPUT.put_line('Rebuild index start: ' || to_char(sysdate, 'DD-MM-YYYY hh24:Mi:SS'));
    OPEN cur_global_indexes(iv_table_name);
		  LOOP
		    FETCH cur_global_indexes INTO rec_global_indexes;
		    EXIT WHEN cur_global_indexes%NOTFOUND;
		    		
		      lv_table_sql := 'ALTER INDEX '||rec_global_indexes.index_name||' REBUILD ONLINE';
		      EXECUTE IMMEDIATE lv_table_sql;
    	END LOOP;
    CLOSE cur_global_indexes;
	*/
    DBMS_OUTPUT.put_line('Gather stats start: ' || to_char(sysdate, 'DD-MM-YYYY hh24:Mi:SS'));
	
	DBMS_STATS.GATHER_TABLE_STATS(ownname=>USER,tabname=>iv_table_name,estimate_percent=>100, cascade=>true, degree=>8);
 
    DBMS_OUTPUT.put_line('Finish:' || to_char(sysdate, 'DD-MM-YYYY hh24:Mi:SS'));
 
 END pr_partition_table;

  PROCEDURE pr_create_temp_part_tables(
    iv_table_name VARCHAR2,
	iv_table_name_short VARCHAR2
  ) IS

  lv_table_sql varchar2(32767);
     CURSOR cur_not_null_columns(iv_table_name_short VARCHAR2) IS
      SELECT column_name
      FROM user_tab_columns 
      WHERE table_name = iv_table_name_short
      AND NULLABLE = 'N';
    rec_not_null_columns   cur_not_null_columns%ROWTYPE;

  BEGIN
  
	-- Drop temp tables, in case they already exist 
    pr_drop_temp_part_tables(iv_table_name_short);
  
    DBMS_OUTPUT.put_line('Create ' || iv_table_name_short || '_TEMP');
    lv_table_sql := 'CREATE TABLE ' || iv_table_name_short || '_TEMP AS SELECT * FROM ' || iv_table_name || ' WHERE 1=2';
    EXECUTE IMMEDIATE lv_table_sql;

    --Yogi P: Setting column 'NULL'able, if its set as 'NOT NULL'able in interim table to avoid exception in subsequent step COPY_TABLE_DEPENDENTS
	-- DBMS_REDEFINITION.COPY_TABLE_DEPENDENTS allows to choose copying (or not copying) constraints but does not handle following exception: 
	-- ORA-01442: column to be modified to NOT NULL is already NOT NULL			-- Oracle should enhance future version of DBMS_REDEFINITION to handle this.
	-- Writing generic code to set all columns NULLable in interim table

    --DBMS_OUTPUT.put_line('Drop NOT NLL constraints start : ' || to_char(sysdate, 'DD-MM-YYYY hh24:Mi:SS'));
    OPEN cur_not_null_columns(iv_table_name_short || '_TEMP');
		  LOOP
		    FETCH cur_not_null_columns INTO rec_not_null_columns;
		    EXIT WHEN cur_not_null_columns%NOTFOUND;
		    		
			  --DBMS_OUTPUT.PUT_LINE ('Drop NOT NULL constraint on ' || rec_not_null_columns.column_name);
			  lv_table_sql := 'ALTER TABLE '|| iv_table_name_short || '_TEMP MODIFY ('|| rec_not_null_columns.column_name || ' NULL)';
			  DBMS_OUTPUT.PUT_LINE (lv_table_sql);
		      EXECUTE IMMEDIATE lv_table_sql;
    	END LOOP;
    CLOSE cur_not_null_columns;

  END pr_create_temp_part_tables;
  
  PROCEDURE pr_drop_temp_part_tables(
    iv_table_name VARCHAR2
  ) IS
    lv_table_sql varchar2(32767);
  BEGIN
     BEGIN
      -- Check to remove old table
      lv_table_sql := 'DROP TABLE ' || iv_table_name || '_TEMP CASCADE constraints PURGE';
      EXECUTE IMMEDIATE lv_table_sql;

      DBMS_OUTPUT.put_line('Dropped ' || iv_table_name || '_TEMP');

	  EXCEPTION WHEN OTHERS THEN
      IF SQLCODE = -00942 THEN
        NULL;
      ELSE
		DBMS_OUTPUT.put_line('SQLCODE ' || SQLCODE);
		DBMS_OUTPUT.put_line('SQLERRM ' || SQLERRM);
      END IF;
     END;

     BEGIN
      -- Check to remove MATERIALIZED VIEW, if any, created by oracle internally to manage online reorganisation of table.
      lv_table_sql := 'DROP MATERIALIZED VIEW ' || iv_table_name || '_PART';
      EXECUTE IMMEDIATE lv_table_sql;
      DBMS_OUTPUT.put_line('Dropped MATERIALIZED VIEW ' || iv_table_name || '_PART CASCADE constraints ');

	  EXCEPTION WHEN OTHERS THEN
        NULL;
     END;

	 BEGIN
      -- Check to remove old table
      lv_table_sql := 'DROP TABLE ' || iv_table_name || '_PART CASCADE constraints PURGE';
      EXECUTE IMMEDIATE lv_table_sql;
      DBMS_OUTPUT.put_line('Dropped ' || iv_table_name || '_PART');

	  EXCEPTION WHEN OTHERS THEN
      IF SQLCODE = -00942 THEN
        NULL;
      ELSE
		DBMS_OUTPUT.put_line('SQLCODE ' || SQLCODE);
		DBMS_OUTPUT.put_line('SQLERRM ' || SQLERRM);
	  END IF;
     END;
  END pr_drop_temp_part_tables;
  
  PROCEDURE pr_run_redefinition(
        iv_table_name 		VARCHAR2,
		iv_table_name_short VARCHAR2
  ) IS
    lv_table_sql varchar2(32767);
    ln_errors PLS_INTEGER;
  BEGIN
  
	-- Ensure that the last run did not fail
    DBMS_REDEFINITION.abort_redef_table(
      uname      => USER,        
      orig_table => iv_table_name,
      int_table  => iv_table_name_short||'_PART');  

    DBMS_OUTPUT.put_line('Start start_redef_table: ' || to_char(sysdate, 'DD-MM-YYYY hh24:Mi:SS'));

    DBMS_REDEFINITION.start_redef_table(
      uname      => USER,        
      orig_table => iv_table_name,
      int_table  => iv_table_name_short||'_PART',
      options_flag => dbms_redefinition.cons_use_rowid);  

    DBMS_OUTPUT.put_line('Start COPY_TABLE_DEPENDENTS: ' || to_char(sysdate, 'DD-MM-YYYY hh24:Mi:SS'));

    DBMS_REDEFINITION.COPY_TABLE_DEPENDENTS(USER, iv_table_name,iv_table_name_short||'_PART',
    DBMS_REDEFINITION.CONS_ORIG_PARAMS, TRUE, TRUE, TRUE, FALSE, ln_errors);

    DBMS_OUTPUT.put_line('Start SYNC_INTERIM_TABLE: ' || to_char(sysdate, 'DD-MM-YYYY hh24:Mi:SS'));
    DBMS_REDEFINITION.SYNC_INTERIM_TABLE(USER, iv_table_name, iv_table_name_short||'_PART');

    DBMS_OUTPUT.put_line('Start FINISH_REDEF_TABLE: ' || to_char(sysdate, 'DD-MM-YYYY hh24:Mi:SS'));
    DBMS_REDEFINITION.FINISH_REDEF_TABLE(USER, iv_table_name, iv_table_name_short||'_PART');

    DBMS_OUTPUT.put_line('End pr_run_redefinition: ' || to_char(sysdate, 'DD-MM-YYYY hh24:Mi:SS'));

  END pr_run_redefinition; 

  PROCEDURE pr_copy_vpd_policies(
    iv_table_name VARCHAR2,
	iv_table_interim VARCHAR2
  ) IS

--disabling policies, if any, on original table and copying it to interim table to get back in the end.
  lv_table_sql varchar2(32767);

     CURSOR cur_policies(iv_table_name VARCHAR2) IS
	 SELECT object_name, 
		policy_name
		policy_name
	  , package ||'.'||function as policy_function
	  , regexp_replace(decode(SEL,'YES',',SELECT','') || 	  decode(INS,'YES',',INSERT','') || decode(UPD,'YES',',UPDATE','') ||  decode(DEL,'YES',',DELETE',''),',','',1,1) as statement_types
	  , decode(enable,'YES','TRUE','FALSE') as enable
	  , decode(static_policy,'YES', 'TRUE','FALSE') as static_policy
	  , 'DBMS_RLS.' || policy_type as policy_type
	  , decode(long_predicate,'YES', 'TRUE','FALSE') as long_predicate
      , 'NULL' as sec_relevant_cols
      , 'NULL' as sec_relevant_cols_opt
      FROM user_policies 
      WHERE object_name = iv_table_name ;
    rec_policies   cur_policies%ROWTYPE;

  BEGIN
    DBMS_OUTPUT.put_line('Enable/Disable policy start : ' || to_char(sysdate, 'DD-MM-YYYY hh24:Mi:SS'));
    OPEN cur_policies(iv_table_name);
		  LOOP
		    FETCH cur_policies INTO rec_policies;
		    EXIT WHEN cur_policies%NOTFOUND;

			--Copying vpd policies to interim table
  		    lv_table_sql := 'begin dbms_rls.add_policy(object_name     => ''' || iv_table_interim 	||'''
								, policy_name     => ''' || rec_policies.policy_name ||'''
								, policy_function => ''' || rec_policies.policy_function ||'''
								, statement_types => ''' || rec_policies.statement_types ||'''
								, enable          => ' || rec_policies.enable ||'
								, static_policy   => ' || rec_policies.static_policy ||'
								, policy_type     => ' || rec_policies.policy_type ||'
								, long_predicate  => ' || rec_policies.long_predicate ||'
								,sec_relevant_cols=> ' || rec_policies.sec_relevant_cols ||'
								,sec_relevant_cols_opt=>' || rec_policies.sec_relevant_cols_opt ||'
								); end;';
			--DBMS_OUTPUT.PUT_LINE (lv_table_sql);
			EXECUTE IMMEDIATE lv_table_sql;

			-- Disable policy on original table to allow dbms_redefinition
			lv_table_sql := 'begin dbms_rls.enable_policy(object_name=>''' || rec_policies.object_name ||''', policy_name => ''' || rec_policies.policy_name || ''', enable=> FALSE); end;';
			EXECUTE IMMEDIATE lv_table_sql;
			DBMS_OUTPUT.PUT_LINE ('Disabled policy ' || rec_policies.policy_name);

    	END LOOP;
    CLOSE cur_policies;

  END pr_copy_vpd_policies;  
END PKG_PARTITION_MIGRATION;
--
--
/
