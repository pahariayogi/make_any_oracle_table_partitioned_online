--
--
/******************************************************************************
 * Description: This package contains procedures to drop table partitions
 * Author: Ypahariya
 *
 ******************************************************************************/

 /* The current logic may not work as its drops the trailing partitions assuming the case of interval partitions */

-- CREATE PROCEDURE TO RUN FROM A JOB
--The current logic may not work as its drops the trailing partitions assuming the case of interval partitions
-- create or replace
-- PROCEDURE purge_by_partition(i_table_name varchar2, i_no_of_part_retained number)
-- IS
-- --	i_table_name varchar2(100) := 'SYSTEM_AUDIT_LOG_PARAMETERS';
-- --	i_no_of_part_retained number := 1;	-- Change this to tweak retention of data
	-- lv_partion_dropped varchar2(1);
	  -- -- partition position = 1 always grabs the "oldest" partition. This does not get dropped.
	-- CURSOR cur_partition IS
	-- SELECT partition_name,high_value
	-- FROM user_tab_partitions
	-- where table_name = i_table_name
	  -- and partition_position <> 1
    -- AND partition_position <= (SELECT MAX(partition_position) FROM user_tab_partitions where table_name = i_table_name) - i_no_of_part_retained
	-- order by partition_position;

	-- rec_partition   cur_partition%ROWTYPE;

-- begin
	-- OPEN cur_partition;
	-- LOOP
		-- FETCH cur_partition INTO rec_partition;
		-- EXIT WHEN cur_partition%NOTFOUND;

		-- execute immediate 'ALTER TABLE '||i_table_name||' DROP PARTITION ' || rec_partition.partition_name || ' UPDATE INDEXES';
		-- dbms_output.put_line(i_table_name||' partition dropped => ' || rec_partition.partition_name);
		-- lv_partion_dropped := 'Y';
	-- END LOOP; -- End of loop
	-- CLOSE cur_partition;
-- end purge_by_partition;
-- /

--prompt Executing purge_by_partition...
--exec purge_by_partition('SYSTEM_AUDIT_LOG_PARAMETERS',3);

/* **** Manually sys audit partitons for now like... */

ALTER TABLE SYSTEM_AUDIT_LOG_PARAMETERS DROP PARTITION SYALP31JAN2015 UPDATE INDEXES;	--Change partition name 'SYALP31JAN2015' as required



