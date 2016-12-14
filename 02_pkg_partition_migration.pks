CREATE OR REPLACE PACKAGE PKG_PARTITION_MIGRATION AS
--
--
/******************************************************************************
 * Description: This package contains procedures and functions to add partitioning to a table
 * Author: Ypahariya
 * Features:
 * 			1. Facilitates partitioning of any regular db tables ONLINE using DBMS_REDEFINITION
 * 			2. Supports RANGE PARTITIONING ONLY as of now
 *			3. Takes care of Global Index rebuild post partitioning
 *			4. Handles tables with vpd policies (Oracle does not hnadle VPD policy well with DBMS_REDEFINITION earlier than Oracle 12c)
 *			
 * Needs bug fixes and further improvement:
 * 			1. To fix vpd policy issue - Fixed
 *			2. Hard coding of columns 'CREATED_DATETIME' to be removed and make it generic for any column	- Done
 * 			3. To support REFERENCE PARTITION
 *			4. To support Sub-partitions
 *			5. To support other partition types (LIST, INTERVAL etc)
 *			6. To support other partitions schemes in  (like Daily, Weekly, Qtry, Yearly etc)
 *			7. To allow conversion of monthly partitions to daily or vice versa (Above point will automatically fulfil this)
 * 
 ******************************************************************************/


  PROCEDURE pr_partition_table (
    iv_table_name            VARCHAR2,
    iv_table_name_short      VARCHAR2,
    in_num_of_partitions     NUMBER,
	iv_part_column			 VARCHAR2,
	iv_part_scheme			 VARCHAR2		-- Like 'MONTHLY', 'DAILY'
  );

  PROCEDURE pr_create_temp_part_tables(
    iv_table_name VARCHAR2,
	iv_table_name_short VARCHAR2
  );
   
  PROCEDURE pr_drop_temp_part_tables(
      iv_table_name VARCHAR2
  );
  
  PROCEDURE pr_run_redefinition(
        iv_table_name VARCHAR2,
        iv_table_name_short VARCHAR2
  );
  
  PROCEDURE pr_copy_vpd_policies(
    iv_table_name VARCHAR2,
	iv_table_interim VARCHAR2
  );	
END PKG_PARTITION_MIGRATION;
--
--                              SITA INTERNAL
--
/

