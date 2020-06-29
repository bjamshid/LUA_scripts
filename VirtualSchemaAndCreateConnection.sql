open schema ADAPTER;

drop CONNECTION sf_conn;
DROP ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER;
drop VIRTUAL SCHEMA snowflake CASCADE;

CREATE CONNECTION sf_conn TO 'jdbc:snowflake://<account_name>.snowflakecomputing.com/?warehouse=xs&db=<db_name>'
	USER '<user>'
	IDENTIFIED BY '<password>';

export (select 333 as TEST2, '2023-01-01' as TEST1 union all select 444 as TEST2, '2024-01-01' as TEST1) into jdbc at sf_conn
	 STATEMENT 'insert into "<SCHEMA>"."TEST_TABLE"(TEST2, TEST1) values (?,?)';

-- dont forget to upload the jars (SF jdbc and adapter jar) from GUI
CREATE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS
    %scriptclass com.exasol.adapter.RequestDispatcher;
    %jar /buckets/bucketfs1/snowflake_bucket/snowflake-virtual-schema-dist-0.1.0.jar;
/

CREATE VIRTUAL SCHEMA SNOWFLAKE
 USING adapter.jdbc_adapter
 WITH
  CONNECTION_NAME = 'sf_conn'
  SQL_DIALECT	  = 'SNOWFLAKE'
  SCHEMA_NAME     = '<SCHEMA>'
  DEBUG_ADDRESS   = '<ip>:3000'
  LOG_LEVEL       = 'ALL'
 TABLE_FILTER     = 'TEST_TABLE';

SELECT *
FROM SNOWFLAKE.TEST_TABLE;

-- see what has been pushed down to SF
SELECT pushdown_id, pushdown_involved_tables, pushdown_sql FROM
(EXPLAIN VIRTUAL SELECT * FROM SNOWFLAKE.TEST_TABLE);
