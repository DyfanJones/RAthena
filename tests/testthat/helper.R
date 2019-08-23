# helper function to skip tests if we don't have the 'boto3' module
skip_if_no_boto <- function() {
  have_boto <- py_module_available("boto3")
  if(!have_boto) skip("boto3 not available for testing")
}

# expected athena ddl's
tbl_ddl <- 
  list(tbl1 = 
DBI::SQL(paste0("CREATE EXTERNAL TABLE test_df (
  x INT,
  y STRING
)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	ESCAPED BY '\\\\'
	LINES TERMINATED BY ", gsub("_","","'\\_n'"),
"\nLOCATION 's3://test-rathena/removeable_table/test_df/'
TBLPROPERTIES (\"skip.header.line.count\"=\"1\");")),
tbl2 = 
DBI::SQL(paste0("CREATE EXTERNAL TABLE test_df (
  x INT,
  y STRING
)
ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t'
	ESCAPED BY '\\\\'
	LINES TERMINATED BY ", gsub("_","","'\\_n'"),
           "\nLOCATION 's3://test-rathena/removeable_table/test_df/'
TBLPROPERTIES (\"skip.header.line.count\"=\"1\");")), 
tbl3 = 
  DBI::SQL("CREATE EXTERNAL TABLE test_df (
  x INT,
  y STRING
)
STORED AS PARQUET
LOCATION 's3://test-rathena/removeable_table/test_df/'\n;"),
tbl4 = 
  DBI::SQL("CREATE EXTERNAL TABLE test_df (\n  x INT,\n  y STRING\n)
PARTITIONED BY (timestamp STRING)
STORED AS PARQUET
LOCATION 's3://test-rathena/removeable_table/test_df/'\n;"))
