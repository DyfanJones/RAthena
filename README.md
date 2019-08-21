
# RAthena

The goal of the RAthena package is to provide a DBI-compliant interface
to Athena without the requirement of using odbc and jdbc drivers. This
will allow the connection to be athena to be straight forward and only
require the installation of python and the AWS SDK boto3.

With the development of AWS sagemaker RAthena will make the connection
to AWS Athena simpler.

## Installation:

``` r
remotes::install_bitbucket("shopdirect/rathena",
                           auth_user = "<bitbucket_username>",
                           password = "<bitbucket_password>")
```

## Usage

### Basic Usage

Connect to athena, and send a query and return results back to R.

``` r
library(DBI)

con <- dbConnect(RAthena::athena(),
                aws_access_key_id='YOUR_ACCESS_KEY_ID',
                aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
                s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                region_name='us-west-2')

res <- dbExecute(con, "SELECT * FROM one_row")
dbFetch(res)
dbClearResult(res)
```

To retrieve query in 1 step.

``` r
dbGetQuery(con, "SELECT * FROM one_row")
```

### Intermediate Usage

To create a tables in athena, `dbExecute` will send the query to athena
and wait until query has been executed. This makes it and idea method to
create tables within athena.

``` r
query <- 
  "CREATE EXTERNAL TABLE impressions (
      requestBeginTime string,
      adId string,
      impressionId string,
      referrer string,
      userAgent string,
      userCookie string,
      ip string,
      number string,
      processId string,
      browserCookie string,
      requestEndTime string,
      timers struct<modelLookup:string, requestTime:string>,
      threadId string,
      hostname string,
      sessionId string)
  PARTITIONED BY (dt string)
  ROW FORMAT  serde 'org.apache.hive.hcatalog.data.JsonSerDe'
      with serdeproperties ( 'paths'='requestBeginTime, adId, impressionId, referrer, userAgent, userCookie, ip' )
  LOCATION 's3://elasticmapreduce/samples/hive-ads/tables/impressions/' ;"
  
dbExecute(con, query)
```

To find out how the table has been partitioned simply call the
`dbGetParitiions` on the table you wish to find out. The result will be
returned in a dataframe.

``` r
RAthena::dbGetPartition(con, "impressions")
```

### Advanced Usage

#### Sending data to Athena

RAthena has created a method to send data.frame from R to Athena.

``` r
# Check existing tables
dbListTables(con)
# Upload mtcars to Athena
dbWriteTable(con, "mtcars", mtcars, 
             partition=c("TIMESTAMP" = format(Sys.Date(), "%Y%m%d")),
             s3.location = "s3://mybucket/data/")

# Read in mtcars from Athena
dbReadTable(con, "mtcars")

# Check new existing tables in Athena
dbListTables(con)

# Check if mtcars exists in Athena
dbExistsTable(con, "mtcars")
```

### Tidyverse Usage

``` r
library(DBI)
library(dplyr)

con <- dbConnect(RAthena::athena(),
                aws_access_key_id='YOUR_ACCESS_KEY_ID',
                aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
                s3_staging_dir='s3://YOUR_S3_BUCKET/path/to/',
                region_name='us-west-2')
tbl(con, sql("SELECT * FROM one_row"))
```
