## Project Description

### Background

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Architecture

This project uses 2 AWS services - S3 and Spark.

Data sources come from two public S3 buckets (user activity and songs metadata) in the form of JSON files.

As part of the ETL pipeline, Spark is used to process the data to build a set of dimensional tables, which will be loaded back into a different S3 bucket.

## Project structure:
```
# config file
dl.cfg

# run ETL
etl.py
```

## How to run:

You need to create the output S3 bucket defined in `etl.py`.
Fill in your AWS credentials in the template file `dl.cfg` as shown below

```
[AWS]
AWS_SECRET_ACCESS_KEY=
AWS_ACCESS_KEY_ID=
```

Run the following scripts:
```
# extract data from JSON files in S3, process with spark, and store the processed tables in S3
python etl.py
```

