What it does
============
If successfully started, this software will 
- determine whether there are any fasta files in an OCI object store 
- determine which have already been loaded into the server
- start loading the batch, earliest first, and process until this is finished.
- catalogue the results of the is activity in a database.

It will then repeat the process, unless no files were found, in which case it will wait 3 minutes and then repeat.

### to install
```
pipenv install --skip-lock
```

### to run
```
pipenv run python3 objectstoreaccess.py --help
```

### load fasta files from a bucket into fn4
from 'FN4-queue' in namespace 'lxxxx' into a database identified by 'unittest_oracle'
```
pipenv run python3 objectstoreaccess.py "http://localhost:5025" "lxxxx" "FN4-queue" "unittest_oracle"
```

## Monitoring progress
The software writes to two database tables
- fnbatchload: one row is written each time a batch of samples is started
- fn4loadattempt: one row is written each time an attempt is made to load a sample into the fn4 database.

Fields are commented and are self explanatory.
You can log insertions into multiple fn4 servers into the same database;
the machine and server combination is identified by host_name and findneighbour_server_url, and the below SQL will need to be modified slighly to accomodate study multiple logging.  

```
select * from db.fn4batchload order by bl_int_id;
select * from db.fn4loadattempt ;
```

### things which should not happen and could form the basis of a dashboard

* The server returns status 500 codes
```
select * FROM FN4LOADER.FN4LOADATTEMPT where not insert_status_code = 200 order by gp_int_id desc;
```
* The batch loader isn't trying to insert data.  It tries every minute, unless insertion is in progress.

Because of this, one of two things must have occurred in the last 60 seconds:
* we inserted something successfully
* a batch of samples was analysed (and no samples were found)

```
select * FROM FN4LOADER.FN4LOADATTEMPT where not insert_status_code = 200 order by gp_int_id desc;
select max(check_time) from FN4LOADER.FN4BATCHLOAD;
```

The following (in PL/SQL, oracle specific SQL) will recover the dates needing comparison:
```
select current_timestamp, rct.recent_check_time, rsi.recent_successful_insert_time,  from dual
cross JOIN
(select max(check_time) recent_check_time from FN4LOADER.FN4BATCHLOAD) rct
cross join 
(select max(insert_start_time) recent_successful_insert_time FROM FN4LOADER.FN4LOADATTEMPT where insert_status_code = 200) rsi
;
```

## Error logging
If the environment variable FN_SENTRY_URL is set to a sentry.io connection string (it has to be set in the .env in you are using a virtual environment), then all errors will be logged to sentry.io.

## Background
Oracle provide a [SDK](https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/) for their infrastructure.  It requires [authentication](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm).  Examples of use in python are [provided](https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/configuration.html).  The SDK includes an [ObjectStorageClient](https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/api/object_storage/client/oci.object_storage.ObjectStorageClient.html) which can perform the objectives.

This code uses 
- the Oracle SDK to access the objectstore
- SQLalchemy to access databases  to log the results of the loader's activities. 

## Prerequisites
[OCI Python SDK](https://oracle-cloud-infrastructure-python-sdk.readthedocs.io/en/latest/installation.html) which can be installed via PyPI

Notes
==========
Objective of the code is to 
- access Oracle Object Stores
- enumerate the contents of such stores
- download data from them
- upload data to them
- load data from object stores in a findNeighbour4 server
- catalogue the results of this activity to a database
At present all the classes doing this are in one file, objectstoreaccess.py
