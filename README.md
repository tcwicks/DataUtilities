# Data Utilities
This repository will contain various convenience utilities which I create to enhance my quality of life while working with the Google Cloud Platform.

* SQL CLR Extensions - BCP Operations Support
* SQL CLR Extensions - GCP Credential Store
* SQL CLR Extensions - SQL Server to GCP Big Query Replication
* SQL SChema Only Replication command line utility
* CSV Merge command line utility

## SQL CLR Extensions
Adds support within SQL Server for Select command to Table BCP operations.
Adds support within SQL Server for replicating schema and data to Google Big Query datasets
Adds support for securely storing GCP service account credentials within SQL server

Please see source code for explanation on how to install within SQL Server and instrutions on properly signing the assemby

### SQL Server BCP support
Often with ETL processes we need to insert large recordsets from a view, table or select statement into a table. This presents a number of challenges:
* Insert Into is the only minimally logged operation
* Large inserts will lock the table for extended periods of time
* Not possible to leverage concurrent inserts to a table targeting different partition files

Three BCP extension methods are implemented:
* CXSQLExt.BCPSourceSQLToTable():   Simple BCP suitable only for small record sets (Reads to memory and then writes)
* CXSQLExt.BCPSourceSQLToTableThreaded():  Multi threaded BCP operation which will run one thread per select statement provided
* CXSQLExt.BCPCopyToTableThreaded(): Work in progress - Runs independent reader threads and independent writer threads - (Not yet fully stable on large datasets) 

### Secure credentials store
GCP Big Query integration requires securely storing service account credentials. Storing these as plain text JSON files on the file system is incredibly risky. Storing these in a database table in plain text is perhaps even worse.
This functionality implements a triple encrypted store for service account credentials.
Credentials are encrypted using Microsoft DPAPI and wrapped with SHA256 encryption
* Layer 1: Encrypt using standard SHA256 and password derived key (This is weak but adds extra protection as the code would have to be downloaded and decompiled to obtain the hard coded component of the key)
* Layer 2: Encrypt using DPAPI against the current user. This locks the credentials to the database user identity which saved the credentials in the first place. Additionally hacking this would require hacking the user account master key in Active Directory
* Layer 3: Encrypt using DPAPI against the machine account. This locks the credentials to the database server where the credentials were saved in the first place. Additionally hacking this would require hacking the machine account master key in Active Directory

Implemented methods:
* CXSQLExt.CredentialStoreSetup() -- Sets up the dataase table for storing encrypted credentials in the current database
* CXSQLExt.CredentialSave(string secretKey, string secretValue)  -- Saves credentials under the specified key

No retreve function is implemented. This is intentional. GCP Big Query methods will retrieve the credentials internally and use them based on the specified key.

### SQL Server To Google Big Query Replication
Google big query is amazing for querying ginormous record sets. However its not the best for complex ETL operations with point updates. CDC based replication carries a significant overhead. This implementation uses a keep it simple approach. This implementation will:
* Create a compatible Big Query Schema for any SQL Database View
* Multi threaded streaming replication of source table data into the Big Query table

Note: For replication the system expect the SQL server table to contain 3 key fields:
* Created By Version Field. This is expected to be an integer value with creation batch version number of each set of rows created
* Modified By Version Field. This is expected to be an integer value with the modification batch version number of each set of rows modified
* Unique Key. This is expected to be a unique key field

Note: Requires a view in SQL Server as the source. If the source is the table data with no modifications then simply wrap it with a view.

Implemented methods:
* CXSQLExt.GCPBQExecSQL()  Use with care - will execute specified SQL in big query. *** use with care
* CXSQLExt.GCPBQDropSchema()   Use with care - will drop the specified table in Big Query. *** use with care
* CXSQLExt.GCPBQSyncSchema()   Will analyze the specified SQL Server view and create a compatible table in Big Query
* CXSQLExt.GCPBQSyncDataSet()    Will syncronize the dataset from the specified SQL server view into the specified destination table in Big Query.

## SQL Schema Replicate
There are multiple use cases for this.
My motivation was because complex ETL processes some databases may contain very large ephemeral staging databases. While the data itself is not worth backing up the stored procedures, functions, schemas etc.. are all very important. In this context the schema can be synced to a different database which is regularly backed up. In summary this allows backing up only the schema and excluding the data of a large terrabyte database

Command line utility with options provided to:
* Compare and Generate SQL Script
* Compare and Directly Publish Schema Syncronization Changes

## CSVMerge
Often we need to extract data from Big Query tables. Unfortunately the extract operation likes to split the output into a large number of files. This is especially true if the row counts are very large. Sure we can add a Limit 9999999999 type operator to the query to force a single file by forcing executing on only the leader node. However for large datasets this is not an option as it will take a very long time which if greater than 6 hours will simply timeout.

Connsider the scenario where the dataset extraction has created 3000 gzipped files in the destination cloud storage bucket.
Obviously we exported this data in order to import it into somewhere else. For example SQL server for analysis or whatever purpose.
However importing 3000 files one by one is not something anyone wants to do.

CSVMerge will take the a folder containing the 3000 files and concatenate them (uncompressing if specified) and write the output to a single flat file. You can specify that the headers should be kept for the first file and stripped out of the remaining files.

### Example:

Step 1) run EXPORT DATA OPTIONS in Big Query to generate the extract

Note: include the compression = 'GZIP' option to greatly reduce file sizes and speed up the download.

Step 2) Install the Google Cloud CLI Installer - Currently at: https://cloud.google.com/sdk/docs/install

Step 3) Download the bucket containing the large number of extract output files

Example: gsutil -m cp -R gs://<<Your Bucket>> c:\Temp\Data_Extract

Step 4) Run CSVMerge to join the files

Example: CSVMerge.exe 0 1 gzip c:\Temp\Data_Extract "C:\Temp\Merged Data Extract.csv"

### Limitations
This utility is currently single threaded. At some point I will make it multi threaded. The catch with muti threading here is that it would require buffering out of files processed concurrently so that they can be written to the output in the correct order.

