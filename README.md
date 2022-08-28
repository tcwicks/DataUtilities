# GCPUtilities
This repository will contain various convenience utilities which I create to enhance my quality of life while working with the Google Cloud Platform.

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

Example: CSVMerge.exe 1 0 gzip c:\Temp\Data_Extract "C:\Temp\Merged Data Extract.csv"

### Limitations
This utility is currently single threaded. At some point I will make it multi threaded. The catch with muti threading here is that it would require buffering out of files processed concurrently so that they can be written to the output in the correct order.
