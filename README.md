# FHV_test
Contract Data Engineer assignment.
The program demonstrates a concept of a data loading from For Hire Vehicles dataset and importing of the loaded data into Oracle RDBMS.

Since I have no deep and recent experience with AWS, a regular Oracle RDBMS was used as a target data storage.
Instead of specialized SodaPy, standard "requests" library was used. The main reason - the API is quite simple and doesn't require a complex request preparation.

The program executes an incremental loading, the update timestamp of the most recent record is kept in a local file config.json.
Since I don't have an account on https://data.cityofnewyork.us/ site, I don’t use an user authentication. A real implementation should use Authentication header with a registered user token. The token maybe stored in the same config.json file.
Overall program steps -
1) Execute an incremental request to the FHV API using last time update timestamp.
2) Convert the JSON result into a CSV file (this step can be skipped if there an option to load data directly in CSV format). For the file location we assume an existing directory on Oracle server available via NAS.
3) Connect to Oracle and create an external table mapped to the just created CSV file.
4) The external table is used as a stage table to execute an actual import (using merge statement) into a target table (for this example I assumed an existence of only three columns in the target table). This step may include some sort of data transformation.
5) The external table is dropped.
6) The data file can be deleted or preserved just in case of missing/bad data investigation.
7) Imported data are available for usage via any SQL-enabled analytical and reporting tools.


This implementation only outlines the overall task. A real application should
1) use more reliable configuration storage. I.e. instead of local config file we may use a DB table
2) add an user authentication token to the HTTP request
2) the process should be split between several processes
	- loading of a new incremental portion and saving it to a local file, move it to "pending" directory
	- loading of a pending data file into Oracle table, moving the file into "loaded" or "archive" directory
	- in case of error, move the file into an "error" directory
    - separated loading of records from external table into real Oracle table. Depends on required transformations and destrinations we may use multi-step transformation processes.
	- optionally, an additional data transformation, i.e. join data acquired from different sources
	- send a notification about success/failure of the execution
	- if the iteration fails, implement a logic for a retry attempts
	- ...
