# FHV_test
Contract Data Engineer assignment.
The program demostrates a concept of a data loading from For Hire Vehicles dataset and importing of the loaded data into Oracle RDBMS.

Since I have no deeep and recent experience with AWS, a regular Oracle RDBMS was used as a target data storage.
Instead of specialized SodaPy, standard "requests" library was used. The main reason - the API is quite simple and doesn't require a complext request preparation.
The program executes an incremental loading, the update timestamp of the most recent record is kept in a local file config.json.
Since I don't have an account on https://data.cityofnewyork.us/ site, I did'nt use user authentication. A real implemennation should use Authentication header with a registered user token. The token maybe stored in the same config.json file.

Overall program steps -
Execute an incremental request to the FHV API using last time update timestamp.
Convert the JSON result into a CSV file (this step can be skipped if there an option to load data directly in CSV format). For the file location we assume an existing directory on Oracle server available via NAS.
Connect to Oracle and create an external table mapped to the just created CSV file.
The exteranl table is used as astage table to execute an actual import (using merge statement) into a target table (for this example I assuemed an existance of only two columns in the target table). This step may inlcude some sort of data transformation.
The external table is dropped.
The data file can be deleted or preserved just in case of missing/bad data investigation.
Inported data are available for usage via any SQL-enabled analitical and reporting tools.


This implementation only outlines the overall task. A real application should
1) use more reliable configuration storage. I.e. instead of local config file we may use a DB table
2) add an user authentication token to the HTTP request
2) the process should be split between different sub-processes
	- loading of a new incremental portion and saving it to a local file, move it to "pending" directory
	- loading of a pending data new data into Oracle table, moving the file into "loaded" or "archive" directory
	- in case of error, move the file into an "error" directory
	- optional, an additional data transformation, i.e. join data aquired from different sources
	- send a notification about success/failure of the execution
	- if the iteration fails, implement a logic for a retry attemps
	- ...