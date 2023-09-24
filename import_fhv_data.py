#!/usr/bin/env python

# make sure to install these packages before running:
# pip install cx_Oracle

import json
import csv
import os.path
import requests
import cx_Oracle

# load settings and last update timestamp
with open('settings.json') as json_file:
    config = json.load(json_file)
last_ts = config.get("last_ts", "2023-09-18")
oracle_dir = config["oracle_dir"]

# load dat updated since last timestamp
result = requests.get(f"https://data.cityofnewyork.us/resource/8wbx-tsch.json", params={"$where": "last_date_updated >= '2023-09-17T00:00:00.000'", "$order": last_ts})
if result.status_code==200:
    data = result.json()

    # create new CSV file in the directory on Oracle server ...
    data_file = open(os.path.join(oracle_dir, 'dhv_data.csv'), 'w')
    # create the csv writer object
    csv_writer = csv.writer(data_file)

    count = 0
    import pdb; pdb.set_trace()
    for rec in data:

        if count == 0: # 1st record, prepare CSV header...
            # Writing headers of CSV file
            header = rec.keys()
            csv_writer.writerow(header)
            count += 1
     
        # Writing data of CSV file
        csv_writer.writerow(rec.values())
        last_ts = rec["last_date_updated"]

    data_file.close()
    pdb.set_trace()

    connection = cx_Oracle.connect(user=config["oracle_user"], password=config["oracle_pswd"], dsn=config["oracle_instance"])
    cur = connection.cursor()
    # create an external table to serve as a stage table
    cur.execute("""CREATE TABLE DHV_DATA (
      active                 VARCHAR2(5),
      vehicle_license_number VARCHAR2(15),
      name                   VARCHAR2(50),
      license_type           VARCHAR2(15),
      expiration_date        DATE,
      permit_license_number  VARCHAR2(20),
      dmv_license_plate_number VARCHAR2(20),
      vehicle_vin_number     VARCHAR2(18),
      certification_date    DATE,
      hack_up_date          DATE,
      vehicle_year          VARCHAR2(4),
      base_number           VARCHAR2(10), 
      base_name             VARCHAR2(50),
      base_type             VARCHAR2(20),
      veh                   VARCHAR2(50),
      base_telephone_number VARCHAR2(15),
      base_address          VARCHAR2(100),
      reason                VARCHAR2(10),
      last_date_updated     DATE,
      last_time_updated     DATE
     )
  ORGANIZATION EXTERNAL
    (TYPE ORACLE_LOADER
     DEFAULT DIRECTORY def_dir_1
     ACCESS PARAMETERS
       (RECORDS DELIMITED BY NEWLINE
        FIELDS (
          active                 VARCHAR2(5),
          vehicle_license_number VARCHAR2(15),
          name                  VARCHAR2(50),
          license_type          VARCHAR2(15),
          expiration_date       VARCHAR2(23) date_format DATE mask "yyy-mm-ddThh24:mi:ss.zzz",
          permit_license_number VARCHAR2(20),
          dmv_license_plate_number VARCHAR2(20),
          vehicle_vin_number    VARCHAR2(18),
          certification_date    VARCHAR2(23) date_format DATE mask "yyy-mm-ddThh24:mi:ss.zzz",
          hack_up_date          VARCHAR2(23) date_format DATE mask "yyy-mm-ddThh24:mi:ss.zzz",
          vehicle_year          VARCHAR2(4),
          base_number           VARCHAR2(10), 
          base_name             VARCHAR2(50),
          base_type             VARCHAR2(20),
          veh                   VARCHAR2(50),
          base_telephone_number VARCHAR2(15),
          base_address          VARCHAR2(100),
          reason                VARCHAR2(10),
          last_date_updated     VARCHAR2(23) date_format DATE mask "yyy-mm-ddThh24:mi:ss.zzz",
          last_time_updated     VARCHAR2(23) date_format DATE mask "yyy-mm-ddThh24:mi:ss.zzz"
        )
       )
     LOCATION ('dhv_data.cvs')
    )""")

    # import into an actual table, we assume only three columnt in the target table - VEHICLE_LICENSE_NUMBER (PK), NAME, and VEHICLE_YEAR
    cur.execute("""MERGE INTO MY_TARGET_TABLE DST USING (SELECT * FROM DHV_DATA) SRC on (SRC.vehicle_license_number=DST.vehicle_license_number)
        when matched then
            update set DST.name = SRC.name, DST.vehicle_year=SRC.vehicle_year
        when not matched then                     -- if match is not found, insert
            insert (vehicle_license_number, name, vehicle_year) values (src.vehicle_license_number, src.name, src.vehicle_year)"""
               )
    
    # drop the stage table
    cur.execute("DROP TABLE DHV_DATA")

    # save last timestamp...
    config.get["last_ts"] = last_ts
    with open('settings.json', 'w') as json_file:
        json.dump(config, json_file)

else:
    print("Unable to load FHV data, HTTP status code=", result.statuc_code)