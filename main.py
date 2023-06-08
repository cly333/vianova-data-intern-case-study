# --> comments for you, it's not really suited for a documentation
# To document properly as they expect you to, I recommend using this VS Code extension : autoDocstring - Python Docstring Generator
import requests
import mysql.connector
import csv
from datetime import date
import os

def fetch_city_dataset(example_parameter=0)->list[dict]:
    
    # You have to fetch the dataset which is at the link they gave you 
    # (https://public.opendatasoft.com/explore/dataset/geonames-all-cities-with-a-population-1000/export/?disjunctive.cou_name_en) 
    # To do so, you have to do an API call to the Open DataSoft API
    # It is really important for companies to know how to be able to do API calls the right way,
    # so try to keep this kind of function in mind and don't hesitate to reuse it 
    api_endpoint = "https://public.opendatasoft.com/api/"
    search_request_path = "records/1.0/search/"
    # Add here the right parameters to put inside your request (ex: here, dataset is a parameter)
    # you can find all of the parameters of this request here :
    # https://public.opendatasoft.com/explore/dataset/geonames-all-cities-with-a-population-1000/api/?disjunctive.cou_name_en
    # you have to experiment with the parameters to get the right ones (your objective is to get all the dataset, you might have to do it in multiple requests in a for loop)
    parameters= {
            'dataset': "geonames-all-cities-with-a-population-1000",
            'rows': 1000000
    }
    # Url of the request you want to make
    full_url = api_endpoint+search_request_path
    # Send the request to the API endpoint and store the result in the result variable
    result = requests.get(full_url, parameters)
    # check the status code of the request (200 means it is successful)
    if result.status_code==200:
        # result json file looks like this : 
        # { parameters: dict, records: list[ { datasetid:str, recordid:str, fields:dict, geometry:dict, record_timestamp:str } ]
        # what we care about is the fields of the records, the recordid and the record timestamp as it is the values that go into a database
        records = []
        for record in result.json()['records']:
            row = record['fields']
            row['id'] = record['recordid']
            row['timestamp'] = record['record_timestamp']
            records.append(row)
        return records

def create_database():
    # you have to create a mysql database with at least one table in it (city)
    # to do so, you should use the mysql package
    # here is a full tutorial on how to do all of these steps :
    # https://www.w3schools.com/python/python_mysql_getstarted.asp 
    # for the city database, you should create one column per field in the result of the fetch_city_dataset function
    # I put an example of record in the record_example.json file
    # the coordinates field is a list that can be turned into two fields : coordinates_x and coordinates_y 
    # for 'alternate_names', you can create a second table with a 1-n relationship
    # you should also have a 'country' table, with country_name and country_code
    mydb=mysql.connector.connect(host="127.0.0.1",user="root",password="Pa$$w0rd",database="city_country")
    x_cursor=mydb.cursor()
    x_cursor.execute("""
        CREATE TABLE city (
        GeonameID INT PRIMARY KEY,
        Name VARCHAR(255),
        ASCIName VARCHAR(255),
        FeatureClass CHAR(1),
        FeatureCode VARCHAR(10),
        CountryCode CHAR(2),
        Admin1Code CHAR(2),
        Admin2Code VARCHAR(20),
        Admin3Code VARCHAR(20),
        Admin4Code VARCHAR(20),
        Population INT,
        Elevation INT,
        DigitalElevationModel INT,
        Timezone VARCHAR(50),
        ModificationDate DATE,
        Coordinates VARCHAR(50)
        );""")
    x_cursor.execute("""
        CREATE TABLE country (
        CountryCode CHAR(2) PRIMARY KEY,
        CountryNameEN VARCHAR(255),
        CountryCode2 CHAR(2)
        );""")
    x_cursor.execute("""
        CREATE TABLE alternate_names (
        GeonameID INT,
        AlternateNames VARCHAR(255),
        FOREIGN KEY (GeonameID) REFERENCES city (GeonameID)
        );""")  
    return

def insert_city_record(fields: dict):
    # process one field and insert the right fields in the right table
    # do an insert statement : https://www.w3schools.com/python/python_mysql_insert.asp
    mydb=mysql.connector.connect(host="127.0.0.1",user="root",password="Pa$$w0rd")
    x_cursor=mydb.cursor()
    sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    val = ("John", "Highway 21")
    x_cursor.execute(sql, val)
    return

def select_no_megapolis_countries()->MySQLCursor:
    # run the following query into the database
    # run a query : https://www.w3schools.com/python/python_mysql_select.asp
    query = """
        SELECT 
              co.country_code
            , co.country_name
        FROM
            country co 
        WHERE 
            co.country_code NOT IN 
                (SELECT
                    ci.country_code
                FROM
                    city ci
                WHERE
                    ci.population > 10000000)            
        """
    return 

def cursor_to_csv(cursor:MySQLCursor, csv_file_path):
    rows = cursor.fetchall()
    fp = open(csv_file_path, 'w')
    myFile = csv.writer(fp, delimiter='\t')
    myFile.writerows(rows)
    fp.close()

def __main__():
    create_database()
    cities = fetch_city_dataset()
    for city in cities :
        insert_city_record(city)
    no_megapolis_countries = select_no_megapolis_countries()
    export_dir = 'Exports'
    file_name = 'no_megapolis_countries '+date.today().isoformat()+'.tsv'
    cursor_to_csv(no_megapolis_countries, os.path.join(export_dir, file_name))
    