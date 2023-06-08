import requests
import mysql.connector
import csv
from datetime import date
import os

def fetch_city_dataset(example_parameter=0)->list[dict]:
    api_endpoint = "https://public.opendatasoft.com/api/"
    search_request_path = "records/1.0/search/"
    parameters= {
            'dataset': "geonames-all-cities-with-a-population-1000",
            'rows': 1000000
    }
    full_url = api_endpoint+search_request_path
    result = requests.get(full_url, parameters)
    if result.status_code==200:
        records = []
        for record in result.json()['records']:
            row = record['fields']
            row['id'] = record['recordid']
            row['timestamp'] = record['record_timestamp']
            records.append(row)
        return records

def create_database():   
    mydb=mysql.connector.connect(host="127.0.0.1",user="root",password="Pa$$w0rd",database="city_country")
    x_cursor=mydb.cursor()
    x_cursor.execute("""
        CREATE TABLE city (
        GeonameID INT AUTO_INCREMENT PRIMARY KEY,
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
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="Pa$$w0rd",
        database="city_country"
    )
    x_cursor = mydb.cursor()

    with open('geonames-all-cities-with-a-population-1000.csv', 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file, delimiter=';')
        for row in csv_reader:
            city_data = (
                row['Geoname ID'],
                row['Name'],
                row['ASCII Name'],
                row['Feature Class'],
                row['Feature Code'],
                row['Country Code'],
                row['Admin1 Code'],
                row['Admin2 Code'],
                row['Admin3 Code'],
                row['Admin4 Code'],
                row['Population'],
                row['Elevation'],
                row['DIgital Elevation Model'],
                row['Timezone'],
                row['Modification date'],
                row['Coordinates']
            )
            city_sql = """
            INSERT INTO city (
                GeonameID, Name, ASCIName, FeatureClass, FeatureCode, CountryCode,
                Admin1Code, Admin2Code, Admin3Code, Admin4Code, Population,
                Elevation, DigitalElevationModel, Timezone, ModificationDate, Coordinates
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            x_cursor.execute(city_sql, city_data)
    with open('geonames-all-cities-with-a-population-1000.csv', 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file, delimiter='\t')
        for row in csv_reader:
            country_data = (
                row['Country Code'],
                row['Country name EN'],
                row['Country Code 2']
            )
            country_sql = """
            INSERT INTO country (CountryCode, CountryNameEN, CountryCode2) VALUES (%s, %s, %s)
            """
            x_cursor.execute(country_sql, country_data)
    with open('geonames-all-cities-with-a-population-1000.csv', 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file, delimiter='\t')
        for row in csv_reader:
            geoname_id = row['Geoname ID']
            alternate_names = row['Alternate Names'].split(',')
            for name in alternate_names:
                alternate_names_data = (
                    geoname_id,
                    name.strip()
                )

                alternate_names_sql = """
                INSERT INTO alternate_names (GeonameID, AlternateNames) VALUES (%s, %s)
                """
                x_cursor.execute(alternate_names_sql, alternate_names_data)
    mydb.commit()
    return

def select_no_megapolis_countries()->MySQLCursor:
    query = """
        SELECT 
            co.CountryCode
            ,co.CountryNameEN
        FROM
            country co 
        WHERE 
            co.country_code NOT IN 
                (SELECT
                    ci.country_code
                FROM
                    city ci
                WHERE
                    ci.population > 10000000
                ORDER BY
                    co.CountryCode
                    ,co.CountryNameEN)            
        """
    return 

def cursor_to_csv(cursor:mysql.connector.MySQLCursor, csv_file_path):
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
    