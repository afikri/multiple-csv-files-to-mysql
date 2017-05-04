#!/usr/bin/python

import csv
import glob
import MySQLdb

import pandas as panda

db_connection = MySQLdb.connect('localhost','root','superuser@mariadb','dbcsv')
cursor = db_connection.cursor()

# Read small multiple files and merge them into large singel file using panda lib
myfiles = glob.glob("*.csv")
df_list = []

for filename in sorted(myfiles):
    print(filename)
    df = panda.read_csv(filename, header=None)
    df_list.append(df)
concat_df = panda.concat(df_list, axis=0)    
concat_df.to_csv('outfile.csv', index=None, header=None)

csv_data = csv.reader(file('outfile.csv'))

sql = "INSERT INTO `ref_country_division_type`(`level`, `name`) VALUES (%s,%s);"

# execute and insert the csv into the mysql database.
csv_data.next()
for row in csv_data:
    cursor.execute((sql),row)
db_connection.commit()
cursor.close()
print "CSV has been imported into the database"
