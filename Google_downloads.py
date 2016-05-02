__author__ = 'brucepannaman'

import os
import boto
import configparser
from datetime import date, timedelta
from subprocess import call, check_output
import psycopg2
import zipfile
from dateutil.relativedelta import relativedelta


config = configparser.ConfigParser()
ini = config.read('conf2.ini')


AWS_ACCESS_KEY_ID = config.get('AWS Credentials', 'key')
AWS_SECRET_ACCESS_KEY = config.get('AWS Credentials', 'secret')
RED_HOST = config.get('Redshift Creds', 'host')
RED_PORT = config.get('Redshift Creds', 'port')
RED_USER = config.get('Redshift Creds', 'user')
RED_PASSWORD = config.get('Redshift Creds', 'password')

conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
bucket = conn.get_bucket('bibusuu')

start_date = date(2015, 9, 1)
end_date = date(2016, 4, 1)
# end_date = date.today()

app_list = ['installs_com.busuu.kids.es_','installs_com.busuu.kids.en_', 'installs_com.busuu.android.zh_', 'installs_com.busuu.android.pt_', 'installs_com.busuu.android.es_', 'installs_com.busuu.android.fr_', 'installs_com.busuu.android.en_', 'installs_com.busuu.android.enc_', 'installs_com.busuu.android.tr_', 'installs_com.busuu.android.ru_', 'installs_com.busuu.android.pl_', 'installs_com.busuu.android.ja_', 'installs_com.busuu.android.it_', 'installs_com.busuu.android.de_']

print "Looking for Sales files in s3 not downloaded yet"

while start_date < end_date:

    print start_date
    rs = check_output(["s3cmd", "ls", "s3://bibusuu/Google_Downloads_Reports/%s/" % start_date.strftime("%Y%m")])

    if len(rs) > 1 and start_date.strftime('%Y-%m') != end_date.strftime('%Y-%m'):
        print "File Exists for %s \n Moving on ;-)" % start_date.strftime("%Y%m")

    else:
        for app in app_list:
            print "Downloading download information for " + app

            print "Downloading Google downloads report for %s" % start_date.strftime("%Y%m")
            call(["gsutil", "cp", "gs://pubsite_prod_rev_02524245599547527969/stats/installs/%s%s_overview.csv" % (app, start_date.strftime("%Y%m")), "google_downloads_%s_%s.csv" % (app, start_date.strftime("%Y%m"))])

            print "Uploading Google sales report for %s" % start_date.strftime("%Y%m")
            call(["s3cmd", "put", "google_downloads_%s_%s.csv" % (app, start_date.strftime("%Y%m")), "s3://bibusuu/Google_Downloads_Reports/%s/downloads_report_%s_%s.csv" % (start_date.strftime("%Y%m"), app, start_date.strftime("%Y%m"))])

            print "Removing local file for %s" % start_date
            os.remove("google_downloads_%s_%s.csv" % (app, start_date.strftime("%Y%m")))

    start_date = start_date + relativedelta(months=1)

print "Finished processing Google Sales Data \n Now Importing into redshift"

# Update files to redshift once completed

# Connect to RedShift
conn_string = "dbname=%s port=%s user=%s password=%s host=%s" %(RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

# Update the redshift table with the new results
print "Deleting old table Google_Downloads_2"
cursor.execute("drop table if exists Google_Downloads_2;")

print "Creating new table \n Google_Downloads_2"
cursor.execute("Create table google_downloads_2 ( Date date, app_id varchar(50), Current_Device_Installs int, Daily_Device_Installs int, Daily_Device_Uninstalls int, Daily_Device_Upgrades int, Current_User_Installs int, Total_User_Installs int, Daily_User_Installs int, Daily_User_Uninstalls int );")

print "Copying Google data from S3 to  \n Google_Downloads_2"
cursor.execute("COPY Google_Downloads_2 FROM 's3://bibusuu/Google_Downloads_Reports/'  CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' IGNOREHEADER 1 csv;" % (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))

print "Deleting old table Google_Downloads"
cursor.execute("drop table if exists Google_Downloads;")

print 'Renaming Google_Downloads_2 to Google_Downloads'
cursor.execute("alter table Google_Downloads_2 rename to Google_Downloads;")

conn.commit()
conn.close()
