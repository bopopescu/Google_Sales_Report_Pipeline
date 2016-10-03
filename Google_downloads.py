__author__ = 'brucepannaman'

import os
import boto
import configparser
from datetime import date, timedelta, datetime
from subprocess import call, check_output
import psycopg2

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

start_date = date(2015, 1, 1)
end_date = date(2016, 9, 1)
start_date2 = date(2016, 9, 1)
end_date2 = date.today() - timedelta(days=1)

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
            try:
                if len(check_output(["gsutil", "ls", "gs://pubsite_prod_rev_02524245599547527969/stats/installs/%s%s_overview.csv" % (app, start_date.strftime("%Y%m"))])) > 0:
                    print "Downloading Google downloads report for %s" % start_date.strftime("%Y%m")
                    call(["gsutil", "cp", "gs://pubsite_prod_rev_02524245599547527969/stats/installs/%s%s_overview.csv" % (app, start_date.strftime("%Y%m")), "google_downloads_%s_%s_2.csv" % (app, start_date.strftime("%Y%m"))])

                else:
                    print "No " + app + ' for this date'

                with open("google_downloads_%s_%s_2.csv" % (app, start_date.strftime("%Y%m")), 'rb') as source_file:
                    with open("google_downloads_%s_%s.csv" % (app, start_date.strftime("%Y%m")), 'w+b') as dest_file:
                        contents = source_file.read()
                        dest_file.write(contents.decode('utf-16').encode('utf-8'))

                print "Uploading Google sales report for %s" % start_date.strftime("%Y%m")
                call(["s3cmd", "put", "google_downloads_%s_%s.csv" % (app, start_date.strftime("%Y%m")), "s3://bibusuu/Google_Downloads_Reports/%s/downloads_report_%s_%s.csv" % (start_date.strftime("%Y%m"), app, start_date.strftime("%Y%m"))])

                print "Removing local file for %s" % start_date
                os.remove("google_downloads_%s_%s.csv" % (app, start_date.strftime("%Y%m")))
                os.remove("google_downloads_%s_%s_2.csv" % (app, start_date.strftime("%Y%m")))

            except:
                print "No app for this %s yet, maybe try tomorrow?" % start_date

    start_date = start_date + relativedelta(months=1)

# Fix for extra column 2016-10/03
while start_date2 < end_date2:

    rs = check_output(["s3cmd", "ls", "s3://bibusuu/Google_Downloads_Reports2/%s/" % start_date2.strftime("%Y%m")])

    if len(rs) > 1 and start_date2.strftime('%Y-%m') != end_date2.strftime('%Y-%m'):
        print "File Exists for %s \n Moving on ;-)" % start_date2.strftime("%Y%m")

    else:
        for app in app_list:
            print "Downloading download information for " + app
            try:
                if len(check_output(["gsutil", "ls", "gs://pubsite_prod_rev_02524245599547527969/stats/installs/%s%s_overview.csv" % (app, start_date2.strftime("%Y%m"))])) > 0:
                    print "Downloading Google downloads report for %s" % start_date2.strftime("%Y%m")
                    call(["gsutil", "cp", "gs://pubsite_prod_rev_02524245599547527969/stats/installs/%s%s_overview.csv" % (app, start_date2.strftime("%Y%m")), "google_downloads_%s_%s_2.csv" % (app, start_date2.strftime("%Y%m"))])

                else:
                    print "No " + app + ' for this date'

                with open("google_downloads_%s_%s_2.csv" % (app, start_date2.strftime("%Y%m")), 'rb') as source_file:
                    with open("google_downloads_%s_%s.csv" % (app, start_date2.strftime("%Y%m")), 'w+b') as dest_file:
                        contents = source_file.read()
                        dest_file.write(contents.decode('utf-16').encode('utf-8'))

                print "Uploading Google sales report for %s" % start_date2.strftime("%Y%m")
                call(["s3cmd", "put", "google_downloads_%s_%s.csv" % (app, start_date2.strftime("%Y%m")), "s3://bibusuu/Google_Downloads_Reports2/%s/downloads_report_%s_%s.csv" % (start_date2.strftime("%Y%m"), app, start_date2.strftime("%Y%m"))])

                print "Removing local file for %s" % start_date2
                os.remove("google_downloads_%s_%s.csv" % (app, start_date2.strftime("%Y%m")))
                os.remove("google_downloads_%s_%s_2.csv" % (app, start_date2.strftime("%Y%m")))

            except:
                print "No app for this %s yet, maybe try tomorrow?" % start_date2

    start_date2 = start_date2 + relativedelta(months=1)

# Fix for late data and rebuild of last month
if int(datetime.now().strftime("%d")) <= 5:

    start_date = datetime.now() - timedelta(days=30)

    print "Making sure data is complete for %s" % start_date

    for app in app_list:
        print "Downloading download information for " + app
        try:
            if len(check_output(["gsutil", "ls", "gs://pubsite_prod_rev_02524245599547527969/stats/installs/%s%s_overview.csv" % (app, start_date.strftime("%Y%m"))])) > 0:
                print "Downloading Google downloads report for %s" % start_date.strftime("%Y%m")
                call(["gsutil", "cp", "gs://pubsite_prod_rev_02524245599547527969/stats/installs/%s%s_overview.csv" % (app, start_date.strftime("%Y%m")), "google_downloads_%s_%s_2.csv" % (app, start_date.strftime("%Y%m"))])

            else:
                print "No " + app + ' for this date'

            with open("google_downloads_%s_%s_2.csv" % (app, start_date.strftime("%Y%m")), 'rb') as source_file:
                with open("google_downloads_%s_%s.csv" % (app, start_date.strftime("%Y%m")), 'w+b') as dest_file:
                    contents = source_file.read()
                    dest_file.write(contents.decode('utf-16').encode('utf-8'))

            print "Uploading Google sales report for %s" % start_date.strftime("%Y%m")
            call(["s3cmd", "put", "google_downloads_%s_%s.csv" % (app, start_date.strftime("%Y%m")), "s3://bibusuu/Google_Downloads_Reports2/%s/downloads_report_%s_%s.csv" % (
                  start_date.strftime("%Y%m"), app, start_date.strftime("%Y%m"))])

            print "Removing local file for %s" % start_date
            os.remove("google_downloads_%s_%s.csv" % (app, start_date.strftime("%Y%m")))
            os.remove("google_downloads_%s_%s_2.csv" % (app, start_date.strftime("%Y%m")))

        except:
            print "In catchup string \nNo app for this %s yet, maybe try tomorrow?" % start_date

# Doing Redshift Stuff

print "Finished processing Google Downloads Data \n Now Importing into redshift"


# Update files to redshift once completed

# Connect to RedShift
conn_string = "dbname=%s port=%s user=%s password=%s host=%s" %(RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

# Update the redshift table with the new results
print "Deleting old table Google_Downloads_first_bit"
cursor.execute("drop table if exists Google_Downloads_first_bit;")

print "Creating new table \n Google_Downloads_first_bit"
cursor.execute("create table google_downloads_first_bit (Date date,app_id varchar(50),Current_Device_Installs float,Daily_Device_Installs float,Daily_Device_Uninstalls float,Daily_Device_Upgrades float,Current_User_Installs float,Total_User_Installs float,Daily_User_Installs float,Daily_User_Uninstalls float);")

print "Copying Google data from S3 to  \n Google_Downloads_first_bit"
cursor.execute("COPY Google_Downloads_first_bit FROM 's3://bibusuu/Google_Downloads_Reports/'  CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' ignoreheader 1 csv;" % (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))

print "Deleting old table Google_Downloads_second_bit"
cursor.execute("drop table if exists Google_Downloads_second_bit;")

print "Creating new table \n Google_Downloads_second_bit"
cursor.execute("create table google_downloads_second_bit (Date date,app_id varchar(50),Current_Device_Installs float,Daily_Device_Installs float,Daily_Device_Uninstalls float,Daily_Device_Upgrades float,Current_User_Installs float,Total_User_Installs float,Daily_User_Installs float,Daily_User_Uninstalls float, active_device_installs float);")

print "Copying Google data from S3 to  \n Google_Downloads_2"
cursor.execute("COPY Google_Downloads_second_bit FROM 's3://bibusuu/Google_Downloads_Reports2/'  CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' ignoreheader 1 csv;" % (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))


print "Deleting old table Google_Downloads"
cursor.execute("drop table if exists Google_Downloads;")

print 'Aggregating Google_Downloads_first_bit and Google_Downloads_second_bit'
cursor.execute("create table google_downloads as select *, null as active_device_installs from google_downloads_first_bit union all select * from google_downloads_second_bit;")

print "Dropping staging tables"
cursor.execute("drop table if exists Google_Downloads_first_bit;")
cursor.execute("drop table if exists Google_Downloads_second_bit;")

conn.commit()
conn.close()
