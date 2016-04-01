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

start_date = date(2013,9,1)
end_date = date.today()


print "Looking for Sales files in s3 not downloaded yet"

while start_date < end_date:

    rs = check_output(["s3cmd", "ls", "s3://bibusuu/Google_sales_reports/%s/" % start_date.strftime("%Y%m")])

    if len(rs) > 1 and start_date.strftime('%Y-%m') != end_date.strftime('%Y-%m'):
         print "File Exists for %s \n Moving on ;-)" % start_date.strftime("%Y%m")

    else:

        print "Downloading Google sales report for %s" % start_date.strftime("%Y%m")
        call(["gsugsutil", "cp", "gs://pubsite_prod_rev_02524245599547527969/sales/salesreport_%s.zip" % start_date.strftime("%Y%m"), "/home/busuuadmin/Google_Sales_Report_Pipeline/google_sales_data_%s.zip" % start_date.strftime("%Y%m")])

        print "Unzipping File"
        zip = "/home/busuuadmin/Google_Sales_Report_Pipeline/google_sales_data_%s.zip" % start_date.strftime("%Y%m")
        with zipfile.ZipFile(zip, "r") as z:
            z.extractall("" )


        print "Uploading Google sales report for %s" % start_date.strftime("%Y%m")
        call(["s3cmd", "put", "salesreport_%s.csv" % start_date.strftime("%Y%m")  , "s3://bibusuu/Google_sales_reports/%s/salesreport_%s.csv" % (start_date.strftime("%Y%m"),start_date.strftime("%Y%m"))])

        print "Removing local file for %s.zip" % start_date
        os.remove("/home/busuuadmin/Google_Sales_Report_Pipeline/google_sales_data_%s.zip" % start_date.strftime("%Y%m"))
        os.remove("salesreport_%s.csv" % start_date.strftime("%Y%m"))



    start_date = start_date + relativedelta(months=1)

print "Finished processing Google Sales Data \n Now Importing into redshift"

# Update files to redshift once completed

# Connect to RedShift
conn_string = "dbname=%s port=%s user=%s password=%s host=%s" %(RED_USER, RED_PORT, RED_USER, RED_PASSWORD, RED_HOST)
print "Connecting to database\n        ->%s" % (conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

# Update the redshift table with the new results
print "Deleting old table Google_raw_2"
cursor.execute("drop table if exists Google_raw_2;")
print "Creating new table \n Google_raw_20"
cursor.execute("CREATE table Google_raw_20( order_number varchar(50), order_charged_date varchar(15), order_charged_ts int, financial_status varchar(25), device_model varchar(50), product_title varchar(150), product_id varchar(200), product_type varchar(100), SKU varchar(200), currency varchar(50), Price varchar(200), taxes varchar(200), charged_amount varchar(200), city varchar(250), state varchar(100), postal_code varchar(100), country varchar(20) );")
print "Copying Google data from S3 to  \n Google_raw_2 "
cursor.execute("COPY Google_raw_20  FROM 's3://bibusuu/Google_sales_reports/'  CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' IGNOREHEADER 1 csv;" %(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY))
print "Deleting old table Google_raw"
cursor.execute("drop table if exists Google_raw;")
print "Aggregating and Cleaning Google_raw_20"
cursor.execute("create table Google_raw as select order_charged_date as purchase_date, split_part(order_number, '..',1) as order_number, case when split_part(order_number, '..',2) != '' then (split_part(order_number, '..',2)::INTEGER +1 ) else 0 end  as recurring, financial_status as status, product_title as model, product_id as app_id, product_type as subscription, google.currency as currency, replace(price,',','')::float as price, replace(taxes,',','')::float as taxes, replace(charged_amount,',','')::float as charged_amount, replace(charged_amount,',','')::float/ber.rate as eur_amount, country as country from  google_raw_20 google left join bs_exchange_rates ber  on date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) = google.order_charged_date and ber.currency = google.currency ; ")
print 'Deleting Staging table'
cursor.execute("drop table if exists google_raw_20;")
conn.commit()
conn.close()