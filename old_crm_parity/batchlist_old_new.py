from flask import Flask,  request
import pyodbc
import datetime
from flask_restful import Resource, Api
import json
import re
import pandas as pd
import numpy as np
import psycopg2

app = Flask(__name__)
api = Api(app)  

#//---------logging file code -----//
import os
import logging
log_filename = "schedulerlogs/batchlist_old_to_new.txt"
def remove_if_exists(filename):
  if os.path.exists(filename):
    os.remove(filename) 

logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(log_filename)

#//---------logging file code -----//


#-------------------------------------------- connection string for pgadmin -----------------------------------------#
condb = psycopg2.connect(dbname='dfvse6iqus1htq', user='Josh', password='pc2c7f3f51d5b8d2f66cff17dff1e08b78252494246c07393e602d3f84f5ef419', host='ec2-34-231-33-80.compute-1.amazonaws.com', port='5432')
cursor = condb.cursor()

#-------------------------------------------- connection string for ms-sql server -----------------------------------------#
conn = pyodbc.connect("Driver={SQL Server};"
                      "Server=SERVER1;"
                      "Database=rascrm;"
                      "uid=rascrm_db;pwd=#@123456Code"
                       )
connection = conn.cursor()



try:

       #fetching data of last 7 days from postgres db of old crm (for our batchlist & giftcardlist table)
       sql = ("SELECT batch_id, batch_date, merchent_name, amount_paid, total_cards, created_at, updated_at FROM card_orders"
              " where batch_date > current_date - interval '7 days'")

       cursor.execute(sql)
       #fetching giftcardl
       data = cursor.fetchall() 
       #print(data)


       #inserting data in our giftcardlist & batchlist table
       for i in data:
              batch_orderNo  = i[0].strip()
              print('batch_orderNo', batch_orderNo)
              batch_orderNo  = batch_orderNo.replace(" ", "")
              print('batch_orderNo', batch_orderNo)
              batch_orderNo = batch_orderNo.replace("'", "")
              print('batch_orderNo', batch_orderNo)
              batch_orderNo = batch_orderNo.replace("?", "")
              print('batch_orderNo', batch_orderNo)
              batch_date  = str(i[1])
              print('batch_date', batch_date)
              vendor  = None if i[2] == "" else (None if i[2] == None else i[2].strip())
              print('vendor', vendor)
              batch_amount_paid  = 0 if (i[3] == "" or i[3] == None) else float(i[3])
              print('batch_amount_paid', batch_amount_paid)
              no_of_cards  = 0 if (i[4] == "" or i[4] == None) else int(i[4])
              print('no_of_cards', no_of_cards)
              batch_created_at  = str(i[5])[0:19]
              print('batch_created_at', batch_created_at)
              batch_updated_at  = str(i[6])[0:19]
              print('batch_updated_at', batch_updated_at)

       
              #if batch order no and batch date is not none 
              if i[0] != None and i[1] != None and vendor != 'Monheit':
                     batch_date = datetime.datetime.strptime(batch_date, '%Y-%m-%d')
                     print(batch_date)
                     batch_created_at = datetime.datetime.strptime(batch_created_at, '%Y-%m-%d %H:%M:%S')
                     print(batch_created_at)
                     print(type(batch_created_at))
                     batch_updated_at = datetime.datetime.strptime(batch_updated_at, '%Y-%m-%d %H:%M:%S')
                     print(batch_updated_at)
                     print(type(batch_updated_at))

                     sql = "If Not Exists(select id from [dbo].[batchlist] where orderNo= ?) BEGIN INSERT INTO [dbo].[batchlist] (userid, companyid, vendor, orderNo, date_of_purchase, Date_creation, Date_modify, no_of_cards, emailUsed, registeredmail_id, total, parity) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) END"
                     val = [batch_orderNo, 2, 2, vendor, batch_orderNo, batch_date, batch_created_at, batch_updated_at, no_of_cards, 'info@doublegame.net', 'admin@gmail.com', batch_amount_paid, 1]
                     connection.execute(sql, val)
                     connection.commit() 
                     
                   

except Exception as e:
        print(str(e))
        logger.info(str(e))



    
    

  

