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
log_filename = "schedulerlogs/update_total_and_no_of_cards.txt"
def remove_if_exists(filename):
  if os.path.exists(filename):
    os.remove(filename) 

logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(log_filename)

#//---------logging file code -----//


#-------------------------------------------- connection string for ms-sql server -----------------------------------------#
conn = pyodbc.connect("Driver={SQL Server};"
                      "Server=SERVER1;"
                      "Database=rascrm;"
                      "uid=rascrm_db;pwd=#@123456Code"
                       )
connection = conn.cursor()

try: 
    #this script will add in scheduler to run after every 4 hours 


    sql = ("SELECT orderNo, count(giftcardlist.id) as total_cards, sum(giftcardlist.gift_card_value) as total_card_value" 
    " FROM [dbo].[batchlist] left JOIN [dbo].[giftcardlist] ON [dbo].[batchlist].id = [dbo].[giftcardlist].batch_id" 
    " where batch_modify_date >= DATEADD(day,-1, GETDATE()) group by"
    " vendor, orderNo, date_of_purchase, batchlist.id, giftcardlist.batch_id")
    connection.execute(sql)
    #fetching giftcardl
    data = connection.fetchall() 
    #print(data)

    #inserting data in our giftcardlist & batchlist table
    for i in data:
        batch_orderNo  = i[0].strip()
        print('batch_orderNo', batch_orderNo)
        batch_orderNo  = batch_orderNo.replace(" ", "")
        #print('batch_orderNo', batch_orderNo)
        batch_orderNo = batch_orderNo.replace("'", "")
        #print('batch_orderNo', batch_orderNo)
        batch_orderNo = batch_orderNo.replace("?", "")

        total_cards  = 0 if (i[1] == "" or i[1] == None) else int(i[1]) 

        giftcard_amount_paid  = 0 if (i[2] == "" or i[2] == None) else float(i[2]) 
        #print('giftcard_amount_paid', giftcard_amount_paid)


        # #sql query for updating data on the database --> balance and status
        sql = "UPDATE [dbo].[batchlist] SET [no_of_cards] = ?, total = ? FROM [dbo].[batchlist] WHERE orderNo = ?"
        val = (total_cards, giftcard_amount_paid, batch_orderNo)
        connection.execute(sql, val) 
        connection.commit()

    

except Exception as e:
        print(str(e))
