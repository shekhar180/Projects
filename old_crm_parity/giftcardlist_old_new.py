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
log_filename = "schedulerlogs/giftcardlist_old_to_new.txt"
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
    sql = ("SELECT giftcard_number, giftcard_pin, giftcard_value, giftcards.amount_paid, vender_name, giftcards.batch_id," 
    "giftcards.created_at, giftcards.updated_at, balance, status,"
    "batch_date FROM public.giftcards inner join card_orders ON giftcards.batch_id = card_orders.batch_id"
    " where batch_date > current_date - interval '7 days'") 


    cursor.execute(sql)
    #fetching giftcardl
    data = cursor.fetchall() 
    #print(data)


    #inserting data in our giftcardlist & batchlist table
    for i in data:
        giftcard_number = i[0].strip()
        print('giftcard_number', giftcard_number)
        giftcard_number = giftcard_number.replace(" ", "")
        #print('giftcard_number', giftcard_number)
        giftcard_number = giftcard_number.replace("'", "")
        #print('giftcard_number', giftcard_number)
        giftcard_number = giftcard_number.replace("?", "")
        #print('giftcard_number', giftcard_number)
        giftcard_pin  = None if i[1] == "" else (None if i[1] == None else i[1])
        #print('giftcard_pin', giftcard_pin)
        giftcard_value  = 0 if (i[2] == "" or i[2] == None) else float(i[2])
        #print('giftcard_value', giftcard_value)
        giftcard_amount_paid  = 0 if (i[3] == "" or i[3] == None) else float(i[3]) 
        #print('giftcard_amount_paid', giftcard_amount_paid)
        retailler  = None if i[4] == "" else (None if i[4] == None else i[4].strip())
        #print('retailler', retailler)
        batch_orderNo  = i[5].strip()
        #print('batch_orderNo', batch_orderNo)
        batch_orderNo  = batch_orderNo.replace(" ", "")
        #print('batch_orderNo', batch_orderNo)
        batch_orderNo = batch_orderNo.replace("'", "")
        #print('batch_orderNo', batch_orderNo)
        batch_orderNo = batch_orderNo.replace("?", "")
        giftcards_creation  = str(i[6])[0:19]
        #print('giftcards_creation', giftcards_creation)
        giftcards_updation  = giftcards_creation if (i[7] == None) else (str(i[7])[0:19])
        #print('giftcards_updation', giftcards_updation)
        balance  = 0 if (i[8] == "" or i[8] == None) else float(i[8])
        #print('balance', balance)
        status  =  None if i[9] == "" else (None if i[9] == None else i[9])
        #print('status', status)
        batch_date  = str(i[10])
        #print('batch_date', batch_date)

        if  giftcard_pin != None: 
            giftcard_pin = giftcard_pin.replace(" ", "")
            #print('giftcard_pin', giftcard_pin)
            giftcard_pin = giftcard_pin.replace("'", "")
            #print('giftcard_pin', giftcard_pin)
            giftcard_pin = giftcard_pin.replace("?", "")
            #print('giftcard_pin', giftcard_pin)



        #batch date & giftcard no is not none
        if i[10] != None and i[0] != None:
            batch_date = datetime.datetime.strptime(batch_date, '%Y-%m-%d')
            #print(batch_date)
            giftcards_creation = datetime.datetime.strptime(giftcards_creation, '%Y-%m-%d %H:%M:%S')
            #print(giftcards_creation)
            #print(type(giftcards_creation))
            giftcards_updation = datetime.datetime.strptime(giftcards_updation, '%Y-%m-%d %H:%M:%S')
            #print(giftcards_updation)
            #print(type(giftcards_updation))
        

            sql = "select id, vendor, orderNo from [dbo].[batchlist] where orderNo= ?"
            connection.execute(sql, batch_orderNo)
            batch_data = connection.fetchall()
            #print(batch_data)

            if len(batch_data) != 0:
                batch_id  = batch_data[0][0]
                print('batch_id', batch_id)
                vendor  = batch_data[0][1]
                print('vendor', vendor)
                orderNo  = batch_data[0][2]
                print('orderNo', orderNo)


                status = 'No Balance' if status == 'used in full' else ('Balance' if status == 'used partial' else('Balance' if status == 'new card' else('Bad' if status == 'bad card' else status)))


                sql = ("If Not Exists(select id from [dbo].[giftcardlist] where gift_card_no = ?)" 
                        " BEGIN INSERT INTO [dbo].[giftcardlist] (userid, companyid,"
                        "gift_card_no, gift_card_pin, gift_card_value, amount_paid, retailler, batch_id, batch_vendor, batch_orderNo, updated_at,"
                        "balance, status, Date_creation, Date_modify, parity, batch_modify_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) END")
                val = [giftcard_number, 2, 2, giftcard_number, giftcard_pin, giftcard_value, giftcard_amount_paid, retailler, 
                    batch_id, vendor, orderNo, batch_date, balance, status, giftcards_creation, giftcards_updation, 1, datetime.datetime.now()]
                connection.execute(sql, val)
                connection.commit() 



                sql = ("If Not Exists(select id from [dbo].[checkingqueue] where gift_card_no = ?)" 
                        " BEGIN INSERT INTO [dbo].[checkingqueue] (userid, companyid,"
                        "gift_card_no, gift_card_pin, gift_card_value, amount_paid, retailler, batch_id, batch_vendor, batch_orderNo, updated_at,"
                        "balance, status, Date_creation, Date_modify, parity) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) END")
                val = [giftcard_number, 2, 2, giftcard_number, giftcard_pin, giftcard_value, giftcard_amount_paid, retailler, 
                    batch_id, vendor, orderNo, batch_date, balance, status, giftcards_creation, giftcards_updation,1]
                connection.execute(sql, val)
                connection.commit() 

        else:
            continue

        
    

except Exception as e:
        print(str(e))
        logger.info(str(e))







