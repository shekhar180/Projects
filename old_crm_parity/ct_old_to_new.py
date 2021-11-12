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
log_filename = "schedulerlogs/ct_old_to_new.txt"
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
        #fetching data of last 7 days from postgres db of old crm (for our creditcard transaction table)
        sql = ("SELECT orders.order_number, orders.cc_amount_used, ccs.cc_used, ccs.created_at, ccs.updated_at"
                " FROM public.orders Inner JOIN public.ccs ON orders.cc_id = ccs.id"
                " where ccs.created_at > current_date - interval '30 days'")

        cursor.execute(sql)
        #fetching giftcardl
        data = cursor.fetchall()
        #print(data)


        #inserting data in our creditcard transaction table
        for i in data:
                orderNumber      = i[0].strip()
                orderNumber      = None if orderNumber == "" else (None if orderNumber == None else orderNumber.replace(" ",""))
                print("orderNumber", orderNumber)
                amount           = None if (i[1] == "" or i[1] == None) else float(i[1])
                print("amount", amount)
                creditCard_number =   i[2].strip()
                creditCard_number =  None if creditCard_number == "" else (None if creditCard_number == None else creditCard_number.replace(" ",""))
                print("creditCard_number", creditCard_number)
                created         = str(i[3])[0:19]
                print("created", created)
                updated         = str(i[4])[0:19]
                print("updated", updated) 

                #if credicard number & ordertracking number is not none
                if  orderNumber != None and creditCard_number != None:
                        created_at = datetime.datetime.strptime(created, '%Y-%m-%d %H:%M:%S')
                        print(created_at)
                        #print(type(created_at))
                        updated_at = datetime.datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')
                        print(updated_at)
                        #print(type(updated_at))

                        sql = "If Not Exists(select id from [dbo].[creditCardTransactionList] where orderNumber= ? and creditCard_number= ?) BEGIN INSERT INTO [dbo].[creditCardTransactionList] (creditCard_number, orderNumber, userid, companyid, amount, Date_creation, Date_modify, parity) VALUES (?,?,?,?,?,?,?,?) END"
                        val = [orderNumber, creditCard_number, creditCard_number, orderNumber, 2, 2, amount, created_at, updated_at, 1]
                        connection.execute(sql, val)
                        connection.commit()  

except Exception as e:
        print(str(e))
        logger.info(str(e))



