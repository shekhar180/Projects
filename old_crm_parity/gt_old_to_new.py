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
log_filename = "schedulerlogs/gt_old_to_new.txt"
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

        #fetching data of last 7 days from postgres db of old crm (for our giftcard transaction table)
        sql = ("SELECT orders.vendor, orders.status, orders.total, orders.purchase_date, used_cards.created_at, used_cards.updated_at,"
                "used_cards.card_id, orders.order_number, used_cards.amount"
                " FROM public.used_cards INNER JOIN public.orders"
                " ON used_cards.order_id = orders.id"
                " where used_cards.updated_at > current_date - interval '30 days'")

        cursor.execute(sql)
        #fetching giftcardl
        data = cursor.fetchall() 
        #print(data)


        # #inserting data in our giftcard transaction table
        for i in data:
                retailer        = None if i[0] == "" else (None if i[0] == None else i[0].strip())
                #print("retailer", retailer)
                status          = 'Ordered' if i[1] == "" else ('Ordered' if i[1] == None else ('Refunded' if i[1].strip() == 'refunded' else('Cancelled' if i[1].strip() == 'cancelled' else i[1].strip())))
                #print("status", status)
                total           = None if (i[2] == "" or i[2] == None) else float(i[2])
                #print("total", total)
                #print(type(total))
                order_purchase_date =  str(i[3])
                #print("order_purchase_date", order_purchase_date)
                created         = str(i[4])[0:19]
                #print("created", created)
                updated         = str(i[5])[0:19]
                #print("updated", updated)
                giftcard_number = i[6].strip()
                giftcard_number = None if giftcard_number == "" else (None if giftcard_number == None else giftcard_number.replace(" ",""))
                #print("giftcard_number", giftcard_number)
                orderNumber = i[7].strip()
                orderNumber = None if orderNumber == "" else (None if orderNumber == None else orderNumber.replace(" ",""))
                print("orderNumber", orderNumber)
                amount           = 0 if (i[8] == "" or i[8] == None) else float(i[8])
                #print("amount", amount)
                #print(type(amount))
                cancel_count = 1 if status == 'Cancelled' else 0
        
                #giftcard number & order number not exist
                if giftcard_number != None and orderNumber != None:

                        order_purchase_date = datetime.datetime.strptime(order_purchase_date, '%Y-%m-%d')
                        #print(order_purchase_date)
                        #print(type(order_purchase_date))
                        created_at = datetime.datetime.strptime(created, '%Y-%m-%d %H:%M:%S')
                        #print(created_at)
                        #print(type(created_at))
                        updated_at = datetime.datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')
                        #print(updated_at)
                        #print(type(updated_at))


                        sql = "If Not Exists(select id from [dbo].[giftCardTransactionList] where giftCard_number= ? and orderNumber= ? and order_purchase_total = ?) BEGIN INSERT INTO [dbo].[giftCardTransactionList] (giftCard_number, retailler, userid, companyid,  amount, orderNumber, order_purchase_total, order_purchase_status, order_purchase_date, Date_creation, Date_modify, parity, cancel_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) END"
                        val = [giftcard_number, orderNumber, total, giftcard_number, retailer, 2, 2, amount, orderNumber, total, status, order_purchase_date, created_at, updated_at, 1, cancel_count]
                        connection.execute(sql, val)
                        connection.commit() 

                        sql = "SELECT id from [dbo].[giftcardlist] where gift_card_no = ?"
                        connection.execute(sql, giftcard_number)

                        data = connection.fetchall()
                        if len(data) != 0:
                                giftcardlist_id = data[0][0]
                                print('giftcardlist_id', giftcardlist_id)

                                # #sql query for updating data on the database --> balance and status
                                sql = "UPDATE [dbo].[giftCardTransactionList] SET [giftcardlist_id] = ? FROM [dbo].[giftCardTransactionList] WHERE giftCard_number = ?"
                                val = (giftcardlist_id, giftcard_number)
                                connection.execute(sql, val) 
                                connection.commit()


except Exception as e:
        print(str(e))
        logger.info(str(e))






