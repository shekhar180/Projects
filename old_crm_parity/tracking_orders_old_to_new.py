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
log_filename = "schedulerlogs/tracking_orders_old_to_new.txt"
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
    sql = ("SELECT order_id, purchase_date, order_number, vendor, received, total, tax, shipping,  orders.created_at," 
        " orders.updated_at, tax_refunded, status,"
        " discount_percentage, final_total, order_addresses.address, order_emails.email, affiliates.name"
        " FROM public.orders LEFT JOIN order_addresses ON orders.order_address_id = order_addresses.id"
        " LEFT JOIN order_emails ON orders.order_email_id = order_emails.id" 
        " LEFT JOIN affiliates ON orders.affiliate_id = affiliates.id" 
        " where orders.created_at > current_date - interval '30 days'")

    cursor.execute(sql)
    #fetching giftcardl
    data = cursor.fetchall() 
    #print(data)

    #inserting data in our ordertracking table
    for i in data:
            ordered_date =  str(i[1])
            print("ordered_date", ordered_date)
            order_number =   i[2].strip()
            order_number =  None if order_number == "" else (None if order_number == None else order_number.replace(" ",""))
            print("order_number", order_number)
            retailler       = None if i[3] == "" else (None if i[3] == None else i[3].strip())
            print("retailler", retailler)
            received        = None if i[4] == "" else (None if i[4] == None else i[4])
            print("received", received)
            Subtotal     = None if (i[5] == "" or i[5] == None) else float(i[5])
            print("Subtotal", Subtotal)
            tax_cost     = None if (i[6] == "" or i[6] == None) else float(i[6])
            print("tax_cost", tax_cost)
            shipping_cost = None if (i[7] == "" or i[7] == None) else float(i[7])
            print("shipping_cost", shipping_cost)
            created         = str(i[8])[0:19]
            print("created", created)
            updated         = str(i[9])[0:19]
            print("updated", updated)
            tax_refunded    = None if i[10] == "" else (None if i[10] == None else i[10])
            print("tax_refunded", tax_refunded)
            tracking_status = 'Ordered' if i[11] == "" else ('Ordered' if i[11] == None else('Refunded' if i[11].strip() == 'refunded' else i[11].strip()))
            print("tracking_status", tracking_status)
            Discount_cost = None if (i[12] == "" or i[12] == None) else float(i[12])
            print("Discount_cost", Discount_cost)
            cart_total    = None if (i[13] == "" or i[13] == None) else float(i[13])
            print("cart_total", cart_total)
            shipping_address = None if i[14] == "" else (None if i[14] == None else i[14])
            print("shipping_address", shipping_address)
            emailUsed       = None if i[15] == "" else (None if i[15] == None else i[15].strip())
            print("emailUsed", emailUsed)
            vendor       = None if i[16] == "" else (None if i[16] == None else i[16].strip())
            print("vendor", vendor)
        
            tracking_status = 'Received' if received == True else tracking_status


            #if ordered date & order no not exist
            if i[1] != None and order_number != None:
            
                    orderedDate  = datetime.datetime.strptime(ordered_date, '%Y-%m-%d')
                    ordered_date = ordered_date +' '+ str(datetime.datetime.now().time())
                    orderedDate  = str(datetime.datetime.strptime(ordered_date , '%Y-%m-%d %H:%M:%S.%f'))[:-3]
                    #print(orderedDate)
                    #print(type(orderedDate))
                    created_at = datetime.datetime.strptime(created, '%Y-%m-%d %H:%M:%S')
                    #print(created_at)
                    #print(type(created_at))
                    updated_at = datetime.datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')
                    #print(updated_at)
                    #print(type(updated_at))

                    
                    sql = "If Not Exists(select id from [dbo].[ordertracking] where order_number= ?) BEGIN INSERT INTO [dbo].[ordertracking] (userid, companyid, vendor, retailler, emailUsed, cart_total, order_number, tracking_status, shipping_address, shipping_cost, Discount_cost, tax_cost, Subtotal, Date_creation, Date_modify, orderedDate, tax_refunded, oldcrm_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) END"
                    val = [order_number, 2, 2, vendor, retailler, emailUsed, cart_total, order_number, tracking_status, shipping_address, shipping_cost, Discount_cost, tax_cost, Subtotal, created_at, updated_at, orderedDate, tax_refunded, 452002]
                    connection.execute(sql, val)
                    connection.commit() 

                 
except Exception as e:
        print(str(e))
        logger.info(str(e))






