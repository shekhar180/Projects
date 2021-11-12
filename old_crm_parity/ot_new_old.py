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
log_filename = "schedulerlogs/ot_new_old.txt"
def remove_if_exists(filename):
  if os.path.exists(filename):
    os.remove(filename)

logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(log_filename)

#//---------logging file code end -----//

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
   #fetching order-tracking data from ms-sql db of last 7 days
    sql=("SELECT [orderedDate], [Subtotal], [tax_cost], [shipping_cost], [vendor], [retailler], [tracking_number], [order_number], [emailUsed], [Discount_cost], [tracking_status], [cart_total], [tax_refunded], [shipping_address], [creditCard_number], [amount] FROM [rascrm].[dbo].[ordertracking] left join creditCardTransactionList on creditCardTransactionList.orderNumber = ordertracking.order_number"
        " WHERE ordertracking.Date_creation >= DATEADD(day,-7, GETDATE())")
    #execute sql query
    connection.execute(sql)
    #fetching 
    data = connection.fetchall()
    #print(data)
    list_of_order = []


    for i in data:
        order_number = None if i.order_number == "" else (None if i.order_number == None else i.order_number.replace(" ",""))
        emailUsed = None if i.emailUsed == "" else (None if i.emailUsed == None else i.emailUsed.replace(" ",""))

        #these affliates id exist in DG database
        affiliate_id = 1 if i.vendor == 'Befrugal' else (2 if i.vendor == 'ebates' else (3 if i.vendor == "Mrrebates" else (5 if i.vendor == "Upromise" else(7 if i.vendor == "iconsumer" else(8 if i.vendor == "TopCashBack" else None)))))

        #check email id exist in DG database
        sql = "select id FROM public.order_emails where email = '{}'".format(emailUsed)
        # print(i.order_number)
        cursor.execute(sql)
        #fetching data
        emailData = cursor.fetchall()
        if len(emailData) !=0:
            emailUsed_id = emailData[0][0]
        else:
            emailUsed_id = None

        #check shipping address exist in DG database
        sql = "select id FROM public.order_addresses WHERE address = '{}'".format(i.shipping_address)
        # print(i.order_number)
        cursor.execute(sql)
        #fetching data
        addressData = cursor.fetchall()
        if len(addressData) !=0:
            address_id = addressData[0][0]
        else:
            address_id = None

        #check credit card exist in DG database
        sql = "SELECT id FROM public.ccs WHERE cc_used = '{}'".format(i.creditCard_number)
        # print(i.order_number)
        cursor.execute(sql)
        #fetching data
        creditcardData = cursor.fetchall()
        if len(creditcardData) !=0:
            creditcard_id = creditcardData[0][0]
        else:
            creditcard_id = None

        orderdict = {
                    "purchase_date":i.orderedDate,
                    "order_id":None, 
                    "received":None,
                    "total":i.Subtotal,
                    "tax":i.tax_cost,
                    "shipping":i.shipping_cost,
                    "vendor":i.retailler,
                    "tracking_number":i.tracking_number, 
                    "physical_card":None,
                    "affiliate_id":affiliate_id,
                    "order_number":order_number,
                    "tax_refunded":i.tax_refunded,
                    "order_email_id":emailUsed_id,
                    "discount_percentage":None, 
                    "order_address_id":address_id,
                    "canceled":None,
                    "refunded":None,
                    "office_expense":None,
                    "returned":None,
                    "order_phone_number_id":None, 
                    "cc_id":creditcard_id,
                    "percentage_discount":i.Discount_cost,
                    "coupon":None,
                    "points_rewards":None,
                    "status":i.tracking_status,
                    "cc_amount_used":i.amount,
                    "flag":None,
                    "final_total":i.cart_total,
                    "free_gift_decimal":None 
                    }

        sql = "select exists(select order_number FROM public.orders where order_number = '{}')".format(order_number)
        # print(i.order_number)
        cursor.execute(sql)
        #fetching giftcardl
        data = cursor.fetchall()
        print(data)
        #print(data[0][0])
        #print(type(data[0][0]))
        if data[0][0] == False:
            list_of_order.append(orderdict)
        else:
            continue

    importorder_column = ['purchase_date', 'order_id', 'received', 'total', 'tax', 'shipping', 'vendor', 'tracking_number', 'physical_card' , 'affiliate_id',  'order_number', 'tax_refunded', 'order_email_id',	'discount_percentage', 'order_address_id', 'canceled',  'refunded' , 'office_expense', 'returned' ,	'order_phone_number_id', 'cc_id' , 'percentage_discount' , 'coupon', 'points_rewards', 'status', 'cc_amount_used' ,	'flag',	'final_total', 'free_gift_decimal']

    #creating a dataframe of sql records
    df = pd.DataFrame(list_of_order, columns = importorder_column)
    #print(df)

    df.to_excel('importOrders.xlsx', index=False)




except Exception as e:
    print(str(e))
    logger.info(str(e))

        


