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
log_filename = "schedulerlogs/tracking_sku_old_to_new.txt"
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
        #fetching data of last 7 days from postgres db of old crm (for our ordertracking table)
        sql = ("SELECT orders.order_id, purchase_date, order_number, order_items.sku, orders.vendor, order_item_associations.price, order_item_associations.quantity, orders.total, orders.tax, orders.shipping,"
                "discount_percentage, final_total, orders.status, order_item_associations.created_at, order_item_associations.updated_at,"
                "order_addresses.address, order_emails.email, affiliates.name, order_items.asin, order_items.upc, order_items.name,"
                "orders.tax_refunded, orders.received"
                " FROM public.order_item_associations LEFT JOIN orders ON order_item_associations.order_id = orders.id"
                " LEFT JOIN order_addresses ON orders.order_address_id = order_addresses.id"
                " LEFT JOIN order_emails ON orders.order_email_id = order_emails.id" 
                " LEFT JOIN affiliates ON orders.affiliate_id = affiliates.id" 
                " LEFT JOIN order_items ON order_item_associations.order_item_id = order_items.id"
                " where orders.created_at > current_date - interval '30 days'")       
        cursor.execute(sql)
        #fetching giftcardl
        data = cursor.fetchall() 
        #print(data)

        #column = ["purchase_date", "order_number", "sku", "vendor", "price", "quantity", "total", "tax", "shipping", "discount_percentage",	"final_total", "status", "created_at", "updated_at", "address", "email", "name", "asin" , "upc"	, "title"]	

        #inserting data in our ordertracking table
        for i in data:

                ordered_date =  str(i[1])
                print("ordered_date", ordered_date)
                order_number =  i[2].strip()
                print("order_number", order_number)
                order_number =  None if order_number == "" else (None if order_number == None else order_number.replace(" ",""))
                print("order_number", order_number)
                item_sku     =  i[3]
                print("item_sku", item_sku)
                item_sku     =  None if item_sku == "" else (None if item_sku == None else item_sku.strip())
                print("item_sku", item_sku)
                item_sku     = None if item_sku == None else item_sku.replace(" ","")
                print("item_sku", item_sku)
                retailler       = None if i[4] == "" else (None if i[4] == None else i[4].strip())
                print("retailler", retailler)
                price        = None if (i[5] == "" or i[5] == None) else float(i[5])
                print("price", price)
                qty_cart     = None if (i[6] == "" or i[6] == None) else int(i[6])
                print("qty_cart", qty_cart)
                Subtotal     = None if (i[7] == "" or i[7] == None) else float(i[7])
                print("Subtotal", Subtotal)
                tax_cost     = None if (i[8] == "" or i[8] == None) else float(i[8])
                print("tax_cost", tax_cost)
                shipping_cost = None if (i[9] == "" or i[9] == None) else float(i[9])
                print("shipping_cost", shipping_cost)
                Discount_cost = None if (i[10] == "" or i[10] == None) else float(i[10])
                print("Discount_cost", Discount_cost)
                cart_total    = None if (i[11] == "" or i[11] == None) else float(i[11])
                print("cart_total", cart_total)
                tracking_status = 'Ordered' if i[12] == "" else ('Ordered' if i[12] == None else ('Refunded' if i[12].strip() == 'refunded' else i[12].strip()))
                print("tracking_status", tracking_status)
                created         = str(i[13])[0:19]
                print("created", created)
                updated         = str(i[14])[0:19]
                print("updated", updated)
                shipping_address = None if i[15] == "" else (None if i[15] == None else i[15])
                print("shipping_address", shipping_address)
                emailUsed       = None if i[16] == "" else (None if i[16] == None else i[16].strip())
                print("emailUsed", emailUsed)
                vendor       = None if i[17] == "" else (None if i[17] == None else i[17].strip())
                print("vendor", vendor)
                asin            = None if i[18] == "" else (None if i[18] == None else i[18])
                print("asin", asin)
                upc             = None if i[19] == "" else (None if i[19] == None else i[19])
                print("upc", upc)
                title           = None if i[20] == "" else (None if i[20] == None else i[20])
                print("title", title)
                tax_refunded    = None if i[21] == "" else (None if i[21] == None else i[21])
                print("tax_refunded", tax_refunded)
                received        = None if i[22] == "" else (None if i[22] == None else i[22])
                print("received", received)


                tracking_status = 'Received' if received == True else tracking_status

                if i[1] != None and order_number != None:
                        ordered_date = ordered_date +' '+ str(datetime.datetime.now().time())
                        orderedDate = str(datetime.datetime.strptime(ordered_date , '%Y-%m-%d %H:%M:%S.%f'))[:-3]
                        # print(orderedDate)
                        # print(type(orderedDate))
                        created_at = datetime.datetime.strptime(created, '%Y-%m-%d %H:%M:%S')
                        # print(created_at)
                        # print(type(created_at))
                        updated_at = datetime.datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')
                        # print(updated_at)
                        # print(type(updated_at))


                        sql = "UPDATE [dbo].[ordertracking] SET userid = ? , companyid = ?, vendor = ?, retailler = ?, emailUsed = ?, item_sku = ?, price = ?, qty_cart = ?, cart_total = ?, order_number = ?, tracking_status = ?, shipping_address = ?, title = ?, asin = ?, upc = ?, shipping_cost = ?, Discount_cost = ?, tax_cost = ?, Subtotal = ?, Date_creation = ?, Date_modify = ?, orderedDate = ?, tax_refunded = ?, oldcrm_id = ? where order_number= ? and oldcrm_id = ?"
                        val = [2, 2, vendor, retailler, emailUsed, item_sku, price, qty_cart, cart_total, order_number, tracking_status, shipping_address, title, asin, upc, shipping_cost, Discount_cost, tax_cost, Subtotal, created_at, updated_at, orderedDate, tax_refunded, 461775, order_number, 452002]
                        connection.execute(sql, val)
                        connection.commit() 
                        
                        
                        sql = "If Not Exists(select id from [dbo].[ordertracking] where order_number= ? and item_sku= ? and retailler = ? and price = ?) BEGIN INSERT INTO [dbo].[ordertracking] (userid, companyid, vendor, retailler, emailUsed, item_sku, price, qty_cart, cart_total, order_number, tracking_status, shipping_address, title, asin, upc, shipping_cost, Discount_cost, tax_cost, Subtotal, Date_creation, Date_modify, orderedDate, tax_refunded,oldcrm_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) END"
                        val = [order_number, item_sku, retailler, price, 2, 2, vendor, retailler, emailUsed, item_sku, price, qty_cart, cart_total, order_number, tracking_status, shipping_address, title, asin, upc, shipping_cost, Discount_cost, tax_cost, Subtotal, created_at, updated_at, orderedDate, tax_refunded, 461775]
                        connection.execute(sql, val)
                        connection.commit() 


except Exception as e:
        print(str(e))
        logger.info(str(e))

        




