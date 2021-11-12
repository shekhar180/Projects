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

    #fetching giftcardlist & batchlistlst 7 days data for ms-sql db
    sql=("SELECT giftcardlist.gift_card_no, giftcardlist.gift_card_pin, giftcardlist.gift_card_value, giftcardlist.amount_paid,"
        "batchlist.vendor, batchlist.orderNo, giftcardlist.balance, giftcardlist.status,  CONVERT (varchar(10),last_used ,110) 'last_used'"
        " FROM [dbo].[giftcardlist] LEFT JOIN [dbo].[giftCardTransactionList]" 
        " ON [dbo].[giftcardlist].gift_card_no = [dbo].[giftCardTransactionList].giftCard_number" 
        " inner JOIN [dbo].[batchlist] ON [dbo].[giftcardlist].batch_id = [dbo].[batchlist].id"
        " WHERE giftcardlist.Date_creation >= DATEADD(day,-7, GETDATE())"
        " group by [dbo].[giftcardlist].gift_card_no, giftcardlist.retailler, gift_card_pin, gift_card_value," 
        " giftcardlist.amount_paid, giftcardlist.status, batchlist.vendor, [balance], [last_used], updated_at, batchlist.orderNo"
        )
    #execute sql query
    connection.execute(sql)
    #fetching 
    data = connection.fetchall()
    #print(data)

    giftcard_column = ['giftcard_number', 'giftcard_pin', 'giftcard_value', 'amount_paid', 'vender_name', 'batch_id' ,'number',
            'balance', 'status', 'sold_to_paid', 'comment', 'sold_to_id', 'giftcard_checked', 'giftcard_checked_date',
            'is_refunded', 'for_recent_use', 'bad_card_amount', 'case_created', 'sold_to_order_number', 'amount_refunded', 
            'is_loss_amount' , 'is_sent', 'sent_checked_date', 'filling_id'
            ]

    list_of_cards = []
    for i in data:
        giftcard_no = None if i.gift_card_no == "" else (None if i.gift_card_no == None else i.gift_card_no.replace(" ",""))
        giftcarddict = {
                "giftcard_number":giftcard_no,
                "giftcard_pin":i.gift_card_pin, 
                "giftcard_value":i.gift_card_value,
                "amount_paid":i.amount_paid,
                "vender_name":i.vendor,
                "batch_id":i.orderNo, 
                "number":None,
                "balance":i.balance,
                "status":i.status,
                "sold_to_paid":None, 
                "comment":None,
                "sold_to_id":None,
                "giftcard_checked":None,
                "giftcard_checked_date":i.last_used, 
                "is_refunded":None,
                "for_recent_use":None,
                "bad_card_amount":None,
                "case_created":None, 
                "sold_to_order_number":None,
                "amount_refunded":None,
                "is_loss_amount":None,
                "is_sent":None, 
                "sent_checked_date":None,
                "filling_id":None,
        }

        sql = "select exists(select giftcard_number FROM public.giftcards where giftcard_number = '{}')".format(i.gift_card_no)
        # print(i.order_number)
        cursor.execute(sql)
        #fetching giftcardl
        data = cursor.fetchall()
        print(data[0][0])
        if data[0][0] == False:
            list_of_cards.append(giftcarddict)
        else:
            continue

    #creating a dataframe of sql records
    df = pd.DataFrame(list_of_cards, columns = giftcard_column)
    #print(df)
    df.to_excel('giftcardimport.xlsx', index=False)


except Exception as e:
    print(str(e))
    logger.info(str(e))




