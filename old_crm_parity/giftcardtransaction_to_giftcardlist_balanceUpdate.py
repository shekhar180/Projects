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
log_filename = "schedulerlogs/giftcardtransaction_to_giftcardlist_balanceUpdate.txt"
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
    #getting giftcardno, value & value from giftcardlist if same giftcard present in transacrion list
    sql = "SELECT giftcardlist.gift_card_no, gift_card_value, balance FROM [dbo].[giftcardlist] LEFT JOIN [dbo].[giftCardTransactionList] ON [dbo].[giftcardlist].gift_card_no = [dbo].[giftCardTransactionList].giftCard_number where giftCardTransactionList.Date_creation >= DATEADD(day,-1, GETDATE())"
    #where cast (giftCardTransactionList.Date_creation as date)  = '2020-12-05'
    connection.execute(sql) 
    trnsaction_cards = connection.fetchall()   
    print(trnsaction_cards) 

    for gcard in trnsaction_cards:
        gno     = gcard.gift_card_no.strip()
        gno     = gno.replace(" ", '')
        #print('giftcard_number', gno)
        gvalue  = gcard.gift_card_value
        #print('giftcard_value', gvalue)
        gbalance  = gcard.balance
        #print('giftcardBalance', gbalance)


        #getting giftcard transaction purchased amount from giftcard transaction table
        sql = "SELECT ROUND(SUM(amount), 2) FROM [dbo].[giftCardTransactionList] WHERE [dbo].[giftCardTransactionList].giftCard_number=?"
        connection.execute(sql, gno) 
        cards = connection.fetchall()   
        print(cards) 

        if len(cards) !=0:
            #sum of  giftcard purchased amount
            transaction_amount = cards[0][0]

            #giftcard value  - giftcard transaction purchased amount
            balance = round(gvalue - transaction_amount,2)

            #print(balance)
            status = 'Balance Off Negative' if balance < 0 else('Balance' if balance > 0 else 'No Balance')
            print(status)

            #balance is not equal to giftcard balance
            if balance != gbalance:
                #print('balance', balance)
                #print('giftcardbalance', gbalance)


                #sql query for updating data on the database --> balance and status
                #sql = "UPDATE [dbo].[giftcardlist] SET [balance] = ?, [status] = ?, [Date_modify] = ?, gt = ? WHERE [dbo].[giftcardlist].gift_card_no=? and (check_balance_status is NULL or last_used <= DATEADD(day,-1, GETDATE()))"
                sql = "UPDATE [dbo].[giftcardlist] SET [balance] = ?, [status] = ?, [Date_modify] = ?, gt = ? FROM [dbo].[giftcardlist] LEFT JOIN [dbo].[giftCardTransactionList] ON [dbo].[giftcardlist].gift_card_no = [dbo].[giftCardTransactionList].giftCard_number WHERE [dbo].[giftcardlist].gift_card_no=? and (giftcardlist.last_used is NULL or giftcardlist.last_used < giftCardTransactionList.Date_modify)"
                val = (float(balance), status, datetime.datetime.now(), 1, gno)
                connection.execute(sql, val) 
                connection.commit()

    sql = ("SELECT CONVERT(varchar, max(order_purchase_date),121) as order_purchase_date, giftCard_number, emailUsed FROM [rascrm].[dbo].[giftCardTransactionList]" 
        " inner JOIN [dbo].[ordertracking] ON [dbo].[ordertracking].order_number = [dbo].[giftCardTransactionList].orderNumber" 
        " where giftCardTransactionList.order_purchase_date >= DATEADD(day,-1, GETDATE())"
        " group by giftCard_number, emailUsed order by order_purchase_date desc") 
    connection.execute(sql) 
    data = connection.fetchall()
    for i in data:
        if i.emailUsed != None:
            #print(i.order_purchase_date, i.giftCard_number, i.emailUsed)
            sql = "UPDATE [dbo].[giftcardlist] SET extension_last_use_date = ?, extension_last_use_email = ? where gift_card_no =?"
            val =  [i.order_purchase_date, i.emailUsed, i.giftCard_number] 
            connection.execute(sql, val) 
            connection.commit()


except Exception as e:
        print(str(e))
        logger.info(str(e))
