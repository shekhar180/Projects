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
log_filename = "schedulerlogs/batchtransaction_old_to_new.txt"
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

    sql = "select id, orderNo, Date_creation, Date_modify from [dbo].[batchlist] where date_of_purchase >= DATEADD(day,-7, GETDATE())"
    connection.execute(sql)
    batch_data = connection.fetchall()
    #print(batch_data)

    for i in batch_data:
      batch_id = i.id
      print(batch_id)
      print(type(batch_id))
      print('batch_id', batch_id)
      batch_orderNo = i.orderNo.strip()
      print('batch_orderNo', batch_orderNo)
      batch_orderNo = batch_orderNo.replace(" ","")
      print('batch_orderNo', batch_orderNo)
      batch_orderNo = batch_orderNo.replace("'","")
      print('batch_orderNo', batch_orderNo)
      Date_creation = i.Date_creation
      print('Date_creation', Date_creation)
      Date_modify = i.Date_modify
      print('Date_modify', Date_modify)

      sql = "If Not Exists(select id from [dbo].[batchlistTransaction] where orderNo= ?) BEGIN INSERT INTO [dbo].[batchlistTransaction] (batch_id, orderNo, Date_creation, Date_modify) VALUES (?,?,?,?) END"
      val = [batch_orderNo, batch_id, batch_orderNo, Date_creation, Date_modify] 
      connection.execute(sql, val)
      connection.commit() 

except Exception as e:
        print(str(e))
        logger.info(str(e))

