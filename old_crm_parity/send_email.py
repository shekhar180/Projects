

#!/usr/bin/python
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders


#//---------logging file code -----//
import os
import logging
log_filename = "schedulerlogs/email_smtp.txt"
def remove_if_exists(filename):
  if os.path.exists(filename):
    os.remove(filename)

logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(levelname)s %(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(log_filename)

#//---------logging file code end -----//

try:
    username='rewakencode@gmail.com'
    password = '461775code07576'
    send_from = 'rewakencode@gmail.com'
    send_to = 'joshbochner@gmail.com'
    Cc = 'rahul.shukla@techsolvo.com'
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = send_to
    msg['Cc'] = Cc
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = 'parity EF to DG (excel- importorder, giftcardimport)'
    server = smtplib.SMTP('smtp.gmail.com')
    port = '587'

    f = ['importOrders.xlsx', 'giftcardimport.xlsx']
    for file in f:
        fp = open(file, 'rb')
        part = MIMEBase('application','vnd.ms-excel')
        part.set_payload(fp.read())
        fp.close()
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename= file)
        msg.attach(part)

    smtp = smtplib.SMTP('smtp.gmail.com')
    smtp.ehlo()
    smtp.starttls()
    smtp.login(username,password)
    smtp.sendmail(send_from, send_to.split(',') + msg['Cc'].split(','), msg.as_string())
    smtp.quit()

except Exception as e:
    print(str(e))
    logger.info(str(e))
