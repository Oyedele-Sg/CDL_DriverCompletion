from flask import Flask, render_template, request, send_file
from flask_mail import Mail, Message
from flask_sqlalchemy import SQLAlchemy
from pymysql import Time
from sqlalchemy import exc, cast, Date, Time
from sqlalchemy.ext.automap import automap_base 
from sqlalchemy.orm import Session
from datetime import date, datetime, timedelta, timezone
from smtplib import SMTPException
from dotenv import load_dotenv
from logging.config import DEFAULT_LOGGING_CONFIG_PORT, dictConfig

import xlsxwriter
import os
import csv
import smtplib


dictConfig(
            {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
            'default': {
                        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
                       },
            'simpleformatter' : {
                        'format' : '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            }
    },
    'handlers':
    {
        'custom_handler': {
            'class' : 'logging.FileHandler',
            'formatter': 'default',
            'filename' : 'drivercompletion.log',
            'level': 'WARN',
        }
    },
    'root': {
        'level': 'WARN',
        'handlers': ['custom_handler']
    },
})

os.environ["WERKZEUG_RUN_MAIN"] = "true"
load_dotenv()

app = Flask(__name__)

mail = Mail(app)

# Database 
driver = 'ODBC Driver 17 for SQL Server'
user_name = os.getenv("USER_NAME")
server = os.getenv("SERVER_NAME")
db_name = os.getenv("DB_NAME")
password = os.getenv("DB_PASS")
app.config["SQLALCHEMY_DATABASE_URI"] = f"mssql+pyodbc://{user_name}:{password}@{server}/{db_name}?driver={driver}"
# app.config["SQLALCHEMY_DATABASE_URI"] = f"mssql+pyodbc://{server}/{db_name}?driver={driver}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_COMMIT_ON_TEARDOWN'] = True
app.config['SQLALCHEMY_NATIVE_UNICODE'] = True
# configuration of mail
app.config['MAIL_SERVER']='smtp.gmail.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USERNAME'] = str(os.getenv('EMAIL'))
app.config['MAIL_DEFAULT_SENDER'] = str(os.getenv('EMAIL'))
app.config['MAIL_PASSWORD'] = 'gdpmfostoussfscf'
app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = True
recipients = []
for r in  os.getenv('ADMINS').split(','):
    recipients.append(str(r))

support = []
for r in  os.getenv('SUPPORT').split(','):
    support.append(str(r))


mail = Mail(app)





db = SQLAlchemy(app)

Base = automap_base()



def _name_for_collection_relationship(base, local_cls, referred_cls, constraint):
    if constraint.name:
        return constraint.name.lower()
    # if this didn't work, revert to the default behavior
    return name_for_collection_relationship(base, local_cls, referred_cls, constraint)

Base.prepare(db.engine, reflect=True, name_for_collection_relationship=_name_for_collection_relationship)
session = Session(db.engine,autocommit=False)

# DB Model Classes
Orders = Base.classes.Orders
OrderDrivers = Base.classes.OrderDrivers
OrderScans = Base.classes.OrderScans
OrderPackageItems = Base.classes.OrderPackageItems
ClientMaster = Base.classes.ClientMaster
Employees = Base.classes.Employees 
Terminals = Base.classes.Terminals


non_complete_count = session.query(Orders.OrderTrackingID, ClientMaster.ClientID, OrderDrivers.DriverID)
non_complete_count = non_complete_count.join(OrderDrivers, Orders.OrderTrackingID == OrderDrivers.OrderTrackingID)
non_complete_count = non_complete_count.join(ClientMaster, ClientMaster.ClientID == Orders.ClientID)


complete_count = session.query(OrderDrivers.OrderTrackingID, Orders.ClientID, ClientMaster.ClientID)
complete_count = complete_count.join(Orders, OrderDrivers.OrderTrackingID == Orders.OrderTrackingID)
complete_count = complete_count.join(ClientMaster, ClientMaster.ClientID == Orders.ClientID)

yesterday =  datetime.today() - timedelta(days=1)
yesterday = yesterday.date()
today = datetime.today()
today = today.date()

def send_error_email():
    today = datetime.today()
    today = today.strftime("%m/%d/%Y, %H:%M:%S")
    subject = 'Driver Completion Report - ' + today
    msg = Message(
                    subject,
                    recipients = support
                )
    msg.body = 'There was a server error when trying to perform the driver completion report. Please check app log to see error'

    mail.send(msg)
    return render_template('500.html')

def get_uncomplete_count(employee_id):
    try:
        response = non_complete_count.filter(
            OrderDrivers.DriverID == employee_id, 
            Orders.Status == 'N',
            Orders.DeliveryTargetTo.cast(Date) >= yesterday, 
            Orders.DeliveryTargetTo.cast(Date) <= today)
        response = len(response.all())
        return response
    except:
        return send_error_email()


def get_complete_count(employee_id):
    try:
        status_list = ['N', 'D', 'L']
        response = complete_count.filter(
            OrderDrivers.DriverID == employee_id, 
            ~Orders.Status.in_(status_list),
            Orders.DeliveryTargetTo.cast(Date) >= yesterday, 
            Orders.DeliveryTargetTo.cast(Date) <= today)
        response = len(response.all())
        return response
    except:
        return send_error_email()


def get_driver_report():
    try:
        driver_complete = session.query(Terminals.TerminalID, Terminals.TerminalName, Employees.ID, Employees.DriverNo, Employees.LastName, Employees.FirstName)
        driver_complete = driver_complete.join(Terminals, Employees.TerminalID == Terminals.TerminalID)
        driver_complete = driver_complete.filter(Employees.Status == 'A', Employees.Driver == 'Y', Employees.DriverType == 'C')
        driver_complete = driver_complete.group_by(Terminals.TerminalID, Terminals.TerminalName, Employees.ID, Employees.DriverNo, Employees.LastName, Employees.FirstName)
        driver_complete = driver_complete.all()

        drivers = [r._asdict() for r in driver_complete]
        
        for driver in drivers:
            driver['Uncompleted'] = get_uncomplete_count(driver['ID'])
            driver['Completed'] = get_complete_count(driver['ID'])

        today = date.today()
        today = today.strftime("%m_%d_%y")
        file_name = 'Driver_Completion_Report-' + today + '.xlsx'
        workbook = xlsxwriter.Workbook(file_name)
        worksheet = workbook.add_worksheet()

        headers = ['Terminal ID', 'Terminal Name', 'Employee ID', 'Driver NO', 'Last Name', 'First Name', 'Uncompleted', 'Completed']
        for x in range(len(headers)):
            worksheet.write(0, x, headers[x])
        
        for idx, driver in enumerate(drivers):
            
            worksheet.write(idx+1, 0, driver['TerminalID'])
            worksheet.write(idx+1, 1, driver['TerminalName'])
            worksheet.write(idx+1, 2, driver['ID'])
            worksheet.write(idx+1, 3, driver['DriverNo'])
            worksheet.write(idx+1, 4, driver['LastName'])
            worksheet.write(idx+1, 5, driver['FirstName'])
            worksheet.write(idx+1, 6, driver['Uncompleted'])
            worksheet.write(idx+1, 7, driver['Completed'])

        workbook.close()

        

        subject = 'Driver Completion Report - ' + today
        msg = Message(
                        subject,
                        recipients = recipients
                    )
        msg.body = 'Find attached the driver completion report in the email'
        file = open(file_name, 'rb')
    
        
        msg.attach(file_name, '	application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', file.read())
        mail.send(msg)

        return send_file(
            file_name,
            mimetype='application/vnd.ms-excel', 
            as_attachment=True
        )
    except:
        return send_error_email()

@app.route('/')
def home_rte():
    try:
        return render_template('home.html')
    except:
        return send_error_email()

@app.route('/report')
def report_rte():
    get_driver_report()

@app.route('/driverreport', methods=["GET", "POST"])
def driver_report_rte():
    passcode=request.form.get("passcode")
    if passcode != os.getenv("PASSCODE"):
        return render_template('403.html')
    get_driver_report()
    

if __name__ == "__main__":
    app.run()