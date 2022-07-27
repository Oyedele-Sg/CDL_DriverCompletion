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

import logging, logging.handlers
import xlsxwriter
import os

os.environ["WERKZEUG_RUN_MAIN"] = "true"
load_dotenv()
logging.basicConfig(format='%(asctime)s %(message)s',filename='error.log',level=logging.WARN)
# def setup_log(name):
#     logger = logging.getLogger(name)   

#     logger.setLevel(logging.DEBUG)

#     log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#     filename = f"{name}.log"
#     log_handler = logging.FileHandler(filename)
#     smtp_handler = logging.handlers.SMTPHandler(mailhost=('smtp.gmail.com', 587),
#                                                 fromaddr=str(os.getenv('EMAIL')),
#                                                 toaddrs=[str(os.getenv('SUPPORT'))],
#                                                 subject='Error In CDL Driver Completion',
#                                                 credentials=(str(os.getenv('EMAIL')), str(os.getenv('MAIL_PASS'))),
#                                                 secure=())
#     log_handler.setLevel(logging.DEBUG)
#     smtp_handler.setLevel(logging.WARNING)
#     log_handler.setFormatter(log_format)
#     smtp_handler.setFormatter(log_format)

#     logger.addHandler(log_handler)
#     logger.addHandler(smtp_handler)

#     return logger



# def start_log(name):
#     logger = setup_log(name)
#     logger.info("Just logged from %s", name)   

# start_log("driver_completion")

app = Flask(__name__)

mail = Mail(app)

# Database 
# driver = 'ODBC Driver 17 for SQL Server'
driver = 'SQL Server'
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
app.config['MAIL_SERVER']='smtp.office365.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USERNAME'] = os.getenv("EMAIL")
app.config['MAIL_DEFAULT_SENDER'] = os.getenv("EMAIL")
app.config['MAIL_PASSWORD'] = os.getenv("MAIL_PASS")
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USE_SSL'] = False
recipients = []
if os.getenv('ADMINS') is not None:
    for r in  os.getenv('ADMINS').split(','):
        recipients.append(str(r))

support = []
if os.getenv('SUPPORT') is not None:
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

def get_uncomplete_count(employee_id):
    response = non_complete_count.filter(
        OrderDrivers.DriverID == employee_id, 
        Orders.Status == 'N',
        Orders.DeliveryTargetTo.cast(Date) == today)
    response = len(response.all())
    return response


def get_complete_count(employee_id):
    status_list = ['N', 'D', 'L']
    response = complete_count.filter(
        OrderDrivers.DriverID == employee_id, 
        ~Orders.Status.in_(status_list),
        Orders.DeliveryTargetTo.cast(Date) == today)
    response = len(response.all())
    return response
  

 
def get_driver_report():
    with app.test_request_context():  
        driver_complete = session.query(Terminals.TerminalID, Terminals.TerminalName, Employees.ID, Employees.DriverNo, Employees.LastName, Employees.FirstName)
        driver_complete = driver_complete.join(Terminals, Employees.TerminalID == Terminals.TerminalID)
        driver_complete = driver_complete.filter(Employees.Status == 'A', Employees.Driver == 'Y', Employees.DriverType == 'C')
        # driver_complete = driver_complete.group_by(Terminals.TerminalID, Terminals.TerminalName, Employees.ID, Employees.DriverNo, Employees.LastName, Employees.FirstName)
        driver_complete = driver_complete.order_by(Terminals.TerminalName)
        driver_complete = driver_complete.all()

        drivers = [r._asdict() for r in driver_complete]
        summary ={}
        total_summary ={
            'active': 0, 
            'complete': 0, 
            'total': 0,
            'percent_complete': 0, 
            'name': 'Total'
        }
        summary = {}
        for driver in drivers:
            uncompleted = get_uncomplete_count(driver['ID'])
            completed = get_complete_count(driver['ID'])
            driver['Uncompleted'] = uncompleted
            driver['Completed'] = completed
            terminal = driver['TerminalName']
            if terminal in summary:
                summary[terminal]['active'] += int(uncompleted)
                summary[terminal]['complete'] += int(completed)
                summary[terminal]['total'] += int(completed) + int(uncompleted)
            else:
                summary[terminal] = {}
                summary[terminal]['active'] = int(uncompleted)
                summary[terminal]['complete'] = int(completed)
                summary[terminal]['name'] = terminal
                summary[terminal]['total'] = int(completed) + int(uncompleted)
            total_summary['active'] += int(uncompleted)
            total_summary['complete'] += int(completed)
            total_summary['total'] += int(completed) + int(uncompleted)
            if summary[terminal]['total'] > 0:
                summary[terminal]['percent_complete'] = round((summary[terminal]['complete']/summary[terminal]['total']) * 100, 2) 
            if total_summary['total'] > 0:
                total_summary['percent_complete'] = round((total_summary['complete']/total_summary['total']) * 100, 2) 
           
        
        today = date.today()
        today = today.strftime("%m_%d_%y")
        file_name = 'Driver_Completion_Report-' + today + '.xlsx'
        workbook = xlsxwriter.Workbook(file_name)
        worksheet = workbook.add_worksheet()
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
        	

        headers = ['Terminal Name',  'Driver NO', 'Last Name', 'First Name', 'Uncompleted', 'Completed', '% Completed']
        for x in range(len(headers)):
            worksheet.write(0, x, headers[x])
        
        for idx, driver in enumerate(drivers):
            
            if (int(driver['Uncompleted'])+int(driver['Completed'])) >0:
                worksheet.write(idx+1, 0, driver['TerminalName'])
                worksheet.write(idx+1, 1, driver['DriverNo'])
                worksheet.write(idx+1, 2, driver['LastName'])
                worksheet.write(idx+1, 3, driver['FirstName'])
                worksheet.write(idx+1, 4, driver['Uncompleted'])
                worksheet.write(idx+1, 5, driver['Completed'])
                divisor = (driver['Completed'] + driver['Uncompleted'])
                if divisor > 0:
                    per = str(round((driver['Completed']/divisor) * 100, 2))
                    per = per + ' %'
                    worksheet.write(idx+1, 6, per )
                else:
                    worksheet.write(idx+1, 6, '0.00 %')

        workbook.close()

        

        subject = 'Driver Completion Report - ' + today
        msg = Message(
                        sender=str(os.getenv('EMAIL')),
                        subject=subject,
                        recipients = recipients
                    )
   
        msg.html = render_template('driver_report.html', day_of_report=date_time, total_summary=total_summary, summary=list(summary.values()))
        file = open(file_name, 'rb')

        
        msg.attach(file_name, '	application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', file.read())
        mail.send(msg)

        return file_name


      

        # return send_file(
        #     file_name,
        #     mimetype='application/vnd.ms-excel', 
        #     as_attachment=True
        # )


@app.route('/')
def home_rte():
    return render_template('home.html')
    

@app.route('/report')
def report_rte():
    file_name = get_driver_report()
    return 'Report generated Successfuly'
    

@app.route('/driverreport', methods=["GET", "POST"])
def driver_report_rte():
    passcode=request.form.get("passcode")
    if passcode != os.getenv("PASSCODE"):
        return render_template('403.html')
    file_name = get_driver_report()
    return 'Report generated Successfuly'


@app.errorhandler(500)
def internal_error(exception):
    return render_template('500.html'), 500


if __name__ == "__main__":
    app.run(host="localhost", port=7007)