import boto3
import pandas as pd
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import os


###############################################################################
S3_BUCKET_NAME = 'beadcodereport'
AWS_REGION = 'eu-west-1'
SENDER = 'bead-barcode@goodyear.com'
CHARSET = "UTF-8"
BODY_TEXT = ("In this e-mail you can find data from beadcode_logging table in attached file\r\n\n"
             "This e-mail was generated automatically.\n"
             "Please don't replay to it.")
TABLE_NAME = 'beadcode_logging'
###############################################################################
RECIPIENT = os.environ['RECIPIENT']


def read_from_table(report_date):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)
    dynamodb_data = table.scan()

    items = dynamodb_data['Items']
    report_name = write_to_s3(report_date, items)
    try:
        last_key = dynamodb_data['LastEvaluatedKey']
    except:
        return report_name
    while last_key != {}:
        dynamodb_data = table.scan()

        items = dynamodb_data["Items"]
        report_name = write_to_s3(report_date, items)

        try:
            last_key = dynamodb_data['LastEvaluatedKey']
        except:
            return report_name


def write_to_s3(report_date, items):
    s3 = boto3.resource('s3')
    file_name = 'full_report_' + str(report_date).replace('-', '_') + '.csv'
    file_name_with_path = '/tmp/' + file_name
    df = pd.DataFrame(items)
    df.to_csv(file_name_with_path)
    s3.Object(S3_BUCKET_NAME, file_name).put(Body=open(file_name_with_path, 'rb'))
    return file_name_with_path


def send_email(report_name, subject):
    ses = boto3.client('ses',
                       region_name=AWS_REGION)
    msg = MIMEMultipart()
    msg['Subject'] = subject
    #msg['From'] = SENDER
    #msg['To'] = RECIPIENT
    part = MIMEText(BODY_TEXT)
    msg.attach(part)
    part = MIMEApplication(open(str(report_name), 'rb').read())
    part.add_header('Content-Disposition', 'attachment', filename=str(report_name[5:]))
    msg.attach(part)
    try:
        response = ses.send_raw_email(
            Destinations=[RECIPIENT],
            FromArn='',
            RawMessage={
                'Data': msg.as_string()
            },
            ReturnPathArn='',
            Source=SENDER,
            SourceArn='',
        )

    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


def main(event, context):
    report_date = datetime.strftime(datetime.now(), '%Y-%m-%d')
    full_report_name = read_from_table(report_date)
    SUBJECT = 'Full data report'
    send_email(full_report_name, SUBJECT)
