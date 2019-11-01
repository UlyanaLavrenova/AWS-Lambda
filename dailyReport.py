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
BODY_TEXT = ("In this e-mail you can find daily or monthly report in attached file\r\n\n"
             "This e-mail was generated automatically.\n"
             "Please don't replay to it.")
TABLE_NAME = 'beadcode_logging'
##############################################################################
RECIPIENT = os.environ['RECIPIENT']


def read_from_table(date_filter):
    dynamodb = boto3.resource('dynamodb',
                              region_name='eu-central-1')
    table = dynamodb.Table(TABLE_NAME)
    dynamodb_data = table.scan(
        IndexName='timestamp-beadcode-index',
        Select='ALL_ATTRIBUTES',
        FilterExpression=Attr('timestamp').begins_with(date_filter)
    )

    items = dynamodb_data['Items']
    row_count = dynamodb_data['Count']

    if row_count != 0:
        report_name = write_to_s3(date_filter, items)
        try:
            last_key = dynamodb_data['LastEvaluatedKey']
        except:
            return report_name, row_count
        while last_key != {}:
            dynamodb_data = table.scan(
                ExclusiveStartKey=last_key,
                IndexName='timestamp-beadcode-index',
                Select='ALL_ATTRIBUTES',
                FilterExpression=Attr('timestamp').begins_with(date_filter)
            )

            items = dynamodb_data["Items"]

            report_name = write_to_s3(date_filter, items)

            try:
                last_key = dynamodb_data['LastEvaluatedKey']
            except:
                return report_name, row_count
    else:
        return '0', row_count


def write_to_s3(report_date, items):
    s3 = boto3.resource('s3')
    file_name = 'report_' + str(report_date).replace('-', '_') + '.csv'
    file_name_with_path = '/tmp/' + file_name
    df = pd.DataFrame(items)
    df.to_csv(file_name_with_path)
    s3.Object(S3_BUCKET_NAME, file_name).put(Body=open(file_name_with_path, 'rb'))
    return file_name_with_path


def send_email(report_name, subject, row_count):
    ses = boto3.client('ses')
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
        print("E-mail was sent! " + str(row_count) + " rows were found. Message ID:"),
        print(response['MessageId'])


def main(event, context):
    report_date = datetime.now()
    response=''
    if report_date.day == 1:
        #sending monthly report
        SUBJECT = 'Monthly report'
        date_filter_month = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m')
        month_report_name, row_count = read_from_table(date_filter_month)
        send_email(month_report_name, SUBJECT, row_count)

        date_filter_day = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
        SUBJECT = 'Daily report'
        day_report_name, row_count = read_from_table(date_filter_day)
        if row_count != 0:
            send_email(day_report_name, SUBJECT, row_count)
        else:
            response='E-mail was not sent. Empty result.'
            print(response)
    else:
        date_filter_day = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
        day_report_name, row_count = read_from_table(date_filter_day)
        SUBJECT = 'Daily report'
        if row_count != 0:
            send_email(day_report_name, SUBJECT, row_count)
        else:
            response='E-mail was not sent. Empty result.'
            print(response)

    return response


if __name__ == "__main__":
    main('', '')
