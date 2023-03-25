#!/usr/bin/env python

'''
This script detects if a Job has missed its scheduled execution, and if so,
it sends a notification email.  

The script can be run using cron at the appropriate interval 


Prerequisites:

 - Python 3.6+

 - StreamSets SDK for Python v3.x
   See: https://docs.streamsets.com/sdk/latest/installation.html

 - Python v3.x pytz module
    $ pip3  install pytz

 - Control Hub login and password for a user with Organization Administrator role

 - An SMTP Server with a user login and password in order to send email notifications

 - Set the following variables in the script:

        job_id - the ID of the Job to be monitored
        job_expected_frequency_minutes - for example, if a Job is expected to run once a day, use a value of 24 * 60
        sdc_timezone - the timezone for the Data Collector where the Job runs, for example 'US/Pacific'
        smtp_host - the hostname of the SMTP server
        smtp_port - the SMTP port
        email_sender - the email "from" address
        email_recipient - the email "to" address

 - To avoid including credentials in the script, export these environment variables
   prior to running the script:

        export SCH_USER=<the Control Hub user>
        export SCH_PASSWORD=<the Control Hub password>
        export SMTP_USER=<the smtp user>
        export SMTP_PASSWORD=<the smtp password>
'''

# Imports
import pytz, os, smtplib, ssl, time, sys
from datetime import datetime
from email.message import EmailMessage
from streamsets.sdk import ControlHub

# Time Zone of Data Collector where the Job runs
sdc_timezone = 'US/Pacific'

# Job to monitor
job_id = '<your job id>'

# Control Hub Connection
sch_url = 'https://cloud.streamsets.com'
sch_user = os.getenv('SCH_USER')
sch_password = os.getenv('SCH_PASSWORD')

# Job expected run frequency (minutes)
job_expected_frequency_minutes= 24 * 60 # example for 24 hours: 24 * 60

# SMTP Settings
smtp_host = '<your smtp host>'
smtp_port = 587
email_sender = '<your from email>'
email_recipient = '<your to email>
smtp_user = os.getenv('SMTP_USER')
smtp_password = os.getenv('SMTP_PASSWORD')


# =============================================================
# Method: convert_utc_millis_to_sdc_local_datetime
def  convert_utc_millis_to_sdc_local_datetime(utc_millis, sdc_offset_millis):
    local_seconds = (utc_millis + sdc_offset_millis) / 1000
    local_dt_str = str(datetime.fromtimestamp(local_seconds))
    local_dt_str_trim_nanos = local_dt_str[0:19]
    return local_dt_str_trim_nanos
# =============================================================

# =============================================================
# Method: get_sdc_utc_offset_millis
# There is undoubtedly a better way to do this!
def get_sdc_utc_offset_millis(sdc_timezone):

    # Get timezone offset as a string
    offset = datetime.now(pytz.timezone(sdc_timezone)).strftime('%z')

    negative_offset = False

    # trim the leading '-' if it's negative
    if offset[0] == '-':
        negative_offset = True
        offset = offset[1:]

    # trim leading zero if it exists
    if offset[0] == '0':
        offset = offset[1:]

    # Get the offset hours
    offset_hours = int(offset[:-2])

    # Handle 30 minute timezones
    if offset[2:] == '30':
        offset_hours = offset_hours + .5

    # Convert hours to millis
    offset_millis = offset_hours * 60 * 60 * 1000

    if negative_offset:
        return offset_millis * -1
    else:
        return offset_millis
# =============================================================


# Connect to Control Hub
sch = ControlHub(sch_url,
    username=sch_user,
    password=sch_password)

# Get the Job
try:
    job = sch.jobs.get(job_id = job_id)
except Exception as e:
    print('Error: could not find Job with ID: \'' + job_id + '\'')
    sys.exit(-1)

print('\nGeting status for Job \'' + job.job_name + '\' with Job ID \'' + job_id + '\'\n')

print('Current job status is \'' + job.status.status + '\'')

# Get the TZ offset millis for the SDC machine
sdc_offset_millis = get_sdc_utc_offset_millis(sdc_timezone)

# Convert Job frequency minutes to millis
job_expected_frequency_millis = job_expected_frequency_minutes * 1000 * 60

notify = False

# Get Job Status
if job.status.status == 'INACTIVE':

    # Get the most recent run if it exists
    if len(job.status.run_history) > 0:

        # Get the time of the most recent run
        last_run_time_millis = job.status.run_history[0].time

        # Convert the most recent run time millis to SDC local time
        last_run_time = convert_utc_millis_to_sdc_local_datetime(last_run_time_millis, sdc_offset_millis)

        # Get the current time in millis
        current_time_millis = time.time() * 1000

        print('\nThe last Job run was at: ' + last_run_time)

        job_deadline_millis = last_run_time_millis + job_expected_frequency_millis

        job_deadline_time = convert_utc_millis_to_sdc_local_datetime(job_deadline_millis, sdc_offset_millis)

        current_time = convert_utc_millis_to_sdc_local_datetime(current_time_millis, sdc_offset_millis)

        print('\nNext Job run expected before: ' + job_deadline_time)

        print('\nCurrent time: ' + current_time)

        if job_deadline_millis  < current_time_millis:

            print('\n' + 60 * '=')
            print('MISSED EXECUTION!  The Job did not run within the expected time!')
            print(60 * '=')

            email_subject = 'Missed Execution for Job: ' + job.job_name
            email_message = '\nThe last execution of the Job was at ' + last_run_time
            email_message += '\nThe next Job execution was expected by ' + job_deadline_time
            email_message += '\nAs of ' + current_time + ' the expected Job run has not yet started.'
            notify = True

    else:
        print('\nError: no Job history for Job \'' + job.job_name + '\' with Job ID \'' + job_id + '\'')
        print('--> Please run the Job at least once before using this script.\n')
        sys.exit(-1)

else:
    email_subject = 'Unexpected status for StreamSets Job: ' + job.job_name
    email_message = 'Unexpected status for StreamSets Job ' +  '\'' + job.job_name + '\' with Job ID \'' + job_id + '\''
    email_message += '\nExpected Job status is \'INACTIVE\''
    email_message += '\nActual Job status is \'' + job.status.status + '\''
    notify = True

if notify:

    context = ssl.create_default_context()

    msg = EmailMessage()
    msg['Subject'] = email_subject
    msg['From'] = email_sender
    msg['To'] = email_recipient
    msg.set_content(email_message)
    print('\n' + 60 * '=')
    print('\nSending email:')
    print('Subject: ' + email_subject)
    print('Message body: ' + email_message)
    print('\n' + 60 * '=')
    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls(context=context)
        server.login(smtp_user, smtp_password)
        server.send_message(msg)

else:
    print('\nExpected Job execution was not misssed; no notification was sent.')

print('\nDone')