import pandas as pd, numpy as np, json, datetime, time, random, csv
import csv
import datetime
from pyspark.context import SparkContext

FILE_LOCATION = './data/2015_07_22_mktplace_shop_web_log_sample.log'
SESSION_LIMIT = 15*60 # 15 minutes

column_names = 'timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes request user_agent ssl_cipher ssl_protocol method url protocol client cport'
column_names = column_names.split(' ')

# Keep track of the number of bad lines in the CSV (lines with incorrect number of fields)
n_bad_lines = sc.accumulator(0)
correct_n_cols = sc.broadcast(len(column_names))

def csv_parse(line):
    '''
    Parse each line in the CSV file
    Return list of fields
    '''
    global n_bad_lines
    try:
        parsed_line = next(csv.reader([line], delimiter=' '))
        parsed_line[0] = pd.to_datetime(parsed_line[0])
        parsed_line += parsed_line[-4].split(' ') # Extract HTTP protocol
        parsed_line += parsed_line[2].split(':') # Extract client IP & port
        if len(parsed_line) != correct_n_cols.value:
            n_bad_lines += 1
        return parsed_line
    except Exception as e:
        print e
        return [''] * len(column_names)

# Read and parse CSV file. Ignore bad lines
log_file = sc.textFile(FILE_LOCATION)\
    .map(csv_parse)\
    .filter(lambda row: len(row) == len(column_names))\
    .cache()

# Sessionization function (batch)
def sessionize(key_request_list, session_limit):
    key = key_request_list[0]
    request_list = key_request_list[1].data
    # Sort requests by timestamp
    request_list.sort(key=lambda x: x[0])
    session_list = []

    session_counter = 0
    session_id = lambda: '%s_%s' % (key, session_counter)
    
    current_session_id = session_id()
    current_length = 0
    current_urls = set()
    
    url = request_list[0][-4]
    current_urls.add(url)
    
    for i in xrange(1, len(request_list)):
        url = request_list[i][-4]
        duration = (request_list[i][0] - request_list[i-1][0]).total_seconds()
        if duration <= session_limit:
            current_length += duration
            current_urls.add(url)
        else:
            session_list.append((current_session_id, (current_length, current_urls)))
            session_counter += 1
            current_session_id = session_id()
            current_length = 0
            current_urls = set(url)

    session_list.append((current_session_id, (current_length, current_urls)))

    return session_list

# Group request by IP
requests_by_ip = log_file.groupBy(lambda l: l[-2])

# Convert each group into a list of sessions
sessions = requests_by_ip.flatMap(lambda kv: sessionize(kv, SESSION_LIMIT)).cache()

# Collect sessions for analysis
data = sessions.map(lambda kv: (kv[0], kv[0].split('_')[0], kv[1][0], kv[1][1])).collect()

# Construct a pandas frame of the collected results
session_summary = pd.DataFrame(data, columns=['session_id', 'ip', 'seconds', 'urls'])
print 'Number of sessions:', session_summary.shape[0]
session_summary.sort_values(by='seconds', ascending=False, inplace=True)
session_summary['readable_length'] = session_summary.seconds.map(lambda x: pd.Timedelta(x, 's'))
session_summary['n_urls'] = session_summary['urls'].map(len)

print 'Session summary'
print session_summary.head(10)

# Average Session length
print 'Average session length:', sessions.map(lambda kv: kv[1][0]).mean()

print 'Most  engaged users (IPs with longest sessions)'
ip_max_session = session_summary.groupby('ip')['seconds'].max()
print ip_max_session.sort_values(ascending=False).head(10)



