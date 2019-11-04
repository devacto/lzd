import pandas as pd
import sqlalchemy as sql
import logging

from datetime import datetime
from datetime import timedelta


# details of the database.
DB_HOST = 'terraform-20191101073604464400000001.cqpira9yntzj.ap-southeast-1.rds.amazonaws.com'
DB_USER = 'foo'
DB_PASS = 'foobarbaz'
DB_NAME = 'marketing'


# return session datetime from session_id.
def session_datetime_from_session_id(session_id):
    session_id_split = session_id.split('_')
    return datetime.strptime(session_id_split[1] + ' ' + session_id_split[2], '%Y%m%d %H%M')


# returns a datetime object from visit_date and visit_time details.
def create_datetime(v_date, v_time):
    return datetime.strptime(v_date + ' ' + v_time, '%Y%m%d %H:%M %p')


# returns session_id given device_id, visit_date, and visit_time.
# i_ prefix to denote inner variable.
def create_new_session_id(i_device_id, i_visit_date, i_visit_time):
    return 's{0}_{1}_{2}'.format(i_device_id, i_visit_date, i_visit_time.replace(':', '')[0:4])


# updates session_id in the database.
def update_session_id_for_row(i_session_id, i_row_id, i_sql_engine):
    connection = i_sql_engine.connect()
    connection.execute('update clickstream set session_id = \'{0}\' where id = \'{1}\''.format(i_session_id, i_row_id))


# returns a dataframe row containing existing sessions for a device_id.
def sessions_for(device_id, i_sql_engine):
    sessions_for_device_query = '''
            select device_id, session_id, visit_date, visit_time 
            from clickstream where device_id = \'{0}\' 
            and session_id is not null'''.format(device_id)
    sessions_for_device_df = pd.read_sql_query(sessions_for_device_query, i_sql_engine)
    return sessions_for_device_df


# return last session.
def last_session_from_sessions(sessions):
    sessions['visit_datetime'] = sessions.apply(lambda x: create_datetime(x['visit_date'], x['visit_time']), axis=1)
    sessions_sorted = sessions.sort_values(by='visit_datetime', ascending=False)
    return sessions_sorted.iloc[0]


# row is one row of a dataframe.
def process_row(row, i_sql_engine):
    row_id = row['id']
    device_id = row['device_id']
    visit_date = row['visit_date']
    visit_time = row['visit_time']
    visit_datetime = create_datetime(visit_date, visit_time)

    logging.info('Processing row with ID: {0}'.format(row_id))
    sessions_for_device = sessions_for(device_id, i_sql_engine)

    # if there is no session in the database for that device_id, then create new session.
    if sessions_for_device.empty:
        session_id = create_new_session_id(device_id, visit_date, visit_time)
        update_session_id_for_row(session_id, row_id, i_sql_engine)

    else:
        # get latest existing session.
        latest_session_id = last_session_from_sessions(sessions_for_device)['session_id']
        latest_session_datetime = session_datetime_from_session_id(latest_session_id)

        # if still within the previous session time window, then use the previous session id.
        if latest_session_datetime + timedelta(minutes=60) > visit_datetime:
            update_session_id_for_row(latest_session_id, row_id, i_sql_engine)

        # if not within the previous session time window, then create a new session id.
        else:
            new_session_id = create_new_session_id(device_id, visit_date, visit_time)
            update_session_id_for_row(new_session_id, row_id, i_sql_engine)


# main method.
def main():
    logging.basicConfig(level=logging.INFO)
    connect_string = 'mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(DB_USER, DB_PASS, DB_HOST, DB_NAME)
    sql_engine = sql.create_engine(connect_string)

    df = pd.read_sql_query('select * from clickstream where session_id is null', sql_engine)
    for i in range(len(df)):
        process_row(df.iloc[i], sql_engine)


if __name__ == '__main__':
    main()
