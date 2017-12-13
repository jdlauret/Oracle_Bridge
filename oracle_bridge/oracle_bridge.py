# standard library imports
import re
import json
import sys
import os
import datetime as dt
# related third party imports
import cx_Oracle
# local application/library specific imports
from google_bridge.google_bridge import read_drive_file
from pprint import pprint

from dateutil.parser import parse


def find_data_file(filename):
    if getattr(sys, 'frozen', False):
        # The application is frozen
        data_dir = os.path.dirname(sys.executable)
    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        data_dir = os.path.dirname(__file__)

    return os.path.join(data_dir, filename)


def find_main_dir():
    if getattr(sys, 'frozen', False):
        # The application is frozen
        return os.path.dirname(sys.executable)
    else:
        # The application is not frozen
        # Change this bit to match where you store your data files:
        return os.path.dirname(__file__)


def open_connection(connection='prod', credentials='public'):
    with open(find_data_file('data_warehouse_creds.json')) as infile:
        dw_creds = json.load(infile)
    if credentials.lower() == 'public':
        crendentials = dw_creds['credentials']['public']
    elif credentials.lower() == 'private':
        crendentials = dw_creds['credentials']['private']

    connection_data = dw_creds['connections']
    if connection.lower() == 'prod':
        connection_data = connection_data['prod']
    elif connection.lower() == 'dev':
        connection_data = connection_data['dev']

    host = connection_data.get('host')
    port = connection_data.get('port')
    sid = connection_data.get('sid')

    dns_tns = cx_Oracle.makedsn(host, port, sid)
    try:
        username = crendentials.get('username')
        password = crendentials.get('password')
        db = cx_Oracle.connect(username, password, dns_tns)
        return db
    except Exception as e:
        print(e)
        return 'Error'


def execute_query(query, connection='prod', credentials='public'):
    db = open_connection(connection=connection, credentials=credentials)
    cursor = db.cursor()
    cursor.execute(query)
    db.commit()
    cursor.close()


def run_query(script_id, script_storage, raw_query=False, connection='prod', credentials='public'):
    cursor = open_connection(connection=connection,
                             credentials=credentials).cursor()
    if raw_query is False:
        file_data = read_drive_file(script_id)
        file_data = re.sub(' +', ' ', file_data)
        file_data = file_data.replace(';', '')
    else:
        file_data = raw_query

    cursor.execute(file_data)
    column_names = cursor.description
    header = []
    for item in column_names:
        header.append(item[0])
    header = tuple(header)
    header = [header]
    results = cursor.fetchall()
    results = header + results

    return results


def get_data_from_table(table_name, distict=False, column_name=None, connection='prod', credentials='public'):
    if not distict:
        if column_name is None:
            query = "SELECT * FROM {0}".format(table_name)
        else:
            formatted_column_names = column_name.split(',')
            for i, column in enumerate(formatted_column_names):
                formatted_column_names[i] = table_name + '.' + column
            formatted_column_names = ", ".join(formatted_column_names)
            query = "SELECT {0} FROM {1}".format(formatted_column_names, table_name)
    else:
        query = "SELECT DISTINCT tn.{0} FROM {1} tn".format(column_name, table_name)
    cursor = open_connection(connection=connection, credentials=credentials).cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    return results


def remove_non_ascii(text):
    return ''.join([i if ord(i) < 128 else '' for i in text])


def filter_lines(data, classes):
    print()
    print('Prepping data for the Data Warehouse')
    new_data = data.copy()
    before_filter = len(new_data)
    lines_for_removal = []
    for j, line in enumerate(new_data):
        for i, item in enumerate(line):
            class_name = str(classes[i])
            class_name = class_name.replace('<class', '').replace('\'', '').replace('>', '').replace(' ', '')
            if item is None:
                new_data[j][i] = ''
            elif class_name == 'cx_Oracle.DATETIME':
                if item != '':
                    if not isinstance(item, dt.datetime):
                        try:
                            new_data[j][i] = parse(item).strftime("%d-%b-%y")
                        except (TypeError, ValueError):
                            try:
                                new_data[j][i] = item.strftime("%d-%b-%y")
                            except (TypeError, ValueError):
                                lines_for_removal.append(j)
                    elif isinstance(item, dt.datetime):
                        try:
                            new_data[j][i] = str(item.strftime("%d-%b-%y"))
                        except:
                            lines_for_removal.append(j)
            elif class_name == 'cx_Oracle.STRING':
                try:
                    new_data[j][i] = str(item)
                    try:
                        new_data[j][i] = remove_non_ascii(item)
                    except:
                        pass
                    try:
                        if chr(9) in item:
                            new_data[j][i] = new_data[j][i].replace(chr(9), ' ')
                        if chr(10) in item:
                            new_data[j][i] = new_data[j][i].replace(chr(10), ' ')
                        if chr(13) in item:
                            new_data[j][i] = new_data[j][i].replace(chr(13), ' ')
                    except:
                        pass
                except:
                    lines_for_removal.append(j)
                    print('Line {0} removed because column {1} is not a string'.format(j, i))
            elif class_name == 'cx_Oracle.NUMBER':
                try:
                    new_data[j][i] = str(new_data[j][i])
                except:
                    lines_for_removal.append(j)

    for r in reversed(lines_for_removal):
        del new_data[r]

    after_filter = len(new_data)
    lines_removed = after_filter - before_filter
    if lines_removed > 0:
        print("Removed {0} Lines".format(lines_removed))
        print(lines_for_removal)
    else:
        print("Data prep complete")
    return new_data


def clear_table(table_name, connection='prod', credentials='public'):
    print()
    print("Deleting data in {0}".format(table_name))
    query = """DELETE FROM {0}""".format(table_name)
    db = open_connection(connection=connection, credentials=credentials)
    cursor = db.cursor()
    cursor.execute(query)
    db.commit()
    cursor.close()


def get_column_info(table_name, connection='prod', credentials='public'):
    print()
    print('Retrieving Column Types from {0}'.format(table_name))
    query = "SELECT * FROM {0} WHERE 1=0".format(table_name)
    cursor = open_connection(connection=connection, credentials=credentials).cursor()
    cursor.execute(query)
    return cursor.description


def print_progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    import sys
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = '█' * filled_length + '-' * (bar_length - filled_length)

    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()


def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [
        alist[i * length // wanted_parts: (i + 1) * length // wanted_parts]
        for i in range(wanted_parts)
    ]


def update_table(table_name, data, header_included=True, connection='prod', credentials='public'):
    print()
    print('Preparing Data for Upload to {0}'.format(table_name))
    data_to_upload = data.copy()
    if header_included:
        del data_to_upload[0]
    if len(data_to_upload) > 0:
        column_info = get_column_info(table_name, connection=connection, credentials=credentials)
        header = [item[0] for item in column_info if item[0] != 'ID']
        classes = [item[1] for item in column_info if item[0] != 'ID']
        new_data = filter_lines(data_to_upload, classes)
        value_string = ''
        for i in range(len(header)):
            if i < len(header):
                value_string = value_string + ':{0}, '.format(i + 1)
        header = ', '.join(header)
        value_string = value_string[:-2]
        print('Uploading Data to {0}'.format(table_name))
        db = open_connection(connection=connection, credentials=credentials)
        cursor = db.cursor()
        query = "INSERT INTO {0} ({1}) VALUES ({2})".format(table_name, header, value_string)
        cursor.prepare(query)
        cursor.bindnames()
        if len(new_data) > 500000:
            print('Data set is too large for single submission')
            num_needed = round(len(new_data) / 500000)

            print('Data set will be split into {0} parts'.format(num_needed))
            lists_for_execution = split_list(new_data, num_needed)

            for i, list_for_execution in enumerate(lists_for_execution):
                print('Upload {0} of {1} in progress'.format(i + 1, len(lists_for_execution)))
                cursor.executemany(None, list_for_execution, batcherrors=True)
                batch_errors = cursor.getbatcherrors()
                print('Upload completed')

                if len(batch_errors) > 0:
                    for j, error in enumerate(batch_errors):
                        batch_errors[j] = error.message
                    pprint(batch_errors)
                else:
                    print('No Errors encountered')
        elif len(new_data) == 1:
            try:
                cursor.execute(None, new_data[0])
            except (cx_Oracle.DatabaseError, cx_Oracle.DataError, TypeError) as e:
                error, = e.args
                try:
                    print('Error on data upload for {0} on line {1}'.format(table_name, 1))
                    print("Oracle-Error-Code from row by row insert:", error.code)
                    print("Oracle-Error-Message from row by row insert:", error.message)
                    print(new_data)
                except:
                    print('Type Error from row by row insert: {0}'.format(error))
                    print(new_data)
        else:
            try:
                print('Upload in progress')
                cursor.executemany(None, new_data, batcherrors=True)
                batch_errors = cursor.getbatcherrors()
                if len(batch_errors) > 0:
                    for j, error in enumerate(batch_errors):
                        batch_errors[j] = error.message
                    pprint(batch_errors)
                else:
                    print('No Errors encountered')
            except (cx_Oracle.DatabaseError, cx_Oracle.DataError, TypeError) as e:
                error = e.args
                try:
                    print("Oracle-Error-Code from batch insert", error.code)
                    print("Oracle-Error-Message from batch insert", error.message)
                except:
                    print('Type Error from batch insert: {0}'.format(error))
                    print('Now submitting single rows')
                    for j, line in enumerate(new_data):
                        try:
                            cursor.execute(None, new_data[j])
                        except (cx_Oracle.DatabaseError, cx_Oracle.DataError, TypeError) as e:
                            error, = e.args
                            try:
                                print('Error on data upload for {0} on line {1}'.format(table_name, j + 1))
                                print("Oracle-Error-Code from row by row insert:", error.code)
                                print("Oracle-Error-Message from row by row insert:", error.message)
                                print(new_data[j])
                            except:
                                print('Type Error from row by row insert: {0}'.format(error))
                                print(new_data[j])

        db.commit()
        cursor.close()
        db.close()

    else:
        print("No New Data Uploaded")