#!/usr/local/bin/python
"""
A simple script converting data from OpenTSDB database to CSV format.
"""
import ConfigParser
import argparse
import csv
import subprocess
import datetime

def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='Converts data from OpenTSDB database to CSV file')
    parser.add_argument('-s', '--start', type=str, default=get_default_start_date(), help='Start of time period, absolute or relative value.')
    parser.add_argument('-e', '--end', type=str, default='', help='End of time period, absolute or relative value.')
    parser.add_argument('-c', '--config', type=str, default='config.ini',help='Path to a configuration file')
    parser.add_argument('-o', '--output', type=str, default='output.csv',help='Path to output file')
    return parser.parse_args()

def get_default_start_date():
    """Return datetime of 1h ago in OpenTSDB-acceptable format, which is used as default"""
    return (datetime.datetime.now()-datetime.timedelta(hours=1)).strftime('%Y/%m/%d-%H:%M')

def run():
    """Execute the script"""
    args = parse_args()
    counters = parse_counters(args.config)
    opentsdb_output = {}

    for counter in counters:
        call = ['tsdb', 'query', args.start]
        if args.end:
            call.append(args.end)
        call.extend(counter.query.split(' '))
        opentsdb_output[counter.alias] = subprocess.check_output(call)

    #Merge results by timestamp and dump them to CSV file
        dump(merge(opentsdb_output),args.output)

def parse_counters(config_file):
    """Retrieve counters from a configuration file"""
    config = ConfigParser.RawConfigParser()
    config.read(config_file)

    counters = []

    for section in config.sections():
        counters.append(Counter(section,config.get(section,'query')))

    return counters

def merge(opentsdb_output):
    """
    Merge data from raw OpenTSDB output to a dictionary keyed by timestamp

    @param dict opentsdb_output: a dictionary contained results of fetching OpenTSDB data mapped by file alias
    @return dict result: dictionary of dictionaries containing merged OpenTSDB data mapped by timestamp.
     Each value is a dictionary mapping file alias to the value at given timestamp.
    """
    result = dict()
    for alias,output in opentsdb_output.iteritems():
        rows = output.split('\n')[:-1]

        #Parse rows and add values to the result
        for row in rows:

            split = row.split(' ',3)
            timestamp = split[1]
            value = split[2]

            ts_values = result.setdefault(timestamp,dict())
            ts_values[alias] = float(value)

    return result

def dump(merged_results,csv_file):
    """
    Dump results of merge function to csv file.
    @param dict merged_results: output of merge() function
    @param str csv_file: path to the file to write
    """

    #find all headers
    headers = set()
    for data in merged_results.values():
        [headers.add(alias) for alias in data.keys()]

    #generate and write rows
    headers = list(headers)
    headers.insert(0,'timestamp')

    with open(csv_file, 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        timestamps = sorted(merged_results.keys())
        for timestamp in timestamps:
            row = list()
            row.append(timestamp)
            for header in headers[1:]:
                data = merged_results[timestamp]
                if header in data:
                    row.append(str(data[header]))
                else:
                    row.append('nan')
            writer.writerow(row)


class Counter():

    def __init__(self,alias,query):
        self.alias = alias
        self.query = query

if __name__ == '__main__':
    run()