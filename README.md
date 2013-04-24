#opentsdb2csv

A simple Python wrapper over `tsdb query` to merge several counters from OpenTSDB format into a single CSV file.

##Logic

So we gather a number of metrics in OpenTSDB database and would like to export them for further analysis. To do that, we have to specify the metrics in a configuration file and call a script to process them.

For an example of a configuration file, see `config.ini`.

For each metric you have to make a section in a file. The name of a section is a column name in a resulting file. Per each section you should specify a data extraction query in a format explained [here](http://opentsdb.net/cli.html).

When you launch the script, it merges data from all the metrics found while processing the configuration file and into a single table by timestamp. Then, this data is dumped to the specified .csv file.

##Prerequisities

`tsdb` tool has to be in your PATH.

##Command-line arguments
* '-s' or '--start' -- set start of time period for conversion, e.g. 2013/04/20-12:12. By default, 1 hour ago is taken.
* '-e' or '--end' -- set end of time period for conversion. By default, current time is taken.
* '-c' or '--config' -- path to the config file, `config.ini` by default.
* '-o', '--output' -- path to the output file, `output.csv` by default.

##Run
`./opentsdb2csv.py` to get the things done or `./opentsdb2csv.py -h` for some assistance.
