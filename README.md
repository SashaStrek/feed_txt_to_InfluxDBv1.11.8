# feed_txt_to_InfluxDBv1.11.8
Feeder for InfluxDBv1.11.8 that parses txt files and send lines to InfluxDBv1  
  
File:        feed_txt_to_InfluxDBv1.py  
Created:     2025 by Alexandr Strekalovski  
Description:   
  This script parses "*.txt log files" for the {MEASUREMENT},  
  line by line,  
  starting with "starting_file.txt" through the last file in the same directory (ordered by mtime)  
  and sends data to InfluxDB v1.11.8  
  
Usage:  
  python3 feed_txt_to_InfluxDBv1.py /path/to/starting_file.txt  
  
Attention:  
  Redefine the "process_line" function!  
    The line parser is described in "def process_line".  
    It was written for my custom measurement format, so just redefine the "process_line" function for your format.  
