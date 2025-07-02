# feed_txt_to_InfluxDBv1.11.8
Feeder for InfluxDBv1.11.8 that parses txt files and send lines to InfluxDBv1  
  
File:        feed_txt_to_InfluxDBv1.py  
Created:     2025 by Alexandr Strekalovski  
Description:   
&nbsp;&nbsp;&nbsp;This script parses "*.txt log files" for the {MEASUREMENT},  
&nbsp;&nbsp;&nbsp;line by line,  
&nbsp;&nbsp;&nbsp;starting with "starting_file.txt" through the last file in the same directory (ordered by mtime)  
&nbsp;&nbsp;&nbsp;and sends data to InfluxDB v1.11.8  
  
Usage:  
&nbsp;&nbsp;&nbsp;python3 feed_txt_to_InfluxDBv1.py /path/to/starting_file.txt  
  
Attention:  
&nbsp;&nbsp;&nbsp;Redefine the "process_line" function!  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;The line parser is described in "def process_line".  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;It was written for my custom measurement format, so just redefine the "process_line" function for your format.  
