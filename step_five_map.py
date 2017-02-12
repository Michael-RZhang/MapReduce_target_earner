#!/usr/bin/python2.7
import sys
import datetime

# Data streaming
for row in sys.stdin:
	row = row.strip()
	key, value = row.split('\t')
	split_value = value.split(',')
	
	# Use date and hour as the key
	key = split_value[0], split_value[1] 
	print '%s%s%s' % (key, "\t", value)
