#!/usr/bin/env python2.7
import sys

# Data streaming
for row in sys.stdin:
	row = row.strip()
	line = row.split(',')

	# Use the hacker_licence and pickup_time as the key
	if (len(line) is 14):	# If it is from the trip dataset
		time_key = line[5]	# Extract the pickup_time
	elif (len(line) is 11):	# If it is from the fare dataset
		time_key = line[3]	# Extract the pickup_time
	key = line[1], time_key

	if (key[0] != ' hack_license'):
		print '%s%s%s' % (key, "\t", line)	# print out the key-value pair

