#!/usr/bin/python2.7

from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator = '\t'):
	for line in file:
		yield line.rstrip().split(separator, 1)

def main(separator = '\t'):
    data = read_mapper_output(sys.stdin, separator = separator)
    # Iterate through different groups
    for current_word, group in groupby(data, itemgetter(0)):       
        drivers_onduty = 0
        drivers_occupied = 0
        t_onduty = 0
        t_occupied = 0
        n_pass = 0
        n_trip = 0
        n_mile = 0
        earnings = 0
        #Iterate within a group(same date and same hour)
        #Aggregate the values
        for key, value in group:
            value = value.split(',')
            date = value[0].lstrip('(u\'').rstrip('\'')
            hour = value[1].strip()
            t_onduty += float(value[3].strip())
            if (t_occupied > 0):
                drivers_occupied += 1
            drivers_onduty = drivers_occupied
            t_occupied += float(value[4].strip())
            n_pass += float(value[5].strip())
            n_trip += float(value[6].strip())
            n_mile += float(value[7].strip())
            earnings += float(value[8].rstrip(')').strip())
             
        output_info = date, hour, drivers_onduty, drivers_occupied, t_onduty,t_occupied, n_pass, n_trip, n_mile, earnings
        print output_info


if __name__ == "__main__":
	main()

