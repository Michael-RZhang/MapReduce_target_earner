#!/usr/bin/env python2.7

from itertools import groupby
from operator import itemgetter
import sys
import datetime
import math



format = '%Y-%m-%d %H:%M:%S'

# Data streaming
def read_mapper_output(file, separator = '\t'):
	for line in file:
		yield line.rstrip().split(separator, 1)

# Operate the data within the group
def main(separator = '\t'):
    data = read_mapper_output(sys.stdin, separator = separator)
    # Iterate through different groups
    for key, group in groupby(data, itemgetter(0)):
        total_time_occupied = 0
        total_time_onduty = 0
        total_num_pass = 0
        total_num_trips = 0
        total_distance = 0
        total_earnings = 0
        past_drop_time = None
        past_hour = None
        
        # Iterate inside a single group(a month)
        for key, value in group:
            try:
                is_valid = True
                values = value.strip().split(',')                            
                
                #Assigning timing variables
                pickuptime = values[5].strip().lstrip('\'').rstrip('\'')
                pick_time = datetime.datetime.strptime(pickuptime, format)  

                dropofftime = values[6].strip().lstrip('\'').rstrip('\'')
                drop_time = datetime.datetime.strptime(dropofftime, format)   

                #Assigning trip metrics
                hacker_license = values[1].strip().lstrip('\'').rstrip('\'')
                passenger_count = float(values[7].strip().lstrip('\'').rstrip('\''))
                #trip_time_in_secs = float(value[8].strip().lstrip('\'').rstrip('\''))
                trip_distance =  float(values[9].strip().lstrip('\'').rstrip('\''))
                amount = float(values[24].strip().lstrip('\'').rstrip('\''))
                picklong = float(values[10].strip().lstrip('\'').rstrip('\''))
                picklat = float(values[11].strip().lstrip('\'').rstrip('\''))
                droplong = float(values[12].strip().lstrip('\'').rstrip('\''))
                droplat = float(values[13].strip().lstrip('\'').rstrip('\''))
                euc_dist = math.sqrt((droplong - picklong)**2 + (droplat - picklat)**2)
            except:
                is_valid = False
                
            if (is_valid):
            #Sanity checks within the read data
                if (amount <= 0.0 or amount > 1000.0):
                    is_valid = False
                elif (not passenger_count.is_integer() or passenger_count <= 0.0 or passenger_count > 6.0):
                    is_valid = False
                elif (picklong is 0 or picklat is 0 or droplong is 0 or droplat is 0):
                    is_valid = False
                elif (trip_distance <= euc_dist ):
                    is_valid = False
                elif ((drop_time - pick_time).total_seconds() <= 0):
                    is_valid = False
                elif(past_drop_time is not None and ((pick_time - past_drop_time).total_seconds() < 0)):
                    is_valid = False
                elif((drop_time - pick_time).total_seconds() > 45*60 and  (trip_distance / (drop_time - pick_time).total_seconds()) < 0.00278 ):
                    is_valid = False
                        
                
            if(is_valid):
                
                pick_hour = pick_time.hour
                drop_hour = drop_time.hour
                hour_diff = drop_hour - pick_hour
                current_hour = pick_hour

                if(hour_diff is 1 or hour_diff is -23):
                    #Cross-hour trip that exists 2 hour periods
                    total_time = (drop_time - pick_time).total_seconds()    
                    
                    first_fraction = (3600 - (pick_time.minute * 60 + pick_time.second)) / total_time                    
                    second_fraction = (drop_time.minute * 60 + drop_time.second ) / total_time             
                    
                    first_distance = first_fraction * trip_distance
                    second_distance = second_fraction * trip_distance
                    first_amount = first_fraction * amount
                    second_amount = second_fraction * amount
                    first_time = first_fraction * total_time
                    second_time = second_fraction * total_time

                    #Add the first part in totals
                    total_time_occupied += first_time
                    if(past_drop_time is None or (pick_time - past_drop_time).total_seconds() >= 1800):
                        total_time_onduty += first_time
                    else:
                        total_time_onduty = total_time_onduty + first_time + (pick_time - past_drop_time).total_seconds()

                    total_num_pass += passenger_count
                    total_num_trips += 1
                    total_distance += first_distance
                    total_earnings += first_amount
                    #Since this hour is over, print it out
                    if (past_drop_time is None):
                        past_drop_time = pick_time
                        past_hour = pick_hour
                    info = unicode(past_drop_time.date()), past_hour, hacker_license, total_time_onduty, total_time_occupied, total_num_pass, total_num_trips, total_distance, total_earnings
                    print "%s%s%s" % (key, separator, info)
                    total_time_occupied = 0
                    total_time_onduty = 0
                    total_num_pass = 0
                    total_num_trips = 0
                    total_distance = 0
                    total_earnings = 0

                    #Add second part to totals
                    total_time_occupied += second_time                  
                    total_time_onduty += second_time
                    total_num_pass += passenger_count
                    total_num_trips += 1
                    total_distance += second_distance
                    total_earnings += second_amount

                elif (hour_diff > 2 or hour_diff < 0):
                    #Trip cross more than 2 hours, sucha as 3, 4, 5 hrs, or more                 
                    total_time = (drop_time - pick_time).total_seconds()

                    #First hour part             
                    first_fraction = (3600 - (pick_time.minute * 60 + pick_time.second)) / total_time 
                    first_distance = first_fraction * trip_distance
                    first_amount = first_fraction * amount
                    first_time = first_fraction * total_time

                    total_time_occupied += first_time
                    if(past_drop_time is None or (pick_time - past_drop_time).total_seconds() >= 1800):
                        total_time_onduty += first_time
                    else:
                        total_time_onduty = total_time_onduty + first_time + (pick_time - past_drop_time).total_seconds()

                    total_num_pass += passenger_count
                    total_num_trips += 1
                    total_distance += first_distance
                    total_earnings += first_amount

                    #Since this hour is over, print it out
                    if (past_drop_time is None):
                        past_drop_time = pick_time
                        past_hour = pick_hour
                    info = unicode(past_drop_time.date()), past_hour, hacker_license, total_time_onduty, total_time_occupied, total_num_pass, total_num_trips, total_distance, total_earnings
                    print "%s%s%s" % (key, separator, info)
                    total_time_occupied = 0
                    total_time_onduty = 0
                    total_num_pass = 0
                    total_num_trips = 0
                    total_distance = 0
                    total_earnings = 0
                    
                    #Middle part(could be multiple full hours)
                    if(past_hour is 23):
                        past_hour = -1  
                    elif(past_hour is None):
                        past_hour = pick_hour
                            
                    remaining_time = (1.0 - first_fraction) * total_time
                    num_hours = remaining_time / 3600.0
                    num_completed_hours = int((remaining_time / 3600.0) - ((remaining_time % 3600) / 3600))
                    remainder = (num_hours - num_completed_hours) * 3600
                    hourly_fraction = 3600 / total_time                    
                    
                    for x in range(1, num_completed_hours+1):
                        date_disp = past_drop_time.date()
                        hour_disp = past_hour + x 
                        if(hour_disp > 23):
                            hour_disp = hour_disp - 24
                            date_disp = date_disp + datetime.timedelta(days=1)
                        info = unicode(date_disp), hour_disp, hacker_license, 3600, 3600, passenger_count, 1, hourly_fraction * trip_distance, hourly_fraction * amount
                        print "%s%s%s" % (key, separator, info)
                    past_hour = past_hour + num_completed_hours
                    
                    #Last hour part
                    last_fraction = remainder/total_time
                    last_distance = last_fraction * trip_distance
                    last_amount = last_fraction * amount
                    last_time = last_fraction * total_time
                
                    total_time_occupied += last_time                  
                    total_time_onduty += last_time
                    total_num_pass += passenger_count
                    total_num_trips += 1
                    total_distance += last_distance
                    total_earnings += last_amount              
                
                elif(hour_diff is 0 and past_hour is not None and current_hour is not past_hour):
                    #No cross-hour situation and it starts a new hour
                    #Time on_duty check from last drop off (add the rest of the last hour if still on duty)   
                    if ((pick_time - past_drop_time).total_seconds() < 1800 ):
                        total_time_onduty += pick_hour * 3600 - (past_drop_time.hour * 3600 + past_drop_time.minute * 60 + past_drop_time.second)
                  
                    #Single hour period trip, need to print past hour aggregates and reset
                    info = unicode(past_drop_time.date()), past_hour, hacker_license, total_time_onduty, total_time_occupied, total_num_pass, total_num_trips, total_distance, total_earnings
                    print "%s%s%s" % (key, separator, info)
                    total_time_occupied = 0
                    total_time_onduty = 0
                    total_num_pass = 0
                    total_num_trips = 0
                    total_distance = 0
                    total_earnings = 0

                    #Add this trip to total
                    elapsed = drop_time - pick_time
                    t_occupied = elapsed.total_seconds()
                    total_time_occupied += t_occupied
                    if((pick_time - past_drop_time).total_seconds() >= 1800):
                        total_time_onduty += t_occupied
                    else:
                        total_time_onduty = total_time_onduty + t_occupied + pick_time.minute*60 + pick_time.second
                    total_num_pass += passenger_count
                    total_num_trips += 1
                    total_distance += trip_distance
                    total_earnings += amount

                else:
                    #Normal trip in the same hour period
                    #Aggregate current data
                    # where hour_diff is 0 (not a cross-hour trip) and current_hour is past_hour (past trip and this trip are in the same hour)
                    elapsed = drop_time - pick_time
                    t_occupied = elapsed.total_seconds()
                    total_time_occupied += t_occupied
                    if(past_drop_time is None or (pick_time - past_drop_time).total_seconds() >= 1800):
                        total_time_onduty += t_occupied
                    else:
                        total_time_onduty = total_time_onduty + t_occupied + (pick_time - past_drop_time).total_seconds()

                    total_num_pass += passenger_count
                    total_num_trips += 1
                    total_distance += trip_distance
                    total_earnings += amount
                    
            if(is_valid):
                #Keep track of the previous drop-off to compute the t_onduty
                past_drop_time = drop_time 
                past_hour = past_drop_time.hour
            


if __name__ == "__main__":
	main()

