#!/usr/bin/env python2.7
import sys

# Data streaming
for line in sys.stdin:
    line = line.strip()
    
    key, value = line.split('\t')
    keys = key.split(',')
    index = 0
    new_key = ''
    # Use the hacker_licence and time(year and month) as the key
    for ky in keys:         
        ky = ky.strip()
        ky = ky.lstrip('(')
        ky = ky.rstrip(')')
        ky = ky.strip()
        ky = ky.lstrip('\'')
        ky = ky.rstrip('\'')
        if (index is 1 and len(ky) is 19):
            ky = ' ' + ky[0:7]  # Extract the year and month
        new_key += ky
        index += 1
    print '%s%s%s' % (new_key, "\t", value)
