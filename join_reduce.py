#!/usr/bin/env python2.7

from itertools import groupby
from operator import itemgetter
import sys

# Data streaming
def read_mapper_output(file, separator = '\t'):
	for line in file:
		yield line.rstrip().split(separator, 1)

# Join the data within the same group
def main(separator = '\t'):
	data = read_mapper_output(sys.stdin, separator = separator)
	for key, group in groupby(data, itemgetter(0)):
		info = ''
		try:
			for key, value in group:
				value = value.lstrip('[')
				value = value.rstrip(']')
				info = info + value + ','	# Join the trip information in trip.dataset and fare dataset into a string
			print "%s%s%s" % (key, separator, info)	# print out the key and value
		except:
			pass

if __name__ == "__main__":
	main()
