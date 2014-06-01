#!/usr/bin/env python

import time, sys, os, datetime
#from datetime import date




d = datetime.datetime.today()
#d = datetime.datetime(2013, 05, 13, 07, 00)
print d

d = d - datetime.timedelta(days = 7)# - datetime.timedelta(hours = 8)
print d
print type(d)


print "=========="


report_time = datetime.datetime.today() - datetime.timedelta(7)
print report_time
print type(report_time)
