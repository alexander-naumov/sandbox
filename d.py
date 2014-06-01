#!/usr/bin/env python

import datetime, time


report_time = datetime.date.today() - datetime.timedelta(7)
print report_time

#report_time = str(report_time)

r = report_time.strftime('%Y%m%d')

print r

#datetime.datetime.fromtimestamp(time.mktime(time.strptime(report_time, "%Y.%m.%d")))

#from datetime import datetime

#format = '%Y-%m-%d'
#d1 = datetime.strptime(report_time, format)


import subprocess as sp

rc = sp.Popen(["/sbin/pgrep", "-f", "\"python", "/home/user/scripts/python.py\""], stdout=sp.PIPE)
pid = rc.communicate()[0]
pid = pid.split(" ")[0]

print pid



#print d1
