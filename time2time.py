#!/usr/bin/env python

import agn, sys, datetime, time


db = agn.DBaseID ('db.name')
db.open ()
c = db.cursor ()

report_time = datetime.date.today() - datetime.timedelta(22)

print type(report_time)

print report_time

x = c.queryc ('SELECT time,sender,recipient,status FROM dkim_client_stat_tbl ORDER BY time')
for i in x.data:
	#print "=============> ",str(i[0]).split(' ')[0] 
	#date_object = datetime.strptime(str(i[0]).split(' ')[0], '%Y-%m-%d')
	#tbl_date = datetime.datetime.fromtimestamp(time.mktime(time.strptime(str(i[0]).split(' ')[0], '%Y-%m-%d')))
	
	#print "-------------> ", tbl_date, "\t", type(tbl_date)
	
	#report_time = (tbl_date - datetime.timedelta(7)).isoformat()
	#report_time = tbl_date - datetime.timedelta(7)
	
	print ">>>> ", report_time
	#print type(report_time)
	#print (i[0] - datetime.timedelta(7)).isoformat()

	if i[0].date() > report_time:
		print i
