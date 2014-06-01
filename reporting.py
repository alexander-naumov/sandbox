#!/usr/bin/env python

import agn, sys, os, datetime
import eagn

db = agn.DBaseID ('db.name')
db.open ()
c = db.cursor ()

x = c.queryc ('SELECT time,sender,recipient,status FROM dkim_client_stat_tbl ORDER BY time')
report_time = datetime.date.today() - datetime.timedelta(7)

y = str(datetime.date.today())
filename = "/home/transact/report-" + y + ".txt"

with open(filename, "w") as file:
        file.write("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n")
        file.write("        DATE/TIME       | \t\tSENDER\t\t|               RECIPIENT               | \tSTATUS\n")
        file.write("-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n")

        for i in x.data:
		if i[0].date() > report_time:
			if len(i[1]) < 16:
				if len(i[2]) < 11:
                               		file.write("  %s\t|  %s  \t\t|  %s \t\t\t|  %s  \n" % (i[0], i[1], i[2], i[3]))
				if len(i[2]) > 10 and len(i[2]) < 16:
					file.write("  %s\t|  %s  \t\t|  %s  \t\t\t|  %s  \n" % (i[0], i[1], i[2], i[3]))
				if len(i[2]) > 15 and len(i[2]) < 21:
					file.write("  %s\t|  %s  \t\t|  %s \t\t|  %s  \n" % (i[0], i[1], i[2], i[3]))
				if len(i[2]) > 20:
                                	file.write("  %s\t|  %s  \t\t|  %s \t\t|  %s  \n" % (i[0], i[1], i[2], i[3]))

			if len(i[1]) > 15:
                        	if len(i[2]) < 11:
                                	file.write("  %s\t|  %s  \t|  %s \t\t|  %s  \n" % (i[0], i[1], i[2], i[3]))
                        	if len(i[2]) > 10 and len(i[2]) < 16:
                                	file.write("  %s\t|  %s  \t|  %s \t\t\t|  %s  \n" % (i[0], i[1], i[2], i[3]))
                        	if len(i[2]) > 15 and len(i[2]) < 21:
                                	file.write("  %s\t|  %s  \t|  %s     \t\t|  %s  \n" % (i[0], i[1], i[2], i[3]))
                        	if len(i[2]) > 20 and len(i[2]) < 26:
                                	file.write("  %s\t|  %s  \t|  %s \t\t|  %s  \n" % (i[0], i[1], i[2], i[3]))
				if len(i[2]) > 25 and len(i[2]) < 28:
                                	file.write("  %s\t|  %s  \t|  %s \t\t|  %s  \n" % (i[0], i[1], i[2], i[3]))
				if len(i[2]) > 27 and len(i[2]) < 31:
                                	file.write("  %s\t|  %s  \t|  %s \t|  %s  \n" % (i[0], i[1], i[2], i[3]))
				if len(i[2]) > 30:
                                	file.write("  %s\t|  %s  \t|  %s\t|  %s\n" % (i[0], i[1], i[2], i[3]))

fd = open (filename, 'r')
content = fd.read ()
fd.close ()

#
# These values should come from the database
#
sender = 'your@email.de'
subject = 'Woechentliche Relay-Statistik'
body = """Sehr geehrte Damen und Herren,

im Anhang finden Sie ihre woechentliche Statistik zum Versand der E-Mails
ueber das Mailrelay.

Ihr Support-Team"""

eagn.EMail.forceEncoding ('UTF-8', 'qp')
email = eagn.EMail ()
email.setSender (sender)
email.setSubject (subject)
email.setText (body)
email.addTextAttachment (content, charset = 'UTF-8', filename = os.path.basename (filename))



x = c.queryc ('SELECT receiver FROM dkim_client_mail_tbl WHERE client_id IN (SELECT client_id FROM dkim_client_info_tbl WHERE status = \'active\')')

for receiver in x.data:
	receiver = str(receiver)[2:-3]
	email.addTo (receiver)
(status, rc, stdout, stderr) = email.sendMail ()
if not status:
	pass #Error handling goes here
