#!/usr/bin/env python

import	agn, eagn, dagent


sender = "sender@email.de"
receiver = "receiver@email.de"
subject = "Statistic"
#filename = statistic.cfg.tget ('mail.filename')


if receiver:
	email = eagn.EMail ()
	if sender:
		email.setSender (sender)
	if subject:
		email.setSubject (subject)
	email.addTextAttachment (email, contentType = 'text/plain', filename = "/home/sender/report.txt")
	st = email.sendMail ()
	if not st[0]:
		print "Something goes wrong..."

