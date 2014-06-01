#!/usr/bin/env python

import	socket
import	agn

headers = [
	'Subject: Testmail!',
	'From: your@email.de'
]

print agn.mailsend ('IP.IP.IP.IP', 'your@email.de', ['relay@email.de'], headers, 'test', socket.getfqdn ())[1]
