#!/usr/bin/env python

import subprocess as sp
import sys, re


fd = open("/home/an/test_email5", "r")
msg = fd.read()
fd.close()

#ls = sp.Popen(["ls", "-la"], stdin=sp.PIPE, stdout=sp.PIPE).communicate()[0]
#print ls

rc = sp.Popen(["/usr/bin/spamc", "--headers"], stdin=sp.PIPE, stdout=sp.PIPE)
mail = rc.communicate(msg)[0]
#print mail


for i in mail.split('\n'):
	x_spam = re.search("X-Spam-Status:", i)
	if x_spam is not None:
		i = i.split(' ')
		print i

		spam_status = i[1]
		print "spam_status: %s" % spam_status[:-1]

		spam_score = i[2]
		#spam_score = re.search("score=", i)
		print "spam_score: %s" % spam_score.split('=')[1]
		#print "spam_score: %s" % spam_score

		#spam_required = i.split(' ')[3]
                #print "spam_required: %s" % spam_required.split('=')[1]




#m = re.search ('X-Spam-Status: ', OUT)
#if m is not None:
#	print m

#rc = sp.Popen("pwd", stdout=sp.PIPE).communicate()[0]
#print rc

#import os
#f = os.popen("/usr/bin/spamc --headers < /home/an/test_email3").read()
#print f

