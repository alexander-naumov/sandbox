#!/usr/bin/env python
import sys, re

#print len(sys.argv)
#print str(sys.argv)

n = 0
l = []

s = sys.stdin.read()
out = s
s = s.split(" ")

for i in s:
        n += 1
        if re.search("Additional", i):
		#print ">>>>>>>>>>>", s[n+1]

		if s[n+1] == "OK\n":
			sys.stdout.write(out)

                elif s[n+1] == "CHECK_NRPE:":# and s[n+2] == "Socket":
                        s = s[:n+1]
                        for i in s:
                                l.append(i)
                                l.append(' ')
                        print ''.join(l), 'Konnte innerhalb von 10 Sekunden keine Verbindung mit Server herstehlen'

                elif s[n+1] == "NRPE:":
                        s = s[:n+1]
                        for i in s:
                                l.append(i)
                                l.append(' ')
                        print ''.join(l), 'Nagios konnte C-Briefe-Status nicht ueberpruefen'


#line = sys.stdin
#print line



#while True:

#s = sys.stdin.read()
#s = s.split(" ")
#for i in s: #s.stdin.read():
#	if not i:
#		break
#	print i


#ls = sys.stdin
#print ls


#ne = sys.stdin.readlines()
#print ne

#import fileinput

#line = fileinput.input()
#print line

#print str(sys.argv)
#ls = input()
#print ls
