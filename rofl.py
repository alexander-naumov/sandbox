#!/usr/bin/env python

import subprocess as sp

stdout = sp.Popen(["locate", "spamc"], stdout=sp.PIPE).communicate()[0]

if len(stdout) == 0:
	print "program is not there"

else:
	print "programm is there" 
