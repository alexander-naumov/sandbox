#!/usr/bin/env python

import time, agn, datetime, sys, os, re, subprocess as sp


def proc(name):
        agn.log (agn.LV_DEBUG, 'proc', 'Process: \t%s' % name)
	print name

        rc = sp.Popen(["/usr/bin/pgrep", "-f", name], stdout=sp.PIPE)
        pid = rc.communicate()[0]
        pid = pid.split(" ")[0]
	print pid

        if len(str(pid)) == 0:
                agn.log (agn.LV_DEBUG, 'proc', 'No such process found...')
                print "No such process found: " + name
                return 2

        if re.search("\n", pid):
                pid = pid[:-1]

        fd = open("/proc/" + str(pid) + "/status", "r")
        info = fd.read()
        fd.close()

        info = info.split('\n')
        for i in info:
                if re.match("State:", i):
                        i = i.split(" ")
                        i = i[1][1:-1]
                        agn.log (agn.LV_DEBUG, 'proc', 'Status: \t%s' % i)
			print i

                        if i != "sleeping" and i != "running":
                                agn.log (agn.LV_DEBUG, 'proc', 'Process %s is not active' % name)
                                print "Process " + name + " is not active"
                                return 2


if __name__=='__main__':
	name = "sshd"
	proc(name)	
