#!/usr/bin/env python

# DKIM log agent create log files for DKIM analyser.

import os, sys, getopt, re, agn

class Loghandler (object):
	fname = '/var/log/maillog'
	pattern = re.compile ('^[A-Z][a-z]{2} +[0-9]+ +[0-9]{2}:[0-9]{2}:[0-9]{2} +[^ ]+ +sendmail\\[[0-9]+\\]: +([A-Za-z0-9]+):')
	statFile = agn.mkpath (agn.base, 'var', 'run', 'create-log.pos')
	logStore = agn.mkpath (agn.base, 'var', 'spool', 'sendmail-log')
	
	def readStat (self):
		if os.path.isfile (self.statFile):
			with open (self.statFile, 'r') as fd:
				try:
					parts = [int (_f) for _f in fd.read ().strip ().split (':')]
					self.stat = agn.struct (position = parts[0], inode = parts[1])
				except ValueError:
					pass
		else:
			self.stat = None
	
	def writeStat (self, fd):
		with open (self.statFile, 'w') as fdp:
			st = os.fstat (fd.fileno ())
			fdp.write ('%d:%d\n' % (fd.tell (), st.st_ino))
		
	def __init__ (self):
		self.stat = None

	def createLogs (self):
		self.readStat ()
		fdi = open (self.fname, 'r')
		fds = [fdi]
		st = os.fstat (fdi.fileno ())
		if self.stat is not None:
			if self.stat.inode == st.st_ino:
				if self.stat.position > 0:
					fdi.seek (self.stat.position)
			else:
				directory = os.path.dirname (self.fname)
				for filename in os.listdir (directory):
					path = agn.mkpath (directory, filename)
					st = os.stat (path)
					if st.st_ino == self.stat.inode:
						fdy = open (path, 'r')
						if self.stat.position > 0:
							fdy.seek (self.stat.position)
						fds.insert (0, fdy)
						break
		while fds:
			fd = fds.pop (0)
			for line in fd:
				mtch = self.pattern.match (line)
				if mtch is not None:
					id = mtch.group (1)
					with open (agn.mkpath (self.logStore, '%s.log' % id), 'a') as fdo:
						fdo.write (line)
			if not fds:
				self.writeStat (fd)
			fd.close ()

def main ():
	(opts, param) = getopt.getopt (sys.argv[1:], 'v')
	for opt in opts:
		if opt[0] == '-v':
			agn.outstream = sys.stdout
			agn.outlevel = agn.LV_DEBUG
	agn.lock ()
	agn.log (agn.LV_INFO, 'main', 'Starting up')
	handler = Loghandler ()
	handler.createLogs ()
	agn.log (agn.LV_INFO, 'main', 'Going down')
	agn.unlock ()

if __name__ =='__main__':
	main ()
