#	-*- mode: python; mode: fold -*-
#
"""
Support routines for general and company specific purposes:
	class struct:     general empty class for temp. structured data
	class error:	  new version for general execption
	def chop:         removes trailing newlines
	def atoi:         converts a string to a numeric value (fault tolerant)
	def atob:         converts a string to a boolean value
	def numfmt:       converts a number to pretty printed version
	def validate:     validates an input string
	def filecount:    counts files matching a pattern in a directory
	def which:        finds program in path
	def mkpath:       creates a path from path components
	def fingerprint:  calculates a fingerprint from a file
	def toutf8:       converts input string to UTF-8 encoding
	def fromutf8:     converts UTF-8 encoded strings to unicode

	def splitutf8:    splits UTF-8 encoded string into valid UTF-8
	                  substrings
	def msgn:         output a message on stdout, if verbose ist set
	def msgcnt:       output a number for progress
	def msgfcnt:      output final number
	def msg:          output a message with trailing newline on stdout,
	                  if verbose is set
	def err:          output a message on stderr
	def transformSQLwildcard: transform a SQL wildcard string to a regexp
	def compileSQLwildcard: transform and compile a SQL wildcard
	class Parameter:  decode/encode parameter strings
	class UserStatus: describes available user stati

	class Backlog:    support class for enabling backlogging
	def loglevelName: returns a string representation of a log level
	def loglevelValue:returns a numeric value for a log level
	def logfilename:  creates the filename to write logfiles to
	def logappend:    copies directly to logfile
	def log:          writes an entry to the logfile
	def logexc:       writes details of exception to logfile
	def mark:         writes a mark to the logfile, if nothing had been
	                  written for a descent time
	def backlogEnable: switch backlogging on
	def backlogDisable: switch backlogging off
	def backlogRestart: flush all recorded entries and restart with
	                    a clean buffer
	def backlogSuspend: suspend storing entries to backlog
	def backlogResume: resume storing entries to backlog
	def backlogSave:  write current backlog to logfile

	class LogfileParser: support parsing written logfile

	def lock:         creates a lock for this running process
	def unlock:       removes the lock
	def signallock:   send signal to process owing a lockfile

	def createPath:   creates a full path with all missing subdirectories
	def mkArchiveDirectory:  creates a subdirectory for daily archives
	class Filepos:    line by line file reading with remembering th
	                  the file position
	
	def die:          terminate the program removing aquired lock, if
	                  neccessary
	rip = die         alias for die
	
	class Messenger:  wrapper for subprocess named queueing
	def mcomm:        return bound communicator
	def mcreate:      create a new global channel
	def msend:        send using global channels
	def manser:       answer to global channel request
	def mrecv:        receiver message through global channels
	
	class Parallel:   wrapper for subprocessing, can also be used
	                  externally
	def pfork:        starts a method as subprocess
	def palive:       returns tuple (alive, active) in numbers of
	                  processes
	def pwait:        joins all  subprocess(es)
	def pready:       joins max. one process, if one has terminated
	def pterm:        terminates subprocesses
	
	def mailsend:     send a mail using SMTP

	def httpget:      send simple http requests with one line
	                  answers (+ succes/- failure)
	def startOnDemandMailing: start an on demand mailing via HTTP
	def URLEncode:    encode a string as parameter for URLs
	def URLEncodeHash:encode a whole hash for usage in URLS
	def call:         interrupt save version of subprocess.call
	def fileAccess:   creates list of PIDs of processes accesing a file
	def fileExistance: check existance, content and mode of a file
	class UID:         handles parsing and validation of UIDs

	class XUID:        handles parsing and validation of new UIDs
	class PubID:       handles parsing and validation of Public IDs
	class METAFile:    generic class to parse XML meta file names

	class DBCore:      an abstract core database driver class
	class DBCursor:    a cursor instance for database access
	class DBMySQL:     MySQL driver and cursor
	class DBMySQLCursor:

	class DBOracle:    Oracle driver and cursor
	class DBOracleCursor:
	class DBCursorSQLite3:
	class DBSQLite3:   simple database based on sqlite3
	class DBConfig:    class to read and parse global database
	                   configuration file
	class DBDriver:    factory class for providing proper database
	                   driver for given ID to reference from the
	                   database configuration file
	def DBaseID:       wrapper for DBDriver.request() and replacement
	                   for create a database instance using class DBase()
	
	class Datasource:  easier handling for datasource IDs

	class ParseTimestamp: parser for converting from/to datetime
	class Timestamp:   wrapper to handle timestamp_tbl
	class MessageCatalog: message catalog for templating
	class Template:    simple templating system


	class Remote:      handle remote files and executions via ssh/scp
	class RemotePassword:  same, but for passwort authentication
	class sDNS:	   simple DNS class relies on external command

	def statdProxy:    Creates a XML-RPC client object to access statd

	For access verification (e.g. in SimpleHTTPServer):
	class IPEntry:     represents one (range of) IP address(es)
	class IPAccess:    collection of all allowed IP blocks
	
	class CSVWriter:   Wrapper around CSV module for writing
	class CSVDictWriter: Same for dictionary writing
	class CSVReader:   dito for reading
	class CSVDictReader: dito
	class CSVAuto:     generate a dialect agn-auto with guessing format
	                   from a given file content
	
	class Cache:       cache with limited number of entries
	
	class Family:      mass parallel child framework
"""
#
# Imports, Constants and global Variables
#{{{
import	sys, os, types, errno, stat, signal
import	time, re, socket, subprocess, collections
try:
	import	hashlib
	
	hash_md5 = hashlib.md5
	hash_sha1 = hashlib.sha1
except ImportError:
	import	md5, sha
	
	hash_md5 = md5.md5
	hash_sha1 = sha.sha
import	platform, traceback, codecs
import	smtplib

import	base64, datetime, csv
import	httplib
import	xmlrpclib
#
changelog = [
	('2.0.0', '2008-04-18', 'Initial version of redesigned code', 'ud@agnitas.de'),

	('2.0.1', '2008-06-19', 'Added simple database interface', 'ud@agnitas.de'),
	('2.0.1', '2008-07-01', 'Added autocommitment', 'ud@agnitas.de'),

	('2.0.2', '2008-07-15', 'Added layout for new database for simple databases', 'ud@agnitas.de'),
	('2.0.2', '2008-07-18', 'Set NLS_LANG prior loading cx_Oracle to avoid charset conversion', 'ud@agnitas.de'),
	('2.0.3', '2008-07-31', 'Some oracle optimizations', 'ud@agnitas.de'),
	('2.0.3', '2008-07-31', 'Template with inclusing support', 'ud@agnitas.de'),

	('2.0.4', '2008-08-04', 'Added XMLWriter', 'ud@agnitas.de'),
	('2.0.4', '2008-08-07', 'Added numfmt', 'ud@agnitas.de'),
	('2.0.5', '2008-08-11', 'Added validate', 'ud@agnitas.de'),

	('2.0.6', '2008-08-21', 'Added class Timestamp', 'ud@agnitas.de'),
	('2.0.7', '2008-09-12', 'Added class CSVWriter/CSVReader', 'ud@agnitas.de'),
	('2.0.8', '2008-11-04', 'Added customLogfilename', 'ud@agnitas.de'),

	('2.1.0', '2008-11-20', 'Added support for Oracle to set input/output size', 'ud@agnitas.de'),
	('2.1.1', '2008-11-27', 'Added function startOnDemandMailing to trigger an on demand mailing', 'ud@agnitas.de'),
	('2.1.2', '2008-12-10', 'Added cache with limitation', 'ud@agnitas.de'),
	('2.1.3', '2009-01-13', 'Fixed bug in template including', 'ud@agnitas.de'),
	('2.1.4', '2009-01-15', 'Add MessageCatalog for templating', 'ud@agnitas.de'),

	('2.1.5', '2009-01-23', 'Support for parsing licence ID in UID', 'ud@agnitas.de'),

	('2.1.6', '2009-02-09', 'Added CSVDictWriter/CSVDictReader', 'ud@agnitas.de'),
	('2.1.7', '2009-04-29', 'Added createPath', 'ud@agnitas.de'),
	('2.1.8', '2009-05-06', 'Added logexc', 'ud@agnitas.de'),

	('2.1.9', '2009-05-06', 'Added sDNS', 'ud@agnitas.de'),
	('2.2.1', '2009-05-18', 'Added @deprecated decorator', 'ud@agnitas.de'),

	('2.2.2', '2009-05-26', 'Modified sDNS interface to allow different helper', 'ud@agnitas.de'),
	('2.2.3', '2009-07-28', 'validate: Added support for keyword reason', 'ud@agnitas.de'),
	('2.2.5', '2009-11-09', 'Extend class struct with __init__ method', 'ud@agnitas.de'),
	('2.2.6', '2009-11-17', 'Minor bugfix in createPath on relative pathes', 'ud@agnitas.de'),

	('2.2.7', '2010-02-16', 'Extend XMLWriter', 'ud@agnitas.de'),
	('2.2.8', '2010-02-26', 'Added class Family', 'ud@agnitas.de'),
	('2.3.0', '2010-03-09', 'Added logging for database interface', 'ud@agnitas.de'),

	('2.3.1', '2010-03-09', 'Added class PubID', 'ud@agnitas.de'),
	('2.3.2', '2010-04-14', 'Removed non working depracted decorator', 'ud@agnitas.de'),
	('2.3.3', '2010-04-14', 'Added general stack trace generator', 'ud@agnitas.de'),
	('2.4.0', '2010-04-21', 'Added class METAFile', 'ud@agnitas.de'),

	('2.4.1', '2010-05-19', 'Added class XUID', 'ud@agnitas.de'),
	('2.4.2', '2010-09-09', 'Added CSVAuto to auto recognize CSV data', 'ud@agnitas.de'),
	('2.5.0', '2010-12-13', 'Added XLSWriter', 'ud@agnitas.de'),
	('2.5.1', '2010-12-14', 'Added XLSReader', 'ud@agnitas.de'),
	('2.5.2', '2010-12-21', 'Modified CSV* classes to accept a stream as well as a filename', 'ud@agnitas.de'),
	('2.5.3', '2010-12-24', 'Support forumlae in XLSWriter', 'ud@agnitas.de'),
	('2.5.4', '2010-12-28', 'Support splits and outlines in XLSWriter', 'ud@agnitas.de'),
	('2.6.0', '2010-12-29', 'Removed obsolete customLogfilename', 'ud@agnitas.de'),

	('2.6.1', '2010-12-29', 'Extended XMLWriter with output character set', 'ud@agnitas.de'),
	('2.6.2', '2011-01-03', 'Added support for hidden rows and columns in XLSWriter', 'ud@agnitas.de'),
	('2.6.2', '2011-01-03', 'Added support to accept styles as list/tuple in XLSWriter', 'ud@agnitas.de'),
	('2.7.0', '2011-01-17', 'Moved XLS* classes to dagent', 'ud@agnitas.de'),
	('2.7.1', '2011-03-14', 'Added support to parse ancient UIDs', 'ud@agnitas.de'),
	('2.7.3', '2011-03-15', 'Changed toutf8 for better handling of passed charset', 'ud@agnitas.de'),
	('2.7.4', '2011-03-29', 'Enhanced locking to treat empty lock files as invalid', 'ud@agnitas.de'),

	('2.7.5', '2011-03-31', 'Enhanced version check in require from simple to numeric', 'ud@agnitas.de'),
	('2.7.6', '2011-04-01', 'Handle invalid UTF-8 passwords during XUID generation', 'ud@agnitas.de'),
	('2.7.7', '2011-04-14', 'Handle invalid sequence of UTF-8 as generator', 'ud@agnitas.de'),
	('2.7.8', '2011-04-19', 'Added splitutf8', 'ud@agnitas.de'),
	('2.8.0', '2011-04-27', 'Moved XMLWriter to separate XML library file xagn.py', 'ud@agnitas.de'),
	('2.8.1', '2011-05-31', 'Added fileExistance', 'ud@agnitas.de'),
	('2.8.2', '2011-08-16', 'Reworked database interface', 'ud@agnitas.de'),
	('2.8.3', '2011-08-18', 'Added simple parallel infrastructure using pfork, pwait and pterm', 'ud@agnitas.de'),
	('2.8.4', '2011-08-19', 'Added atoi, a fault tolerant variant of int(...)', 'ud@agnitas.de'),
	('2.8.5', '2011-08-22', 'Enhanced simple parallel infrastructure', 'ud@agnitas.de'),
	('2.8.6', '2011-10-18', 'Added simple multiprocessing based messenger system', 'ud@agnitas.de'),

	('2.8.7', '2011-10-19', 'Enhanced class Cache for timeout value', 'ud@agnitas.de'),
	('2.8.8', '2011-10-28', 'Better debuging for Family handling', 'ud@agnitas.de'),
	('2.8.9', '2012-02-06', 'Added queryp/querypc/queryps for DBcursor for keyword parameters', 'ud@agnitas.de'),
	('2.9.0', '2012-04-05', 'Added keyword query results', 'ud@agnitas.de'),

	('2.9.1', '2012-04-10', 'Added external configuration for database access parameter', 'ud@agnitas.de'),
	('2.9.2', '2012-04-12', 'Added an initial timestamp for class Timestamp', 'ud@agnitas.de'),
	('2.9.3', '2012-05-02', 'Enhanced class Timestamp for cascading operation', 'ud@agnitas.de'),
	('2.9.4', '2012-05-04', 'Enhanced DBSQLite3 class', 'ud@agnitas.de'),
	('2.9.5', '2012-05-15', 'Created dedicated class for timestamp parsing', 'ud@agnitas.de'),
	('2.9.6', '2012-06-27', 'RemotePassword: make prompt and passphrase optional', 'ud@agnitas.de'),
	('2.9.7', '2012-07-04', 'LogfileParser added', 'ud@agnitas.de'),
	('2.9.8', '2012-07-09', 'Revised database interface', 'ud@agnitas.de'),
	('2.9.9', '2013-02-14', 'Added class Parameter', 'ud@agnitas.de'),
]
version = (changelog[-1][0], '2013-03-01 11:27:17 CET', 'an')

#
licence = 1
#
verbose = 1
system = platform.system ().lower ()
host = platform.node ()
if host.find ('.') != -1:
	host = host.split ('.')[0]
#
try:
	base = os.environ['HOME']
except KeyError:
	base = '.'
scripts = os.path.sep.join ([base, 'scripts'])
if not scripts in sys.path:
	sys.path.insert (0, scripts)
del scripts
#}}}
#
# Support routines
#
#{{{
class struct:
	"""class struct:

General empty class as placeholder for temp. structured data"""
	def __init__ (self, **kws):
		for (var, val) in kws.items ():
			if not var.startswith ('_'):
				self.__dict__[var] = val

	def __str__ (self):
		return '[%s]' % ', '.join (['%s=%r' % (_n, self.__dict__[_n]) for _n in self.__dict__])

class error (Exception):
	"""class error (Exception):

This is a general exception thrown by this module."""
	def __init__ (self, message = None):
		Exception.__init__ (self, message)
		self.msg = message

def __require (checkversion, srcversion, modulename):
	for (c, v) in zip (checkversion.split ('.'), srcversion[0].split ('.')):
		cv = int (c)
		vv = int (v)
		if cv > vv:
			raise error ('%s: Version too low, require at least %s, found %s' % (modulename, checkversion, srcversion[0]))
		elif cv < vv:
			break
	if checkversion.split ('.')[0] != srcversion[0].split ('.')[0]:
		raise error ('%s: Major version mismatch, %s is required, %s is available' % (modulename, checkversion.split ('.')[0], srcversion[0].split ('.')[0]))

def require (checkversion , checklicence = None):
	__require (checkversion, version, 'agn')

	if not checklicence is None and checklicence != licence:
		raise error ('Licence mismatch, require %d, but having %d' % (checklicence, licence))

def chop (s):
	"""def chop (s):

removes any trailing LFs and CRs."""
	while len (s) > 0 and s[-1] in '\r\n':
		s = s[:-1]
	return s

def atoi (s, base = 10, dflt = 0):
	"""def atoi (s, base = 10, dflt = 0):

parses input parameter as numeric value, use default if
it is not parsable."""
	if type (s) in (int, long):
		return s
	try:
		rc = int (s, base)
	except (ValueError, TypeError):
		rc = dflt
	return rc

def atob (s):
	"""def atob (s):

tries to interpret the incoming string as a boolean value."""
	if type (s) is bool:
		return s
	if s and len (s) > 0 and s[0] in [ '1', 'T', 't', 'Y', 'y', '+' ]:
		return True
	return False

def numfmt (n, separator = '.'):
	"""def numfmt (n, separator = '.'):

convert the number to a more readble form using separator."""
	if n == 0:
		return '0'
	if n < 0:
		prefix = '-'
		n = -n
	else:
		prefix = ''
	rc = ''
	while n > 0:
		if n >= 1000:
			rc = '%s%03d%s' % (separator, n % 1000, rc)
		else:
			rc = '%d%s' % (n, rc)
		n /= 1000
	return prefix + rc

def validate (s, pattern, *funcs, **kw):
	"""def validate (s, pattern, *funcs, **kw):

pattern is a regular expression where s is matched against.
Each group element is validated against a function found in funcs."""
	if not pattern.startswith ('^'):
		pattern = '^' + pattern
	if not pattern.endswith ('$') or pattern.endswith ('\\$'):
		pattern += '$'
	try:
		reflags = kw['flags']
	except KeyError:
		reflags = 0
	try:
		pat = re.compile (pattern, reflags)
	except Exception, e:
		raise error ('Failed to compile regular expression "%s": %s' % (pattern, e.args[0]))
	mtch = pat.match (s)
	if mtch is None:
		try:
			reason = kw['reason']
		except KeyError:
			reason = 'No match'
		raise error (reason)
	if len (funcs) > 0:
		flen = len (funcs)
		n = 0
		report = []
		grps = mtch.groups ()
		if not grps:
			grps = [mtch.group ()]
		for elem in grps:
			if n < flen:
				if type (funcs[n]) in (types.ListType, types.TupleType):
					(func, reason) = funcs[n]
				else:
					func = funcs[n]
					reason = '%r' % func
				if not func (elem):
					report.append ('Failed in group #%d: %s' % (n + 1, reason))
			n += 1
		if report:
			raise error ('Validation failed: %s' % ', '.join (report))

def filecount (directory, pattern):
	"""def filecount (directory, pattern):

counts the files in dir which are matching the regular expression
in pattern."""
	pat = re.compile (pattern)
	dirlist = os.listdir (directory)
	count = 0
	for fname in dirlist:
		if pat.search (fname):
			count += 1
	return count

def which (program):
	"""def which (program):

finds 'program' in the $PATH enviroment, returns None, if not available."""
	rc = None
	try:
		paths = os.environ['PATH'].split (':')
	except KeyError:
		paths = []
	for path in paths:
		if path:
			p = os.path.sep.join ([path, program])
		else:
			p = program
		if os.access (p, os.X_OK):
			rc = p
			break
	return rc

def mkpath (*parts, **opts):
	"""def mkpath (*parts, **opts):

create a valid pathname from the elements"""
	try:
		absolute = opts['absolute']
	except KeyError:
		absolute = False
	rc = os.path.sep.join (parts)
	if absolute and not rc.startswith (os.path.sep):
		rc = os.path.sep + rc
	return os.path.normpath (rc)

def fingerprint (fname):
	"""def fingerprint (fname):

calculates a MD5 hashvalue (a fingerprint) of a given file."""
	fp = hash_md5 ()
	fd = open (fname, 'rb')
	while 1:
		chunk = fd.read (65536)
		if chunk == '':
			break
		fp.update (chunk)
	fd.close ()
	return fp.hexdigest ()

__encoder = codecs.getencoder ('UTF-8')
def toutf8 (s, charset = 'ISO-8859-1'):
	"""def toutf8 (s, [charset]):

convert unicode (or string with charset information) inputstring
to UTF-8 string."""
	if type (s) == types.StringType:
		if charset is None:
			s = unicode (s)
		else:
			s = unicode (s, charset)
	return __encoder (s)[0]
def fromutf8 (s):
	"""def fromutf8 (s):

converts an UTF-8 coded string to a unicode string."""
	return unicode (s, 'UTF-8')

def splitutf8 (s, length):
	"""def splitutf8 (s, length):

splits a string to an array of strings with at most
length bytes while ensuring keeping these substrings
valid UTF-8 strings."""
	utf8start = lambda a: ord (a) & 0xc0 == 0xc0
	utf8cont = lambda a: ord (a) & 0xc0 == 0x80
	rc = []
	if type (s) == types.UnicodeType:
		s = toutf8 (s)
	while s:
		clen = len (s)
		if clen > length:
			clen = length
			if utf8cont (s[clen]):
				while clen > 0 and not utf8start (s[clen]):
					clen -= 1
		if clen == 0:
			raise error ('reduced block to zero bytes, aborting')
		rc.append (s[:clen])
		s = s[clen:]
	return rc

def msgn (s):
	"""def msgn (s):

prints s to stdout, if the module variable verbose is not equal to 0."""
	global	verbose

	if verbose:
		sys.stdout.write (s)
		sys.stdout.flush ()
def msgcnt (cnt):
	"""def msgcnt (cnt):

prints a counter to stdout. If the number has more than eight digits, this
function will fail. msgn() is used for the output itself."""
	msgn ('%8d\b\b\b\b\b\b\b\b' % cnt)
def msgfcnt (cnt):
	msgn ('%8d' % cnt)
def msg (s):
	"""def msg (s):

prints s with a newline appended to stdout. msgn() is used for the output
itself."""
	msgn (s + '\n')
def err (s):
	"""def err (s):

prints s with a newline appended to stderr."""
	sys.stderr.write (s + '\n')
	sys.stderr.flush ()

def transformSQLwildcard (s):
	r = ''
	needFinal = True
	for ch in s:
		needFinal = True
		if ch in '$^*?()+[{]}|\\.':
			r += '\\%s' % ch
		elif ch == '%':
			r += '.*'
			needFinal = False
		elif ch == '_':
			r += '.'
		else:
			r += ch
	if needFinal:
		r += '$'
	return r
def compileSQLwildcard (s, reFlags = 0):
	return re.compile (transformSQLwildcard (s), reFlags)

class Parameter (object):
	skipPattern = re.compile (',[ \t]*')
	decodePattern = re.compile ('([a-z0-9_-]+)[ \t]*=[ \t]*"([^"]*)"', re.IGNORECASE | re.MULTILINE)
	def __decode (self, ctx, s, target):
		while s:
			mtch = self.skipPattern.match (s)
			if mtch is not None:
				s = s[mtch.end ():]
			mtch = self.decodePattern.match (s)
			if mtch is not None:
				(var, val) = mtch.groups ()
				target[var] = val
				s = s[mtch.end ():]
			else:
				break
	
	def __encode (self, ctx, source):
		for value in source.values ():
			if '"' in value:
				raise ValueError ('Unable to enocde %s due to "')
		return ', '.join (['%s="%s"' % (str (_d[0]), str (_d[1])) for _d in source.items ()])

	def __init__ (self, s = None):
		self.methods = {}
		self.addMethod (None, self.__decode, self.__encode)
		try:
			import	json

			def jsonDecode (ctx, s, target):
				d = ctx.jdecode.decode (s)
				if type (d) != dict:
					raise ValueError ('JSON: input %r did not lead into a dictionary' % s)
				for (var, val) in d.items ():
					target[var] = val
			def jsonEncode (ctx, source):
				return ctx.jencode.encode (source)
			temp = self.addMethod ('json', jsonDecode, jsonEncode)
			temp.jdecode = json.JSONDecoder ()
			temp.jencode = json.JSONEncoder ()
		except ImportError:
			pass
		self.data = {}
		if s is not None:
			self.loads (s)
	
	def __getitem__ (self, var):
		return self.data[var]
	
	def __setitem__ (self, var, val):
		self.data[var] = val
	
	def __contains__ (self, var):
		return var in self.data
	
	def __iter__ (self):
		return self.data
	
	def __call__ (self, var, dflt = None):
		return self.get (var, dflt)
	
	def get (self, var, dflt = None):
		try:
			return self.data[var]
		except KeyError:
			return dflt
		
	def iget (self, var, dflt = 0):
		try:
			return int (self.data[var])
		except (KeyError, ValueError):
			return dflt
	
	def fget (self, var, dflt = 0.0):
		try:
			return float (self.data[var])
		except (KeyError, ValueError):
			return dflt
	
	def bget (self, var, dflt = False):
		try:
			return atob (self.data[var])
		except KeyError:
			return dflt

	def addMethod (self, method, decoder, encoder):
		m = struct (decode = decoder, encode = encoder)
		self.methods[method] = m
		return m
	
	def hasMethod (self, method):
		return method in self.methods
		
	methodPattern = re.compile ('^([a-z]+):(.*)$')
	def loads (self, s):
		mtch = self.methodPattern.match (s)
		if mtch is not None:
			(method, s) = mtch.groups ()
			if method not in self.methods:
				raise LookupError ('Unknown decode method: %s' % method)
		else:
			method = None
		m = self.methods[method]
		m.decode (m, s, self.data)
	
	def dumps (self, method = None):
		if method not in self.methods:
			raise LookupError ('Unknown encode method: %s' % method)
		m = self.methods[method]
		return m.encode (m, self.data)
		
	
class UserStatus:
	UNSET = 0
	ACTIVE = 1
	BOUNCE = 2
	ADMOUT = 3
	OPTOUT = 4
	WAITCONFIRM = 5
	BLACKLIST = 6
	SUSPEND = 7
	stati = { 'unset': UNSET,
		  'active': ACTIVE,
		  'bounce': BOUNCE,
		  'admout': ADMOUT,
		  'optout': OPTOUT,
		  'waitconfirm': WAITCONFIRM,
		  'blacklist': BLACKLIST,
		  'suspend': SUSPEND
		}
	rstati = None
	
	def __init__ (self):
		if self.rstati is None:
			self.rstati = {}
			for (var, val) in self.stati.items ():
				self.rstati[val] = var
	
	def findStatus (self, st, dflt = None):
		rc = None
		if type (st) in types.StringTypes:
			try:
				rc = self.stati[st]
			except KeyError:
				rc = None
		if rc is None:
			try:
				rc = int (st)
			except ValueError:
				rc = None
		if rc is None:
			rc = dflt
		return rc
	
	def findStatusName (self, stid):
		try:
			rc = self.rstati[stid]
		except KeyError:
			rc = None
		return rc
#}}}
#
# 1.) Logging
#
#{{{
class Backlog:
	def __init__ (self, maxcount, level):
		self.maxcount = maxcount
		self.level = level
		self.backlog = []
		self.count = 0
		self.isSuspended = False
		self.asave = None
	
	def add (self, s):
		if not self.isSuspended and self.maxcount:
			if self.maxcount > 0 and self.count >= self.maxcount:
				self.backlog.pop (0)
			else:
				self.count += 1
			self.backlog.append (s)
	
	def suspend (self):
		self.isSuspended = True
	
	def resume (self):
		self.isSuspended = False
	
	def restart (self):
		self.backlog = []
		self.count = 0
		self.isSuspended = False
	
	def save (self):
		if self.count > 0:
			self.backlog.insert (0, '-------------------- BEGIN BACKLOG --------------------\n')
			self.backlog.append ('--------------------  END BACKLOG  --------------------\n')
			logappend (self.backlog)
			self.backlog = []
			self.count = 0
	
	def autosave (self, level):
		if not self.asave is None and level in self.asave:
			return True
		return False
	
	def addLevelForAutosave (self, level):
		if self.asave is None:
			self.asave = [level]
		elif not level in self.asave:
			self.asave.append (level)
	
	def removeLevelForAutosave (self, level):
		if not self.asave is None and level in self.asave:
			self.asave.remove (level)
			if not self.asave:
				self.asave = None
	
	def clearLevelForAutosave (self):
		self.asave = None
	
	def setLevelForAutosave (self, levels):
		if levels:
			self.asave = levels
		else:
			self.asave = None

LV_NONE = 0
LV_FATAL = 1
LV_REPORT = 2
LV_ERROR = 3
LV_WARNING = 4
LV_NOTICE = 5
LV_INFO = 6
LV_VERBOSE = 7
LV_DEBUG = 8
loglevel = LV_WARNING
logtable = {	'FATAL': LV_FATAL,
		'REPORT': LV_REPORT,
		'ERROR': LV_ERROR,
		'WARNING': LV_WARNING,
		'NOTICE': LV_NOTICE,
		'INFO': LV_INFO,
		'VERBOSE': LV_VERBOSE,
		'DEBUG': LV_DEBUG
}
for __tmp in logtable.keys ():
	logtable[logtable[__tmp]] = __tmp
loghost = host
logname = None
logpath = None
outlevel = LV_FATAL
outstream = None
backlog = None
try:
	logpath = os.environ['LOG_HOME']
except KeyError:
	logpath = mkpath (base, 'var', 'log')
if len (sys.argv) > 0:
	logname = os.path.basename (sys.argv[0])
	(basename, extension) = os.path.splitext (logname)
	if extension.lower ().startswith ('.py'):
		logname = basename
if not logname:
	logname = 'unset'
loglast = 0
#
def loglevelName (lvl):
	"""def loglevelName (lvl):

returns a name for a numeric loglevel."""
	try:
		return logtable[lvl]
	except KeyError:
		return str (lvl)

def loglevelValue (lvlname):
	"""def loglevelValue (lvlname):

return the numeric value for a loglevel."""
	name = lvlname.upper ().strip ()
	try:
		return logtable[name]
	except KeyError:
		for k in [_k for _k in logtable.keys () if type (_k) == types.StringType]:
			if k.startswith (name):
				return logtable[k]
	raise error ('Unknown log level name "%s"' % lvlname)

def logfilename (name = None, epoch = None):
	global	logname, logpath, loghost
	
	if name is None:
		name = logname
	if epoch is None:
		epoch = time.time ()
	now = time.localtime (epoch)
	return mkpath (logpath, '%04d%02d%02d-%s-%s.log' % (now[0], now[1], now[2], loghost, name))

def logappend (s):
	global	loglast

	fname = logfilename ()
	try:
		fd = open (fname, 'a')
		if type (s) in types.StringTypes:
			fd.write (s)
		elif type (s) in (types.ListType, types.TupleType):
			for l in s:
				fd.write (l)
		else:
			fd.write (str (s) + '\n')
		fd.close ()
		loglast = int (time.time ())
	except Exception, e:
		err ('LOGFILE write failed[%r, %r]: %r' % (type (e), e.args, s))

def log (lvl, ident, s):
	global	loglevel, logname, backlog

	if not backlog is None and backlog.autosave (lvl):
		backlog.save ()
		backlogIgnore = True
	else:
		backlogIgnore = False
	if lvl <= loglevel or \
	   (lvl <= outlevel and not outstream is None) or \
	   (not backlog is None and lvl <= backlog.level):
		if not ident:
			ident = logname
		now = time.localtime (time.time ())
		lstr = '[%02d.%02d.%04d  %02d:%02d:%02d] %d %s/%s: %s\n' % (now[2], now[1], now[0], now[3], now[4], now[5], os.getpid (), loglevelName (lvl), ident, s)
		if lvl <= loglevel:
			logappend (lstr)
		else:
			backlogIgnore = False
		if lvl <= outlevel and not outstream is None:
			outstream.write (lstr)
			outstream.flush ()
		if not backlogIgnore and not backlog is None and lvl <= backlog.level:
			backlog.add (lstr)

def logexc (lvl, ident, s = None):
	exc = sys.exc_info ()
	if not s is None:
		log (lvl, ident, s)
	if not None in exc:
		(typ, value, tb) = exc
		for l in [_l for _l in ('\n'.join (traceback.format_exception (typ, value, tb))).split ('\n') if _l]:
			log (lvl, ident, l)
		del tb

def mark (lvl, ident, dur = 60):
	global	loglast
	
	now = int (time.time ())
	if loglast + dur * 60 < now:
		log (lvl, ident, '-- MARK --')

def level_name (lvl):
	return loglevelName (lvl)

def backlogEnable (maxcount = 100, level = LV_DEBUG):
	global	backlog
	
	if maxcount == 0:
		backlog = None
	else:
		backlog = Backlog (maxcount, level)

def backlogDisable ():
	global	backlog
	
	backlog = None

def backlogRestart ():
	global	backlog
	
	if not backlog is None:
		backlog.restart ()

def backlogSave ():
	global	backlog
	
	if not backlog is None:
		backlog.save ()

def backlogSuspend ():
	global	backlog
	
	if not backlog is None:
		backlog.suspend ()

def backlogResume ():
	global	backlog
	
	if not backlog is None:
		backlog.resume ()

def logExcept (typ, value, tb):
	ep = traceback.format_exception (typ, value, tb)
	rc = 'CAUGHT EXCEPTION:\n'
	for p in ep:
		rc += p
	backlogSave ()
	log (LV_FATAL, 'except', rc)
	err (rc)
sys.excepthook = logExcept


class LogfileParser (object):
	pattern = re.compile ('^\\[([0-9]{2})\\.([0-9]{2})\\.([0-9]{4})  ([0-9]{2}):([0-9]{2}):([0-9]{2})\\] ([0-9]+) ([A-Z]+)(/([^ :]+))?: (.*)$')
	def __init__ (self, filename = None):
		if filename is None:
			filename = logfilename ()
		self.filename = filename
	
	def parseLine (self, line):
		m = self.pattern.match (line.strip ())
		if m is None:
			return None
		g = m.groups ()
		rc = struct (timestamp = datetime.datetime (int (g[2]), int (g[1]), int (g[0]), int (g[3]), int (g[4]), int (g[5])), pid = int (g[6]), level = g[7], area = g[9], msg = g[10])
		rc.epoch = time.mktime ((rc.timestamp.year, rc.timestamp.month, rc.timestamp.day, rc.timestamp.hour, rc.timestamp.minute, rc.timestamp.second, -1, -1, -1))
		rc.levelName = loglevelName (rc.level)
		return rc

def logTraceback (sig, stack):
	rc = 'Traceback request:\n'
	for line in traceback.format_stack (stack):
		rc += line
	log (LV_REPORT, 'traceback', rc)
signal.signal (signal.SIGUSR2, logTraceback)
#}}}
#
# 2.) Locking
#
#{{{
lockname = None
try:
	lockpath = os.environ['LOCK_HOME']
except KeyError:
	lockpath = mkpath (base, 'var', 'lock')

def _mklockpath (pgmname):
	global	lockpath
	
	return mkpath (lockpath, '%s.lock' % pgmname)

def lock (isFatal = True):
	global	lockname, logname

	if lockname:
		return lockname
	name = _mklockpath (logname)
	s = '%10d\n' % (os.getpid ())
	report = 'Try locking using file "' + name + '"\n'
	n = 0
	while n < 2:
		n += 1
		try:
			if not lockname:
				fd = os.open (name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0444)
				os.write (fd, s)
				os.close (fd)
				lockname = name
				report += 'Lock aquired\n'
		except OSError, e:
			if e.errno == errno.EEXIST:
				report += 'File exists, try to read it\n'
				try:
					fd = os.open (name, os.O_RDONLY)
					inp = os.read (fd, 32)
					os.close (fd)
					idx = inp.find ('\n')
					if idx != -1:
						inp = inp[:idx]
					inp = chop (inp)
					try:
						pid = int (inp)
					except ValueError:
						pid = -1
					if pid > 0:
						report += 'Locked by process %d, look if it is still running\n' % (pid)
						try:

							os.kill (pid, 0)
							report += 'Process is still running\n'
							n += 1
						except OSError, e:
							if e.errno == errno.ESRCH:
								report += 'Remove stale lockfile\n'
								try:
									os.unlink (name)
								except OSError, e:
									report += 'Unable to remove lockfile: ' + e.strerror + '\n'
							elif e.errno == errno.EPERM:
								report += 'Process is running and we cannot access it\n'
							else:
								report += 'Unable to check: ' + e.strerror + '\n'
					else:
						try:
							st = os.stat (name)
							if st.st_size == 0:
								report += 'Empty lock file, assuming due to crash or disk full\n'
								os.unlink (name)
						except OSError, e:
							report += 'Failed to check for or remove empty lock file: %s\n' % e.strerror
				except OSError, e:
					report += 'Unable to read file: ' + e.strerror + '\n'
			else:
				report += 'Unable to create file: ' + e.strerror + '\n'
	if not lockname and isFatal:
		raise error (report)
	return lockname

def unlock ():
	global	lockname

	if lockname:
		try:
			os.unlink (lockname)
			lockname = None
		except OSError, e:
			if e.errno != errno.ENOENT:
				raise error ('Unable to remove lock: ' + e.strerror + '\n')

def signallock (program, signr = signal.SIGTERM):
	rc = False
	report = ''
	fname = _mklockpath (program)
	try:
		fd = open (fname, 'r')
		pline = fd.readline ()
		fd.close ()
		try:
			pid = int (pline.strip ())
			if pid > 0:
				try:
					os.kill (pid, signr)
					rc = True
					report = None
				except OSError, e:
					if e.errno == errno.ESRCH:
						report += 'Process %d does not exist\n' % pid
						try:
							os.unlink (fname)
						except OSError, e:
							report += 'Unable to remove stale lockfile %s %r\n' % (fname, e.args)
					elif e.errno == errno.EPERM:
						report += 'No permission to signal process %d\n' % pid
					else:
						report += 'Failed to signal process %d %r' % (pid, e.args)
			else:
				report += 'PIDFile contains invalid PID: %d\n' % pid
		except ValueError:
			report += 'Content of PIDfile is not valid: "%s"\n' % chop (pline)
	except IOError, e:
		if e.args[0] == errno.ENOENT:
			report += 'Lockfile %s does not exist\n' % fname
		else:
			report += 'Lockfile %s cannot be opened: %r\n' % (fname, e.args)
	return (rc, report)
#}}}
#
# 3.) file I/O
#
#{{{
def createPath (path, mode = 0777):
	if not os.path.isdir (path):
		try:
			os.mkdir (path, mode)
		except OSError, e:
			if e.args[0] != errno.EEXIST:
				if e.args[0] != errno.ENOENT:
					raise error ('Failed to create %s: %s' % (path, e.args[1]))
				elem = path.split (os.path.sep)
				target = ''
				for e in elem:
					target += e
					if target and not os.path.isdir (target):
						try:
							os.mkdir (target, mode)
						except OSError, e:
							raise error ('Failed to create %s at %s: %s' % (path, target, e.args[1]))
					target += os.path.sep

archtab = {}
def mkArchiveDirectory (path, mode = 0777):
	global	archtab

	tt = time.localtime (time.time ())
	ts = '%04d%02d%02d' % (tt[0], tt[1], tt[2])
	arch = mkpath (path, ts)
	if not arch in archtab:
		try:
			st = os.stat (arch)
			if not stat.S_ISDIR (st[stat.ST_MODE]):
				raise error ('%s is not a directory' % arch)
		except OSError, e:
			if e.args[0] != errno.ENOENT:
				raise error ('Unable to stat %s: %s' % (arch, e.args[1]))
			try:
				os.mkdir (arch, mode)
			except OSError, e:
				raise error ('Unable to create %s: %s' % (arch, e.args[1]))
		archtab[arch] = True
	return arch
	
seektab = []
class Filepos:
	def __stat (self, stat_file):
		try:
			if stat_file:
				st = os.stat (self.fname)
			else:
				st = os.fstat (self.fd.fileno ())
			rc = (st[stat.ST_INO], st[stat.ST_CTIME], st[stat.ST_SIZE])
		except (OSError, IOError):
			rc = None
		return rc

	def __open (self):
		global	seektab

		errmsg = None
		if os.access (self.info, os.F_OK):
			try:
				fd = open (self.info, 'r')
				line = fd.readline ()
				fd.close ()
				parts = chop (line).split (':')
				if len (parts) == 3:
					self.inode = int (parts[0])
					self.ctime = int (parts[1])
					self.pos = int (parts[2])
				else:
					errmsg = 'Invalid input for %s: %s' % (self.fname, line)
			except (IOError, ValueError), e:
				errmsg = 'Unable to read info file %s: %r' % (self.info, e.args)
		if not errmsg:
			try:
				self.fd = open (self.fname, 'r')
			except IOError, e:
				errmsg = 'Unable to open %s: %r' % (self.fname, e.args)
			if self.fd:
				st = self.__stat (False)
				if st:
					ninode = st[0]
					nctime = st[1]
					if ninode == self.inode:
						if st[2] >= self.pos:
							self.fd.seek (self.pos)
						else:
							self.pos = 0
					self.inode = ninode
					self.ctime = nctime
				else:
					errmsg = 'Failed to stat %s' % self.fname
				if errmsg:
					self.fd.close ()
					self.fd = None
		if errmsg:
			raise error (errmsg)
		if not self in seektab:
			seektab.append (self)

	def __init__ (self, fname, info, checkpoint = 64):
		self.fname = fname
		self.info = info
		self.checkpoint = checkpoint
		self.fd = None
		self.inode = -1
		self.ctime = 0
		self.pos = 0
		self.count = 0
		self.__open ()
	
	def __save (self):
		fd = open (self.info, 'w')
		fd.write ('%d:%d:%d' % (self.inode, self.ctime, self.fd.tell ()))
		fd.close ()
		self.count = 0
	
	def close (self):
		if self.fd:
			self.__save ()
			self.fd.close ()
			self.fd = None
		if self in seektab:
			seektab.remove (self)

	def __check (self):
		rc = True
		st = self.__stat (True)
		if st:
			if st[0] == self.inode and st[1] == self.ctime and st[2] > self.fd.tell ():
				rc = False
		return rc

	def __readline (self):
		line = self.fd.readline ()
		if line != '':
			self.count += 1
			if self.count >= self.checkpoint:
				self.__save ()
			return chop (line)
		else:
			return None
	
	def readline (self):
		line = self.__readline ()
		if line is None and not self.__check ():
			self.close ()
			self.__open ()
			line = self.__readline ()
		return line
#
def die (lvl = LV_FATAL, ident = None, s = None):
	global	seektab

	if s:
		err (s)
		log (lvl, ident, s)
	for st in seektab[:]:
		st.close ()
	unlock ()
	sys.exit (1)
rip = die
#}}}
#
# 4.) Parallel process wrapper
#
#{{{
try:
	import	multiprocessing
	#
	class Messenger (object):
		class Communicator (object):
			def __init__ (self, msg, myself):
				self.msg = msg
				self.myself = myself
			
			def send (self, receiver, content):
				self.msg.send (self.myself, receiver, content)
			
			def answer (self, m, content):
				self.msg.send (self.myself, m.sender, content)

			def recv (self):
				self.msg.recv (self.myself)
				
		def __init__ (self):
			self.channels = {}
		
		def comm (self, myself):
			return self.Communicator (self, myself)
			
		def create (self, name, exclusive = True):
			if type (name) not in types.StringTypes:
				raise TypeError ('name expected to be a string')
			if name in self.channels:
				if exclusive:
					raise ValueError ('%s: already existing' % name)
			else:
				self.channels[name] = multiprocessing.Queue ()
			return self.comm (name)
		
		def send (self, sender, receiver, content):
			self.channels[receiver].put ((sender, receiver, content))
		
		def answer (self, m, content):
			self.send (self, m[1], m[0], content)
		
		def recv (self, receiver):
			return self.channels[receiver].get ()
	
	_m = Messenger ()
	mcomm = _m.comm
	mcreate = _m.create
	msend = _m.send
	manswer = _m.answer
	mrecv = _m.recv
	del _m
		
	class Parallel (object):
		class Process (multiprocessing.Process):
			def __init__ (self, method, args, name):
				multiprocessing.Process.__init__ (self, name = name)
				self.method = method
				self.args = args
				self.logname = name
				self.resultq = multiprocessing.Queue ()
				self.value = None

			def run (self):
				if self.logname is not None:
					global	logname

					logname = '%s-%s' % (logname, self.logname.lower ().replace ('/', '_'))
					self.logname = None

				if self.args is None:
					rc = self.method ()
				else:
					rc = self.method (*self.args)
				self.resultq.put (rc)
		
			def result (self):
				if self.value is None:
					if not self.resultq.empty ():
						self.value = self.resultq.get ()
				return self.value

		def __init__ (self):
			self.active = set ()
	
		def fork (self, method, args = None, name = None):
			p = self.Process (method, args, name)
			self.active.add (p)
			p.start ()
			return p
		
		def living (self):
			return (len ([_p for _p in self.active if _p.is_alive ()]), len (self.active))
	
		def wait (self, name = None, timeout = None, count = None):
			done = set ()
			rc = {}
			for p in self.active:
				if name is None or p.name == name:
					p.join (timeout)
					if timeout is None or not p.is_alive ():
						rc[p.name] = (p.exitcode, p.result ())
						done.add (p)
						if count is not None:
							count -= 1
							if count == 0:
								break
			self.active = self.active.difference (done)
			return rc
		
		def ready (self):
			for p in self.active:
				if not p.is_alive ():
					rc = self.wait (name = p.name, count = 1)
					if rc:
						return rc.values ()[0]
			return None
	
		def term (self, name = None):
			for p in self.active:
				if name is None or p.name == name:
					p.terminate ()
			return self.wait (name = name)

	_p = Parallel ()
	pfork = _p.fork
	palive = _p.living
	pwait = _p.wait
	pready = _p.ready
	pterm = _p.term
	del _p
except ImportError:
	pass
#}}}
#
# 5.) mailing/httpclient
#
#{{{
def mailsend (relay, sender, receivers, headers, body,
	      myself = 'agnitas.de'):
	codetype = lambda code: code / 100
	rc = False
	if not relay:
		return (rc, 'Missing relay\n')
	if not sender:
		return (rc, 'Missing sender\n')
	if type (receivers) in types.StringTypes:
		receivers = [receivers]
	if len (receivers) == 0:
		return (rc, 'Missing receivers\n')
	if not body:
		return (rc, 'Empty body\n')
	report = ''
	try:
		s = smtplib.SMTP (relay)
		(code, detail) = s.helo (myself)
		if codetype (code) != 2:
			raise smtplib.SMTPResponseException (code, 'HELO ' + myself + ': ' + detail)
		else:
			report += 'HELO %s sent\n%d %s recvd\n' % (myself, code, detail)
		(code, detail) = s.mail (sender)
		if codetype (code) != 2:
			raise smtplib.SMTPResponseException (code, 'MAIL FROM:<' + sender + '>: ' + detail)
		else:
			report += 'MAIL FROM:<%s> sent\n%d %s recvd\n' % (sender, code, detail)
		for r in receivers:
			(code, detail) = s.rcpt (r)
			if codetype (code) != 2:
				raise smtplib.SMTPResponseException (code, 'RCPT TO:<' + r + '>: ' + detail)
			else:
				report += 'RCPT TO:<%s> sent\n%d %s recvd\n' % (r, code, detail)
		mail = ''
		hsend = False
		hrecv = False
		if headers:
			for h in headers:
				if len (h) > 0 and h[-1] != '\n':
					h += '\n'
				if not hsend and len (h) > 5 and h[:5].lower () == 'from:':
					hsend = True
				elif not hrecv and len (h) > 3 and h[:3].lower () == 'to:':
					hrecv = True
				mail = mail + h
		if not hsend:
			mail += 'From: ' + sender + '\n'
		if not hrecv:
			recvs = ''
			for r in receivers:
				if recvs:
					recvs += ', '
				recvs += r
			mail += 'To: ' + recvs + '\n'
		mail += '\n' + body
		(code, detail) = s.data (mail)
		if codetype (code) != 2:
			raise smtplib.SMTPResponseException (code, 'DATA: ' + detail)
		else:
			report += 'DATA sent\n%d %s recvd\n' % (code, detail)
		s.quit ()
		report += 'QUIT sent\n'
		rc = True
	except smtplib.SMTPConnectError, e:
		report += 'Unable to connect to %s, got %d %s response\n' % (relay, e.smtp_code, e.smtp_error)
	except smtplib.SMTPServerDisconnected:
		report += 'Server connection lost\n'
	except smtplib.SMTPResponseException, e:
		report += 'Invalid response: %d %s\n' % (e.smtp_code, e.smtp_error)
	except socket.error, e:
		report += 'General socket error: %r\n' % (e.args, )
	except Exception, e:
		report += 'General problems during mail sending: %r, %r\n' % (type (e), e.args)
	return (rc, report)


def httpget (hostname, port, query):
	success = False
	data = ''
	try:
		conn = httplib.HTTPConnection (hostname, port, True)
		conn.connect ()
		conn.request ('GET', query)
		answ = conn.getresponse ()
		data = answ.read ()
		n = data.find (':')
		if n != -1:
			st = data[:n]
			if st[0] == '+':
				success = True
			data = data[n + 1:].strip ()
		else:
			data = 'Invalid response: ' + data.strip ()
	except Exception, e:
		data = 'Caught exception %r: %r' % (type (e), e.args)
	return (success, data)

def startOnDemandMailing (mailingID, merger = None, port = 8080, timeout = 30):
	def alrmHandler (sig, stack):
		pass
	ohandler = signal.signal (signal.SIGALRM, alrmHandler)
	rc = False
	data = None
	try:
		try:
			if merger is None:
				merger = '10.1.1.32'
			signal.alarm (timeout)
			(rc, data) = httpget (merger, port, '/demand?%d' % mailingID)
			if not rc and not data:
				data = 'timeout'
		except Exception, e:
			data = 'Caught exception %r: %r' % (type (e), e.args)
	finally:
		signal.alarm (0)
	signal.signal (signal.SIGALRM, ohandler)
	return (rc, data)

def URLEncode (raw):
	rc = ''
	for ch in raw:
		n = ord (ch)
		if ch in ('+', '%', '=', '&', '?') or n < 20 or n > 126:
			rc += '%%%02X' % n
		elif ch == ' ':
			rc += '+'
		else:
			rc += ch
	return rc

def URLEncodeHash (parm):
	rc = ''
	sep = ''
	for key in parm.keys ():
		rc += '%s%s=%s' % (sep, key, URLEncode (parm[key]))
		sep = '&'
	return rc
#}}}
#
# 6.) system interaction
#
#{{{

def call (*args, **kwargs):
	rc = None
	pp = subprocess.Popen (*args, **kwargs)
	while rc is None:
		try:
			rc = pp.wait ()
		except OSError, e:
			if e.args[0] == errno.ECHILD:
				rc = -1
	return rc

def fileAccess (path):
	if system != 'linux':
		raise error ('lsof only supported on linux')
	try:
		st = os.stat (path)
	except OSError, e:
		raise error ('failed to stat "%s": %r' % (path, e.args))
	device = st[stat.ST_DEV]
	inode = st[stat.ST_INO]
	rc = []
	fail = []
	seen = {}
	isnum = re.compile ('^[0-9]+$')
	for pid in [_p for _p in os.listdir ('/proc') if not isnum.match (_p) is None]:
		bpath = '/proc/%s' % pid
		checks = ['%s/%s' % (bpath, _c) for _c in ('cwd', 'exe', 'root')]
		try:
			fdp = '%s/fd' % bpath
			for fds in os.listdir (fdp):
				checks.append ('%s/%s' % (fdp, fds))
		except OSError, e:
			fail.append ([e.args[0], '%s/fd: %s' % (bpath, e.args[1])])
		try:
			fd = open ('%s/maps' % bpath, 'r')
			for line in fd:
				parts = line.split ()
				if len (parts) == 6 and parts[5].startswith ('/'):
					checks.append (parts[5].strip ())
			fd.close ()
		except IOError, e:
			fail.append ([e.args[0], '%s/maps: %s' % (bpath, e.args[1])])
		for check in checks:
			try:
				if seen[check]:
					rc.append (pid)
			except KeyError:
				seen[check] = False
				cpath = check
				try:
					fpath = None
					count = 0
					while fpath is None and count < 128 and cpath.startswith ('/'):
						count += 1
						st = os.lstat (cpath)
						if stat.S_ISLNK (st[stat.ST_MODE]):
							cpath = os.readlink (cpath)
						else:
							fpath = cpath
					if not fpath is None and st[stat.ST_DEV] == device and st[stat.ST_INO] == inode:
						rc.append (pid)
						seen[check] = True
				except OSError, e:
					fail.append ([e.args[0], '%s: %s' % (cpath, e.args[1])])
	return (rc, fail)

def fileExistance (fname, content, mode, report = None):
	rc = False
	try:
		fd = open (fname, 'rb')
		ocontent = fd.read ()
		fd.close ()
	except IOError, e:
		ocontent = None
		if report is not None:
			report.append ('Failed to read original file: %r' % (e.args, ))
	try:
		if ocontent is None or ocontent != content:
			fd = open (fname, 'wb')
			fd.write (content)
			fd.close ()
		st = os.stat (fname)
		if stat.S_IMODE (st.st_mode) != mode:
			os.chmod (fname, mode)
		rc = True
	except (IOError, OSError), e:
		report.append ('Failed to update file: %r' % (e.args, ))
	return rc
#}}}
#
# 7.) Validate UIDs
#
#{{{
class UID:
	def __init__ (self):
		self.companyID = 0
		self.mailingID = 0
		self.customerID = 0
		self.URLID = 0
		self.signature = None
		self.prefix = None
		self.password = None
	
	def __decodeBase36 (self, s):
		return int (s, 36)
	
	def __codeBase36 (self, i):
		if i == 0:
			return '0'
		elif i < 0:
			i = -i
			sign = '-'
		else:
			sign = ''
		s = ''
		while i > 0:
			s = '0123456789abcdefghijklmnopqrstuvwxyz'[i % 36] + s
			i /= 36
		return sign + s
	
	def __makeSignature (self, s):
		hashval = hash_sha1 (s).digest ()
		sig = ''
		for ch in hashval[::2]:
			sig += self.__codeBase36 ((ord (ch) >> 2) % 36)
		return sig
	
	def __makeBaseUID (self):
		if self.prefix:
			s = self.prefix + '.'
		else:
			s = ''
		s += self.__codeBase36 (self.companyID) + '.' + \
		     self.__codeBase36 (self.mailingID) + '.' + \
		     self.__codeBase36 (self.customerID) + '.' + \
		     self.__codeBase36 (self.URLID)
		return s
	
	def createSignature (self):
		return self.__makeSignature (self.__makeBaseUID () + '.' + self.password)
	
	def createUID (self):
		baseUID = self.__makeBaseUID ()
		return baseUID + '.' + self.__makeSignature (baseUID + '.' + self.password)
	
	def parseUID (self, uid):
		parts = uid.split ('.')
		plen = len (parts)
		if not plen in (5, 6):
			raise error ('Invalid input format')
		start = plen - 5
		if start == 1:
			self.prefix = parts[0]
		else:
			self.prefix = None
		try:
			self.companyID = self.__decodeBase36 (parts[start])
			self.mailingID = self.__decodeBase36 (parts[start + 1])
			self.customerID = self.__decodeBase36 (parts[start + 2])
			self.URLID = self.__decodeBase36 (parts[start + 3])
			self.signature = parts[start + 4]
		except ValueError:
			raise error ('Invalid input in data')
	
	def validateUID (self):
		lsig = self.createSignature ()
		return lsig == self.signature


class XUID (object):
	symbols = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'
	def __init__ (self):
		self.timestamp = 0
		self.timestamps = [0, 0]
		self.secret = None
		self.extensionID = 0
		self.licenceID = licence
		self.companyID = 0
		self.mailingID = 0
		self.customerID = 0
		self.URLID = 0
		self.prefix = None
	
	def __iencode (self, n):
		if n <= 0:
			rc = self.symbols[0]
		else:
			rc = ''
			while n > 0:
				rc = self.symbols[n & 0x3f] + rc
				n >>= 6
		return rc
	
	def __idecode (self, s):
		n = 0
		for ch in s:
			n <<= 6
			v = self.symbols.find (ch)
			if v == -1:
				raise error ('Illegal charater in numeric value')
			n += v
		return n
	
	invalidReplacement = toutf8 (u'\ufffd')
	def __makeSignature (self):
		sig = []
		if self.prefix:
			sig.append (self.prefix)
		sig.append (str (self.extensionID))
		sig.append (str (self.licenceID))
		sig.append (str (self.mailingID))
		sig.append (str (self.customerID))
		sig.append (str (self.URLID))
		#
		# Wild hack to interpret invalid character in secret key
		# as handled in oracle/jdbc/java
		secret = self.secret
		if secret:
			try:
				fromutf8 (secret)
			except UnicodeDecodeError:
				secret = ''
				skip = 0
				for c in self.secret:
					if skip > 0:
						skip -= 1
					else:
						ch = ord (c)
						if ch < 128:
							secret += c
						else:
							if ch < 240:
								if ch >= 224:
									skip = 2
								elif ch >= 192:
									skip = 1
							secret += self.invalidReplacement
		else:
			secret = ''
		sig.append (secret)
		dig = hash_md5 ()
		dig.update ('.'.join (sig))
		return base64.urlsafe_b64encode (dig.digest ()).replace ('=', '')
	
	def __date2timestamp (self, year, month, day, hour, minute, second, dst = -1):
		return int (time.mktime ((year, month, day, hour, minute, second, -1, -1, dst))) * 1000
	
	def setTimestamp (self, timestamp):
		if type (timestamp) == datetime.datetime:
			timestamp = self.__date2timestamp (timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, timestamp.second)
		elif type (timestamp) == time.struct_time:
			timestamp = self.__date2timestamp (timestamp.tm_year, timestamp.tm_mon, timestamp.tm_mday, timestamp.tm_hour, timestamp.tm_min, timestamp.tm_sec, timestamp.tm_isdst)
		self.timestamp = timestamp
		self.timestamps = [self.timestamp & 0xffff, (self.timestamp * 37) & 0xffff]
	
	def createUID (self):
		uid = []
		if self.prefix:
			uid.append (self.prefix)
		uid.append (self.__iencode (self.extensionID))
		uid.append (self.__iencode (self.licenceID))
		uid.append (self.__iencode (self.mailingID))
		uid.append (self.__iencode (self.customerID ^ self.timestamps[0]))
		uid.append (self.__iencode (self.URLID ^ self.timestamps[1] ^ self.companyID))
		uid.append (self.__makeSignature ())
		return '.'.join (uid)
	
	def parseUID (self, uid, callback, doValidate = True):
		elem = uid.split ('.')
		if len (elem) == 7:
			self.prefix = elem[0]
			elem = elem[1:]
		elif len (elem) == 6:
			self.prefix = None
		else:
			raise error ('Invalid formated UID')
		self.extensionID = self.__idecode (elem[0])
		self.licenceID = self.__idecode (elem[1])
		self.mailingID = self.__idecode (elem[2])
		if callback is not None and not callback (self):
			raise error ('Failed to find secrets for mailingID %d' % self.mailingID)
		self.customerID = self.__idecode (elem[3]) ^ self.timestamps[0]
		self.URLID = self.__idecode (elem[4]) ^ self.timestamps[1] ^ self.companyID
		if doValidate and elem[5] != self.__makeSignature ():
			raise error ('Signature mismatch')

class PubID (object):
	scramble = 'w5KMCHOXE_PTuLcfF6D1ZI3BydeplQaztVAnUj0bqos7k49YgWhxiS-RrGJm8N2v'
	sourceInvalid = re.compile ('[^0-9a-zA-Z_-]')

	def __init__ (self):
		self.mailingID = 0
		self.customerID = 0
		self.source = None
		self.selector = None
	
	def __source (self, source):
		if not source is None:
			if len (source) > 20:
				source = source[:20]
			source = self.sourceInvalid.subn ('_', source)[0]
		return source
	
	def __checksum (self, s):
		cs = 12
		for ch in s:
			cs += ord (ch)
		return self.scramble[cs & 0x3f]
	
	def __encode (self, s):
		if type (s) == types.UnicodeType:
			s = toutf8 (s)
		slen = len (s)
		temp = []
		n = 0
		while n < slen:
			chk = s[n:n + 3]
			d = ord (chk[0]) << 16
			if len (chk) > 1:
				d |= ord (chk[1]) << 8
				if len (chk) > 2:
					d |= ord (chk[2])
			n += 3
			temp.append (self.scramble[(d >> 18) & 0x3f])
			temp.append (self.scramble[(d >> 12) & 0x3f])
			temp.append (self.scramble[(d >> 6) & 0x3f])
			temp.append (self.scramble[d & 0x3f])
		temp.insert (5, self.__checksum (temp))
		return ''.join (temp)
	
	def __decode (self, s):
		rc = None
		slen = len (s)
		if slen > 5 and (slen - 1) & 3 == 0:
			check = s[5]
			s = s[:5] + s[6:]
			if check == self.__checksum (s):
				slen -= 1
				collect = []
				ok = True
				n = 0
				while n < slen and ok:
					v = [self.scramble.find (s[_c]) for _c in range (n, n + 4)]
					n += 4
					if -1 in v:
						ok = False
					else:
						d = (v[0] << 18) | (v[1] << 12) | (v[2] << 6) | v[3]
						collect.append (chr ((d >> 16) & 0xff))
						collect.append (chr ((d >> 8) & 0xff))
						collect.append (chr (d & 0xff))
				if ok:
					while collect and collect[-1] == '\0':
						collect = collect[:-1]
					rc = ''.join (collect)
		return rc
	
	def createID (self, mailingID = None, customerID = None, source = None, selector = None):
		if mailingID is None:
			mailingID = self.mailingID
		if customerID is None:
			customerID = self.customerID
		if source is None:
			source = self.source
			if source is None:
				source = ''
		if selector is None:
			selector = self.selector
			if not selector:
				selector = None
		src = '%d;%d;%s' % (mailingID, customerID, self.__source (source))
		if selector is not None:
			src += ';%s' % selector
		return self.__encode (src)
	
	def parseID (self, pid):
		rc = False
		dst = self.__decode (pid)
		if dst is not None:
			parts = dst.split (';', 3)
			
			if len (parts) in (3, 4):
				try:
					mailingID = int (parts[0])
					customerID = int (parts[1])
					source = parts[2]
					if len (parts) > 3:
						selector = parts[3]
					else:
						selector = None
					if mailingID > 0 and customerID > 0:
						self.mailingID = mailingID
						self.customerID = customerID
						if source:
							self.source = source
						else:
							self.source = None
						self.selector = selector
						rc = True
				except ValueError:
					pass
		return rc

def parseAncientUID (uid):
	rc = None
	fix_xor = 0x5CF16053
	if len (uid) in (33, 41):
		ltype = uid[-1:]
		uid = uid[:-1]
	elif len (uid) in (34, 42):
		ltype = uid[-2:]
		uid = uid[:-2]
	elif len (uid) in (32, 40):
		ltype = ''
	else:
		ltype = None
	if ltype is not None:
		try:
			data = uid[:24]
			hashvalue = uid[-8:]
			if len (uid) == 40:
				parameter = int (uid[24:32], 16) ^ fix_xor
			else:
				parameter = 0
			mailing_id = None
			mailinglist_id = None
			hashmode = 0
			customer_id = int (data[16:24], 16) ^ fix_xor
			if ltype == 'gc':
				company_id = int (data[:8], 16)
				mailing_id = int (data[8:16], 16) ^ fix_xor
				hashmode = 1
			elif ltype == 'gS':
				mailinglist_id = int (data[:8], 16)
				company_id = int (data[8:16], 16) ^ fix_xor
			else:
				mailing_id = int (data[:8], 16)
				company_id = int (data[8:16], 16) ^ fix_xor
			rc = struct (companyID = company_id, mailingID = mailing_id, customerID = customer_id, URLID = parameter)
		except ValueError:
			pass
	return rc

class METAFile (object):
	splitter = re.compile ('[^0-9]+')
	def __init__ (self, path):
		self.setPath (path)
	
	def __makeTimestamp (self, epoch):
		tt = time.localtime (epoch)
		return '%04d%02d%02d%02d%02d%02d' % (tt[0], tt[1], tt[2], tt[3], tt[4], tt[5])

	def __parseTimestamp (self, ts):
		if ts[0] == 'D' and len (ts) == 15:
			rc = ts[1:]
		else:
			try:
				rc = self.__makeTimestamp (int (ts))
			except ValueError:
				rc = None
		return rc
	
	def __error (self, s):
		if self.error is None:
			self.error = [s]
		else:
			self.error.append (s)
		self.valid = False
	
	def isReady (self, epoch = None):
		if epoch is None:
			ts = self.__makeTimestamp (time.time ())
		elif type (epoch) in types.StringTypes:
			ts = epoch
		elif type (epoch) in (types.IntType, types.LongType):
			ts = self.__makeTimestamp (epoch)
		else:
			raise TypeError ('Expecting either None, string or numeric, got %r' % type (epoch))
		return self.valid and cmp (self.timestamp, ts) <= 0

	def getError (self):
		if self.error is None:
			return 'no error'
		return ', '.join (self.error)

	def setPath (self, path):
		self.valid = False
		self.error = None
		self.path = path
		self.directory = None
		self.filename = None
		self.extension = None
		self.basename = None
		self.licence = None
		self.company = None
		self.timestamp = None
		self.mailid = None
		self.mailing = None
		self.blocknr = None
		self.blockid = None
		self.single = None
		if path is not None:
			self.directory = os.path.dirname (self.path)
			self.filename = os.path.basename (self.path)
			n = self.filename.find ('.')
			if n != -1:
				self.extension = self.filename[n + 1:]
				self.basename = self.filename[:n]
			else:
				self.basename = self.filename
			parts = self.basename.split ('=')
			if len (parts) != 6:
				self.__error ('Invalid format of input file')
			else:
				self.valid = True
				n = parts[0].find ('-')
				if n != -1:
					try:
						self.licence = int (parts[0][n + 1:])
					except ValueError:
						self.licence = None
						self.__error ('Unparsable licence ID in "%s" found' % parts[0])
				else:
					self.licence = licence
				cinfo = parts[2].split ('-')
				try:
					self.company = int (cinfo[0])
				except ValueError:
					self.company = None
					self.__error ('Unparseable company ID in "%s" found' % parts[2])
				self.timestamp = self.__parseTimestamp (parts[1])
				if self.timestamp is None:
					self.__error ('Unparseable timestamp in "%s" found' % parts[1])
				self.mailid = parts[3]
				mparts = [_m for _m in self.splitter.split (self.mailid) if _m]
				if len (mparts) == 0:
					self.__error ('Unparseable mailing ID in "%s" found' % parts[3])
				else:
					try:
						self.mailing = int (mparts[-1])
					except ValueError:
						self.__error ('Unparseable mailing ID in mailid "%s" found' % self.mailid)
				try:
					self.blocknr = int (parts[4])
					self.blockid = '%d' % self.blocknr
					self.single = False
				except ValueError:
					self.blocknr = 0
					self.blockid = parts[4]
					self.single = True
#}}}
#
# 8.) General database interface
#
#{{{
class DBResultType:
	Array = 0
	List = 1
	Struct = 2
	Hash = 3
	Result = 4

class DBResult (object):
	def __init__ (self, col, row):
		self._Col = col
		self._Row = row

	def __len__ (self):
		return len (self._Row)

	def __getitem__ (self, what):
		if type (what) in (int, long):
			return self._Row[what]
		return self.__dict__[what]

	def __contains__ (self, what):
		return what in self._Col

	def __call__ (self, what, dflt = None):
		try:
			return self[what]
		except (KeyError, IndexError):
			return dflt
	
	def __str__ (self):
		return '[%s]' % ', '.join (['%s=%r' % (_c, _r) for (_c, _r) in zip (self._Col, self._Row)])
	
	def getColumns (self):
		return self._Col
	
	def getValues (self):
		return self._Row
	
	def getItems (self):
		return zip (self._Col, self._Row)

	def NVL (self, what, onNull):
		rc = self[what]
		if rc is None:
			rc = onNull
		return rc

class DBCore (object):
	def __init__ (self, dbms, driver, cursorClass):
		self.dbms = dbms
		self.driver = driver
		self.cursorClass = cursorClass
		self.db = None
		self.lasterr = None
		self.log = None
		self.cursors = []
	
	def __enter__ (self):
		return self
	
	def __exit__ (self, exc_type, exc_value, traceback):
		self.close ()
	
	def error (self, errmsg):
		self.lasterr = errmsg
		self.close ()
	
	def reprLastError (self):
		return str (self.lasterr)
	
	def lastError (self):
		if self.lasterr is not None:
			return self.reprLastError ()
	
	def sync (self, commit = True):
		if self.db is not None:
			if commit:
				self.db.commit ()
			else:
				self.db.rollback ()
				
	def commit (self):
		self.sync (True)
	
	def rollback (self):
		self.sync (False)

	def close (self):
		if self.db is not None:
			for c in self.cursors:
				try:
					c.close ()
				except self.driver.Error:
					pass
			try:
				self.db.close ()
			except self.driver.Error, e:
				self.lasterr = e
			self.db = None
		self.cursors = []
	
	def open (self):
		self.close ()
		try:
			self.connect ()
		except self.driver.Error, e:
			self.error (e)
		return self.isOpen ()
	
	def isOpen (self):
		return self.db is not None
	
	def getCursor (self):
		if self.isOpen () or self.open ():
			try:
				curs = self.db.cursor ()
				if curs is not None:
					try:
						if curs.arraysize < 100:
							curs.arraysize = 100
					except AttributeError:
						pass
			except self.driver.Error, err:
				curs = None
				self.error (err)
		else:
			curs = None
		return curs
	
	def cursor (self, autocommit = False):
		if self.isOpen () or self.open ():
			c = self.cursorClass (self, autocommit)
			if c is not None:
				self.cursors.append (c)
		else:
			c = None
		return c
	
	def release (self, cursor):
		if cursor in self.cursors:
			self.cursors.remove (cursor)
			cursor.close ()
		
	def query (self, req):
		c = self.cursor ()
		if c is None:
			raise error ('Unable to get database cursor: ' + self.lastError ())
		rc = None
		try:
			rc = [r for r in c.query (req)]
		finally:
			c.close ()
		return rc
		
	def update (self, req):
		c = self.cursor ()
		if c is None:
			raise error ('Unable to get database cursor: ' + self.lastError ())
		rc = None
		try:
			rc = c.update (req)
		finally:
			c.close ()
		return rc
	execute = update

class DBCache (object):
	def __init__ (self, data):
		self.data = data
		self.count = len (data)
		self.pos = 0

	def __iter__ (self):
		return self

	def __next__ (self):
		if self.pos >= self.count:
			raise StopIteration ()
		record = self.data[self.pos]
		self.pos += 1
		return record
	next = __next__

class DBCursor (object):
	def __init__ (self, db, autocommit, needReformat):
		self.db = db
		self.autocommit = autocommit
		self.needReformat = needReformat
		self.curs = None
		self.desc = False
		self.defaultRType = None
		self.querypost = None
		self.rowspec = None
		self.cacheReformat = {}
		self.cacheCleanup = {}
		self.log = db.log
		self.qreplace = {}
	
	def __enter__ (self):
		return self
	
	def __exit__ (self, exc_type, exc_value, traceback):
		self.close ()
	
	def rselect (self, s, **kws):
		if kws:
			rplc = self.qreplace.copy ()
			for (var, val) in kws.items ():
				rplc[var] = val
			return s % rplc
		return s % self.qreplace

	qmapper = {
		'MySQLdb':	'mysql',

		'cx_Oracle':	'oracle',
		'sqlite3':	'sqlite',
	}
	def qselect (self, **args):
		try:
			return args[self.db.driver.__name__]
		except KeyError:
			return args[self.qmapper[self.db.driver.__name__]]
		
	def lastError (self):
		if self.db is not None:
			return self.db.lastError ()
		return 'no database interface active'
		
	def error (self, errmsg):
		if self.db is not None:
			self.db.lasterr = errmsg
		self.close ()

	def close (self):
		if self.curs is not None:
			try:
				self.db.release (self.curs)
				if self.log: self.log ('Cursor closed')
			except self.db.driver.Error, e:
				self.db.lasterr = e
				if self.log: self.log ('Cursor closing failed: %s' % self.lastError ())
			self.curs = None
			self.desc = False
	
	def open (self):
		self.close ()
		if self.db is not None:
			try:
				self.curs = self.db.getCursor ()
				if self.curs is not None:
					if self.log: self.log ('Cursor opened')
				else:
					if self.log: self.log ('Cursor open failed')
			except self.db.driver.Error, e:
				self.error (e)
				if self.log: self.log ('Cursor opening failed: %s' % self.lastError ())
		else:
			if self.log: self.log ('Cursor opeing failed: no database available')
		return self.curs is not None
		
	def description (self):
		if self.desc:
			return self.curs.description
		return None

	rfparse = re.compile ('\'[^\']*\'|:[A-Za-z0-9_]+|%')
	def reformat (self, req, parm):
		try:
			(nreq, varlist) = self.cacheReformat[req]
		except KeyError:
			nreq = ''
			varlist = []
			while 1:
				mtch = self.rfparse.search (req)
				if mtch is None:
					nreq += req
					break
				else:
					span = mtch.span ()
					nreq += req[:span[0]]
					chunk = req[span[0]:span[1]]
					if chunk == '%':
						nreq += '%%'
					elif chunk.startswith ('\'') and chunk.endswith ('\''):
						nreq += chunk.replace ('%', '%%')
					else:
						varlist.append (chunk[1:])
						nreq += '%s'
					req = req[span[1]:]
			self.cacheReformat[req] = (nreq, varlist)
		nparm = []
		for key in varlist:
			nparm.append (parm[key])
		return (nreq, nparm)

	def cleanup (self, req, parm):
		try:
			varlist = self.cacheCleanup[req]
		except KeyError:
			varlist = []
			while 1:
				mtch = self.rfparse.search (req)
				if mtch is None:
					break
				span = mtch.span ()
				chunk = req[span[0]:span[1]]
				if chunk.startswith (':'):
					varlist.append (chunk[1:])
				req = req[span[1]:]
			self.cacheCleanup[req] = varlist
		nparm = {}
		for key in varlist:
			nparm[key] = parm[key]
		return nparm

	def __valid (self):
		if self.curs is None:
			if not self.open ():
				raise error ('Unable to setup cursor: ' + self.lastError ())

	def __rowsetup (self, data):
		if self.rowspec is None:
			d = self.description ()
			if d is None:
				self.rowspec = []
				nr = 1
				while nr <= len (data):
					self.rowspec.append ('_%d' % nr)
					nr += 1
			else:
				self.rowspec = [_d[0].lower () for _d in d]
	
	def __rowlist (self, data):
		self.__rowsetup (data)
		return zip (self.rowspec, data)

	def __rowfill (self, hash, data):
		for (var, val) in self.__rowlist (data):
			hash[var] = val
	
	def __rowstruct (self, data):
		rc = struct ()
		self.__rowfill (rc.__dict__, data)
		return rc
		
	def __rowhash (self, data):
		rc = {}
		self.__rowfill (rc, data)
		return rc
		
	def __rowresult (self, data):
		self.__rowsetup (data)
		rc = DBResult (self.rowspec, data)
		self.__rowfill (rc.__dict__, data)
		return rc
	
	def __iter__ (self):
		return self
		
	def __next__ (self):
		try:
			data = self.curs.fetchone ()
		except self.db.driver.Error, e:
			self.error (e)
			raise error ('query next failed: ' + self.lastError ())
		if data is None:
			raise StopIteration ()
		if self.querypost:
			return self.querypost (data)
		return data
	next = __next__

	def setOutputSize (self, *args):
		if self.db.dbms == 'oracle':
			self.__valid ()
			self.curs.setoutputsize (*args)

	def setInputSizes (self, **args):
		if self.db.dbms == 'oracle':
			self.__valid ()
			self.curs.setinputsizes (**args)

	def defaultResultType (self, rtype):
		old = self.defaultRType
		self.defaultRType = rtype
		return old

	def query (self, req, parm = None, cleanup = False, rtype = None):
		self.__valid ()
		if rtype is None:
			rtype = self.defaultRType
		self.rowspec = None
		if rtype == DBResultType.List:
			self.querypost =  self.__rowlist
		elif rtype == DBResultType.Struct:
			self.querypost = self.__rowstruct
		elif rtype == DBResultType.Hash:
			self.querypost = self.__rowhash
		elif rtype == DBResultType.Result:
			self.querypost = self.__rowresult
		else:
			self.querypost = None
		try:
			if parm is None:
				if self.log: self.log ('Query: %s' % req)
				self.curs.execute (req)
			else:
				if self.needReformat:
					(req, parm) = self.reformat (req, parm)
				if cleanup:
					parm = self.cleanup (req, parm)
				if self.log: self.log ('Query: %s using %s' % (req, parm))
				self.curs.execute (req, parm)
			if self.log: self.log ('Query started')
		except self.db.driver.Error, e:
			self.error (e)
			if self.log:
				if parm is None:
					self.log ('Query %s failed: %s' % (req, self.lastError ()))
				else:
					self.log ('Query %s using %r failed: %s' % (req, parm, self.lastError ()))
			raise error ('query start failed: ' + self.lastError ())
		self.desc = True
		return self
		
	def queryc (self, req, parm = None, cleanup = False, rtype = None):
		if self.query (req, parm, cleanup, rtype) == self:
			try:
				data = self.curs.fetchall ()
				if self.querypost:
					data = [self.querypost (_d) for _d in data]
				return DBCache (data)
			except self.db.driver.Error, e:
				self.error (e)
				if self.log:
					if parm is None:
						self.log ('Queryc %s fetch failed: %s' % (req, self.lastError ()))
					else:
						self.log ('Queryc %s using %r fetch failed: %s' % (req, parm, self.lastError ()))
				raise error ('query all failed: ' + self.lastError ())
		if self.log:
			if parm is None:
				self.log ('Queryc %s failed: %s' % (req, self.lastError ()))
			else:
				self.log ('Queryc %s using %r failed: %s' % (req, parm, self.lastError ()))
		raise error ('unable to setup query: ' + self.lastError ())
		
	def querys (self, req, parm = None, cleanup = False, rtype = None):
		rc = None
		for rec in self.query (req, parm, cleanup, rtype):
			rc = rec
			break
		return rc
	
	def queryp (self, req, **kws):
		return self.query (req, kws)
	def querypc (self, req, **kws):
		return self.queryc (req, kws)
	def queryps (self, req, **kws):
		return self.querys (req, kws)
		
	def sync (self, commit = True):
		rc = False
		if self.db is not None:
			if self.db.db is not None:
				try:
					self.db.sync (commit)
					if self.log:
						if commit:
							self.log ('Sync done commiting')
						else:
							self.log ('Sync done rollbacking')
					rc = True
				except self.db.driver.Error, e:
					self.error (e)
					if self.log:
						if commit:
							self.log ('Sync failed commiting')
						else:
							self.log ('Sync failed rollbacking')
			else:
				if self.log: self.log ('Sync failed: database not open')
		else:
			if self.log: self.log ('Sync failed: database not available')
		return rc

	def update (self, req, parm = None, commit = False, cleanup = False):
		self.__valid ()
		try:
			if parm is None:
				if self.log: self.log ('Update: %s' % req)
				self.curs.execute (req)
			else:
				if self.needReformat:
					(req, parm) = self.reformat (req, parm)
				if cleanup:
					parm = self.cleanup (req, parm)
				if self.log: self.log ('Update: %s using %r' % (req, parm))
				self.curs.execute (req, parm)
			if self.log: self.log ('Update affected %d rows' % self.curs.rowcount)
		except self.db.driver.Error, e:
			self.error (e)
			if self.log:
				if parm is None:
					self.log ('Update %s failed: %s' % (req, self.lastError ()))
				else:
					self.log ('Update %s using %r failed: %s' % (req, parm, self.lastError ()))
			raise error ('update failed: ' + self.lastError ())
		rows = self.curs.rowcount
		if rows > 0 and (commit or self.autocommit):
			if not self.sync ():
				if self.log:
					if parm is None:
						self.log ('Commit after update failed for %s: %s' % (req, self.lastError ()))
					else:
						self.log ('Commit after update failed for %s using %r: %s' % (req, parm, self.lastError ()))
				raise error ('commit failed: ' + self.lastError ())
		self.desc = False
		return rows
	execute = update

	def updatep (self, req, **kws):
		return self.update (req, kws)
	executep = updatep

try:
	import	MySQLdb

	class DBCursorMySQL (DBCursor):
		def __init__ (self, db, autocommit):
			DBCursor.__init__ (self, db, autocommit, True)
			self.qreplace['sysdate'] = 'current_timestamp'

	class DBMySQL (DBCore):

		dbhost = 'mysqldb'
		dbuser = 'agnitas'
		dbpass = 'secret'
		dbname = 'emm'
		def __init__ (self, host = dbhost, user = dbuser, passwd = dbpass, name = dbname):
			DBCore.__init__ (self, 'mysql', MySQLdb, DBCursorMySQL)
			self.host = host
			self.user = user
			self.passwd = passwd
			self.name = name
		
		def reprLastError (self):
			return 'MySQL-%d: %s' % (self.lasterr.args[0], self.lasterr.args[1].strip ())

		def connect (self):
			try:
				(host, port) = self.host.split (':', 1)
				self.db = self.driver.connect (host, self.user, self.passwd, self.name, int (port))
			except ValueError:
				self.db = self.driver.connect (self.host, self.user, self.passwd, self.name)

	class CMSDBMySQL (DBMySQL):

		dbhost = 'localhost'
		dbuser = 'cms_agnitas'
		dbpass = 'cms_agnitas'
		dbname = 'cms_emm'
		def __init__ (self, host = dbhost, user = dbuser, passwd = dbpass, name = dbname):
			DBMySQL.__init__ (self, host, user, passwd, name)
except ImportError:
	DBMySQL = None



try:
	os.environ['NLS_LANG'] = 'american_america.UTF8'
	import	cx_Oracle

	class DBCursorOracle (DBCursor):
		def __init__ (self, db, autocommit):
			DBCursor.__init__ (self, db, autocommit, False)
			self.qreplace['sysdate'] = 'sysdate'

		def update (self, req, parm = None, commit = False, cleanup = False, adapt = False):
			if parm is not None and adapt:
				amap = {}
				if cleanup:
					parm = self.cleanup (req, parm)
					cleanup = False
				for (var, val) in parm.items ():
					if type (val) in types.StringTypes and len (val) >= 4000:
						amap[var] = cx_Oracle.CLOB
				if amap:
					self.curs.setinputsizes (**amap)
			return DBCursor.update (self, req, parm, commit, cleanup)
		execute = update

	class DBOracle (DBCore):
		dbuser = 'agnitas'
		dbpass = 'delikat'
		dbsid = 'EMM'
		def __init__ (self, user = dbuser, passwd = dbpass, sid = dbsid):
			DBCore.__init__ (self, 'oracle', cx_Oracle, DBCursorOracle)
			self.user = user
			self.passwd = passwd
			self.sid = sid
		
		def reprLastError (self):
			message = self.lasterr.args[0]
			if type (message) in types.StringTypes:
				return message.strip ()
			return message.message.strip ()
		
		def connect (self):
			self.db = self.driver.connect (self.user, self.passwd, self.sid)
			try:
				if self.db.stmtcachesize < 20:
					self.db.stmtcachesize = 20
			except AttributeError:
				pass
			try:
				if self.db.autocommit:
					self.db.autocommit = 0
			except AttributeError:
				pass
except ImportError:
	DBOracle = None

def DBase (*args, **kws):
	if not DBase.warn:
		log (LV_WARNING, 'dbase', 'Using deprecated DBase()')
		DBase.warn = True
	return DBOracle (*args, **kws)
DBase.warn = False

try:
	import	sqlite3
	
	class DBCursorSQLite3 (DBCursor):
		def __init__ (self, db, autocommit):
			DBCursor.__init__ (self, db, autocommit, False)
		
		modes = {
			'fast':	[
				'PRAGMA cache_size = 65536',
				'PRAGMA automatic_index = ON',
				'PRAGMA journal_mode = OFF',
				'PRAGMA synchronous = OFF',
				'PRAGMA secure_delete = OFF',
			],
			'exclusive': [
				'PRAGMA locking_mode = EXCLUSIVE'
			]
		}
		def mode (self, m):
			try:
				for op in self.modes[m]:
					self.execute (op)
			except KeyError:
				raise error ('invalid mode: %s (expecting one of %s)' % (m, ', '.join (sorted (self.modes.keys ()))))

		def validate (self, collect = None, quick = False, amount = None):
			rc = False
			if quick:
				method = 'quick_check'
			else:
				method = 'integrity_check'
			if amount is not None and amount >= 0:
				method += '(%d)' % amount
			count = 0
			for r in self.queryc ('PRAGMA %s' % method, rtype = DBResultType.Array):
				count += 1
				if r[0]:
					if count == 1 and r[0] == 'ok':
						rc = True
					else:
						rc = False
					if collect is not None:
						collect.append (r[0])
			return rc

	class DBSQLite3 (DBCore):
		def __init__ (self, filename, layout = None):
			DBCore.__init__ (self, 'sqlite', sqlite3, DBCursorSQLite3)
			self.filename = filename
			self.layout = layout
			self.functions = []
		
		def __function (self, name, args):
			self.functions.append ((name, args))
			if self.db is not None:
				name (*args)
		def __createFunction (self, name, noOfParam, method):
			self.db.create_function (name, noOfParam, method)
		def createFunction (self, name, noOfParam, method):
			self.__function (self.__createFunction, (name, noOfParam, method))
		def __createCollation (self, name, method):
			self.db.create_collation (name, method)
		def createCollation (self, name, method):
			self.__function (self.__createCollation, (name, method))
		def __createAggregate (self, name, noOfParam, cls):
			self.db.create_aggregate (name, noOfParam, cls)
		def createAggregate (self, name, noOfParam, cls):
			self.__function (self.__createAggregate, (name, noOfParam, cls))

		def connect (self):
			isNew = not os.path.isfile (self.filename)
			self.db = self.driver.connect (self.filename)
			if self.db is not None:
				for (name, args) in self.functions:
					name (*args)
				if isNew and self.layout:
					c = self.db.cursor ()
					if type (self.layout) in types.StringTypes:
						layout = [self.layout]
					else:
						layout = self.layout
					for stmt in layout:
						c.execute (stmt)
					c.close ()
	SDBase = DBSQLite3
except ImportError:
	DBSQLite3 = None

class DBConfig (object):
	configPath = '/opt/agnitas.com/etc/dbcfg'
	def __init__ (self, path = None):
		if path is None:
			path = self.configPath
		self.path = path
		self.data = None

	class DBRecord (object):
		def __init__ (self, param):
			self.data = {}
			if param:
				for elem in [_p.strip () for _p in param.split (',')]:
					elem = elem.strip ()
					parts = [_e.strip () for _e in elem.split ('=', 1)]
					if len (parts) == 2:
						self.data[parts[0]] = parts[1]
		
		def __getitem__ (self, id):
			return self.data[id]
		
		def __contains__ (self, id):
			return id in self.data
	
		def __call__ (self, id, dflt = None):
			try:
				return self[id]
			except KeyError:
				return dflt

	parseLine = re.compile ('([a-z0-9._+-]+):[ \t]*(.*)$', re.IGNORECASE)
	def __read (self):
		self.data = {}
		fd = open (self.path, 'r')
		for line in fd:
			line = line.strip ()
			if not line or line.startswith ('#'):
				continue
			mtch = self.parseLine.match (line)
			if mtch is None:
				continue
			(id, param) = mtch.groups ()
			self.data[id] = self.DBRecord (param)
		
	def __setup (self):
		if self.data is None:
			self.__read ()

	def __getitem__ (self, id):
		self.__setup ()
		return self.data[id]
	
	def inject (self, id, param):
		self.__setup ()
		rec = self.DBRecord (None)
		rec.data = param
		self.data[id] = rec

class DBDriver (object):
	dbmsdefault = 'oracle'
	dbid = 'agnitas.emm'
	dbcfg = None
	@classmethod
	def request (cls, id):
		if id is None:
			id = cls.dbid
		if cls.dbcfg is None:
			cls.dbcfg = DBConfig ()
		cfg = cls.dbcfg[id]
		dbms = cfg ('dbms', cls.dbmsdefault)
		def dbvalidate (dbcls):
			if dbcls is None:
				raise error ('no driver for "%s" available' % dbms)
		if dbms == 'oracle':
			dbvalidate (DBOracle)
			return DBOracle (cfg ('user'), cfg ('password'), cfg ('sid'))
		elif dbms == 'mysql':
			dbvalidate (DBMySQL)
			return DBMySQL (cfg ('host'), cfg ('user'), cfg ('password'), cfg ('name'))
		elif dbms in ('sqlite', 'sqlite3'):
			dbvalidate (DBSQLite3)
			return DBSQLite3 (cfg ('filename'))
		raise error ('Missing/unknwon dbms "%s" found' % dbms)

	@classmethod
	def inject (cls, id, param):
		if cls.dbcfg is None:
			cls.dbcfg = DBConfig ()
		cls.dbcfg.inject (id, param)

def DBaseID (dbid = None):
	return DBDriver.request (dbid)

class Datasource:
	def __init__ (self):
		self.cache = {}
		
	def getID (self, desc, companyID, sourceGroup, db = None):
		try:
			rc = self.cache[desc]
		except KeyError:
			rc = None
			if db is None:
				db = DBaseID ()
				dbOpened = True
			else:
				dbOpened = False
			if not db is None:
				curs = db.cursor ()
				if not curs is None:
					for state in [0, 1]:
						for rec in curs.query ('SELECT datasource_id FROM datasource_description_tbl WHERE company_id = %d AND description = :description' % companyID, {'description': desc}):
							rc = int (rec[0])
						if rc is None and state == 0:
							query = curs.qselect (oracle = \
								'INSERT INTO datasource_description_tbl (datasource_id, description, company_id, sourcegroup_id, timestamp) VALUES ' + \
								'(datasource_description_tbl_seq.nextval, :description, %d, %d, sysdate)' % (companyID, sourceGroup), \
								mysql = \
								'INSERT INTO datasource_description_tbl (description, company_id, sourcegroup_id, creation_date) VALUES ' + \
								'(:description, %d, %d, current_timestamp)' % (companyID, sourceGroup))
							curs.update (query, {'description': desc}, commit = True)
					curs.close ()
				if dbOpened:
					db.close ()
			if not rc is None:
				self.cache[desc] = rc
		return rc


class ParseTimestamp (object):
	__parser = [
		(re.compile ('([0-9]{4})-([0-9]{2})-([0-9]{2})( ([0-9]{2}):([0-9]{2}):([0-9]{2}))?'), (0, 1, 2)),
		(re.compile (' *([0-9]{1,2})\\.([0-9]{1,2})\\.([0-9]{4})( ([0-9]{2}):([0-9]{2}):([0-9]{2}))?'), (2, 1, 0))
	]
	def __call__ (self, s):
		if s is not None:
			ty = type (s)
			if ty in types.StringTypes:
				for (pattern, seq) in self.__parser:
					m = pattern.match (s)
					if m is not None:
						g = m.groups ()
						if g[3] is None:
							return datetime.datetime (int (g[seq[0]]), int (g[seq[1]]), int (g[seq[2]]))
						else:
							return datetime.datetime (int (g[seq[0]]), int (g[seq[1]]), int (g[seq[2]]), int (g[4]), int (g[5]), int (g[6]))
				if s == 'now':
					now = time.localtime ()
					return datetime.datetime (now.tm_year, now.tm_mon, now.tm_mday, now.tm_hour, now.tm_min, now.tm_sec)
				elif s == 'epoch':
					return datetime.datetime (1970, 1, 1)
			elif ty in (int, long, float):
				tm = time.localtime (s)
				return datetime.datetime (tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec)
			elif ty is datetime.datetime:
				return s
		return None
	
	def dump (self, d):
		return '%04d-%02d-%02d %02d:%02d:%02d' % (d.year, d.month, d.day, d.hour, d.minute, d.second)

class Timestamp (object):
	def __init__ (self, name, initialTimestamp = None):
		self.name = name
		self.initialTimestamp = initialTimestamp
		self.db = None
		self.mydb = False
		self.cursor = None
		self.parm = {'name': self.name}
		self.lowmark = None
		self.highmark = None
		self.parseTimestamp = ParseTimestamp ()
		
	def __cleanup (self):
		if not self.cursor is None:
			self.cursor.close ()
			self.cursor = None
		if self.mydb and not self.db is None:
			self.db.close ()
			self.db = None

	def __setup (self, db):
		if db is None:
			self.db = DBaseID ()
			self.mydb = True
		else:
			self.db = db
			self.mydb = False
		if self.db is None:
			raise error ('Failed to setup database')
		self.cursor = self.db.cursor ()
		if self.cursor is None:
			self.__cleanup ()
			raise error ('Failed to get database cursor')
		count = self.cursor.querys ('SELECT count(*) FROM timestamp_tbl WHERE name = :name', self.parm)
		if count is None or not count[0]:
			rc = self.cursor.querys ('SELECT max (timestamp_id) + 1 FROM timestamp_tbl')
			if not rc is None and not rc[0] is None:
				tid = rc[0]
			else:
				tid = 1
			ts = self.parseTimestamp (self.initialTimestamp)
			if ts is None:
				ts = datetime.datetime (1980, 1, 1)
			if self.cursor.execute ('INSERT INTO timestamp_tbl (timestamp_id, name, cur) VALUES (:tid, :name, :ts)', {'tid': tid, 'name': self.name, 'ts': ts}) != 1:
				self.__cleanup ()
				raise error ('Failed to create new entry in timestamp table')
		elif count[0] != 1:
			raise error ('Expect one entry with name "%s", but found %d' % (self.name, count[0]))
		
	def done (self, commit = True):
		rc = False
		if not self.cursor is None:
			if commit:
				if self.cursor.execute ('UPDATE timestamp_tbl SET prev = cur WHERE name = :name', self.parm) == 1 and \
				   self.cursor.execute ('UPDATE timestamp_tbl SET cur = temp WHERE name = :name', self.parm) == 1:
					self.cursor.sync ()
					rc = True
			else:
				self.cursor.sync (False)
				rc = True
		self.__cleanup ()
		if not rc:
			raise error ('Failed to finalize timestamp entry')
		
	def setup (self, db = None, timestamp = None):
		self.__setup (db)
		timestamp = self.parseTimestamp (timestamp)
		if timestamp is None:
			ts = '%(sysdate)s'
			parm = self.parm
		else:
			ts = ':ts'
			parm = self.parm.copy ()
			parm['ts'] = timestamp
		if self.cursor.execute (self.cursor.rselect ('UPDATE timestamp_tbl SET temp = %s WHERE name = :name' % ts), parm) != 1:
			raise error ('Failed to setup timestamp for current time')
		rc = self.cursor.querys ('SELECT cur, temp FROM timestamp_tbl WHERE name = :name', self.parm)
		if not rc is None:
			(self.lowmark, self.highmark) = rc
		self.cursor.sync ()
		
	def __prepName (self, parm):
		if parm is None:
			name = '\'%s\'' % self.name.replace ('\'', '\'\'')
		else:
			name = ':timestampName'
			parm[name[1:]] = self.name
		return name
		
	def makeSelectLowerMark (self, parm = None):
		return 'SELECT cur FROM timestamp_tbl WHERE name = %s' % self.__prepName (parm)
		
	def makeSelectUpperMark (self, parm = None):
		return 'SELECT temp FROM timestamp_tbl WHERE name = %s' % self.__prepName (parm)
		
	def makeBetweenClause (self, column, parm = None):
		name = self.__prepName (parm)
		return '(%(column)s >= (SELECT cur FROM timestamp_tbl WHERE name = %(name)s) AND %(column)s < (SELECT temp FROM timestamp_tbl WHERE name = %(name)s))' % \
			{'column': column, 'name': name}

	def makeIntervalClause (self, column):
		return '(%(column)s >= :timestampStart AND %(column)s < :timestampEnd)' % {'column': column}

	def makeInterval (self):
		start = self.lowmark
		end = self.highmark
		if start is None:
			start = self.parseTimestamp ('epoch')
		if end is None:
			end = self.parseTimestamp ('now')
		return struct (start = start, end = end)

	def makeCascadingInterval (self, days = 1):
		rc = None
		interval = self.makeInterval ()
		if interval is not None:
			if days < 1:
				days = 1
			cur = interval.start
			while cur < interval.end:
				step = cur.fromordinal (cur.toordinal () + days)
				if step > interval.end:
					step = interval.end
				if rc is None:
					rc = []
				rc.append (struct (start = cur, end = step))
				cur = step
		return rc

	def makeCascadingQueries (self, column, param = None, days = 1):
		rc = None
		cc = self.makeCascadingInterval (days)
		if cc is not None:
			rc = struct (clause = self.makeIntervalClause (column), cascade = [])
			for c in cc:
				if param is not None:
					p = param.copy ()
				else:
					p = {}
				p['timestampStart'] = c.start
				p['timestampEnd'] =  c.end
				rc.cascade.append (p)
		return rc
#}}}
#
# 9.) Simple templating
#
#{{{
class MessageCatalog:
	"""class MessageCatalog:

This class is primary designed to be integrated in the templating system,
but can also be used stand alone. You instanciate the class with a file
name of the message file which contains of a default section (starting
from top or introduced by a section "[*]". For each supported language
you add a section with the language token, e.g. "[de]" for german and
a list of tokens with the translation. A message catalog file may look
like this:
#	comments start as usual with a hash sign
#	this is the default section
yes: Yes
no: No
#
#	this is the german version
[de]
yes: Ja
no: Nein

You may extend an entry over the current line with a trailing backslash.

If you pass a message catalog to the templating system, you can refer
to the catalog by either using ${_['token']} to just translate one token
or using ${_ ('In your mother language YES means %(yes)')}. There are
also shortcut versions for ${_['xxx']) can be written as _[xxx] and
${_ ('xxx')} can be written as _{xxx}.

In a stand alone variante, this looks like this:
>>> m = MessageCatalog ('/some/file/name')
>>> m.setLang ('de')
>>> print m['yes']
Ja
>>> print m ('yes')
yes
>>> print m ('yes in your language is %(yes)')
yes in your language is Ja
>>> print m['unset']
*unset*
>>> m.setFill (None)
>>> print m['unset']
unset

As you can see in the last example an unknown token is expanded to itself
surrounded by a fill string, if set (to easyly catch missing tokens). If
you unset the fill string, the token itself is used with no further
processing.
"""
	messageParse = re.compile ('%\\(([^)]+)\\)')
	commentParse = re.compile ('^[ \t]*#')
	def __init__ (self, fname, lang = None, fill = '*'):
		self.messages = {None: {}}
		self.lang = None
		self.fill = fill
		if not fname is None:
			cur = self.messages[None]
			fd = open (fname, 'r')
			for line in [_l.strip () for _l in fd.read ().replace ('\\\n', '').split ('\n') if _l and self.commentParse.match (_l) is None]:
				if len (line) > 2 and line.startswith ('[') and line.endswith (']'):
					lang = line[1:-1]
					if lang == '*':
						lang = None
					if not lang in self.messages:
						self.messages[lang] = {}
					cur = self.messages[lang]
				else:
					parts = line.split (':', 1)
					if len (parts) == 2:
						(token, msg) = [_p.strip () for _p in parts]
						if len (msg) >= 2 and msg[0] in '\'"' and msg[-1] == msg[0]:
							msg = msg[1:-1]
						cur[token] = msg
			fd.close ()
	
	def __setitem__ (self, token, s):
		try:
			self.messages[self.lang][token] = s
		except KeyError:
			self.messages[self.lang] = {token: s}
	
	def __getitem__ (self, token):
		try:
			msg = self.messages[self.lang][token]
		except KeyError:
			if not self.lang is None:
				try:
					msg = self.messages[None][token]
				except KeyError:
					msg = None
			else:
				msg = None
		if msg is None:
			if self.fill is None:
				msg = token
			else:
				msg = '%s%s%s' % (self.fill, token, self.fill)
		return msg

	def __call__ (self, s):
		return self.messageParse.subn (lambda m: self[m.groups ()[0]], s)[0]
	
	def setLang (self, lang):
		self.lang = lang
	
	def setFill (self, fill):
		self.fill = fill

class Template:
	"""class Template:

This class offers a simple templating system. One instance the class
using the template in string from. The syntax is inspirated by velocity,
but differs in serveral ways (and is even simpler). A template can start
with an optional code block surrounded by the tags '#code' and '#end'
followed by the content of the template. Access to variables and
expressions are realized by $... where ... is either a simple varibale
(e.g. $var) or something more complex, then the value must be
surrounded by curly brackets (e.g. ${var.strip ()}). To get a literal
'$'sign, just type it twice, so '$$' in the template leads into '$'
in the output. A trailing backslash removes the following newline to
join lines.

Handling of message catalog is either done by calling ${_['...']} and
${_('...')} or by using the shortcut _[this is the origin] or
_{%(message): %(error)}. As this is a simple parser the brackets
must not part of the string in the shortcut, in this case use the
full call.

Control constructs must start in a separate line, leading whitespaces
ignoring, with a hash '#' sign. These constructs are supported and
are mostly transformed directly into a python construct:
	
## ...                      this introduces a comment up to end of line
#property(expr)             this sets a property of the template
#pragma(expr)               alias for property
#include(expr)              inclusion of file, subclass must realize this
#if(pyexpr)             --> if pyexpr:
#elif(pyexpr)           --> elif pyexpr:
#else                   --> else
#do(pycmd)              --> pycmd
#pass                   --> pass [same as #do(pass)]
#break			--> break [..]
#continue		--> continue [..]
#for(pyexpr)            --> for pyexpr:
#while(pyexpr)          --> while pyexpr:
#try                    --> try:
#except(pyexpr)         --> except pyexpr:
#finally                --> finally
#with(pyexpr)           --> with pyexpr:
#end                        ends an indention level
#stop                       ends processing of input template

To fill the template you call the method fill(self, namespace, lang = None)
where 'namespace' is a dictonary with names accessable by the template.
Beside, 'lang' could be set to a two letter string to post select language
specific lines from the text. These lines must start with a two letter
language ID followed by a colon, e.g.:
	
en:This is an example.
de:Dies ist ein Beispiel.

Depending on 'lang' only one (or none of these lines) are outputed. If lang
is not set, these lines are put (including the lang ID) both in the output.
If 'lang' is set, it is also copied to the namespace, so you can write the
above lines using the template language:

#if(lang=='en')
This is an example.
#elif(lang=='de')
Dies ist ein Beispiel.
#end

And for failsafe case, if lang is not set:

#try
 #if(lang=='en')
This is an example.
 #elif(lang=='de')
Dies ist ein Beispiel.
 #end
#except(NameError)
 #pass
#end
"""
	codeStart = re.compile ('^[ \t]*#code[^\n]*\n', re.IGNORECASE)
	codeEnd = re.compile ('(^|\n)[ \t]*#end[^\n]*(\n|$)', re.IGNORECASE | re.MULTILINE)
	token = re.compile ('((^|\n)[ \t]*#(#|property|pragma|include|if|elif|else|do|pass|break|continue|for|while|try|except|finally|with|end|stop)|\\$(\\$|[0-9a-z_]+(\\.[0-9a-z_]+)*|\\{[^}]*\\})|_(\\[[^]]+\\]|{[^}]+}))', re.IGNORECASE | re.MULTILINE)
	rplc = re.compile ('\\\\|"|\'|\n|\r|\t|\f|\v', re.MULTILINE)
	rplcMap = {'\n': '\\n', '\r': '\\r', '\t': '\\t', '\f': '\\f', '\v': '\\v'}
	langID = re.compile ('^([ \t]*)([a-z][a-z]):', re.IGNORECASE)
	emptyCatalog = MessageCatalog (None, fill = None)
	def __init__ (self, content, precode = None, postcode = None):
		self.content = content
		self.precode = precode
		self.postcode = postcode
		self.compiled = None
		self.properties = {}
		self.namespace = None
		self.code = None
		self.indent = None
		self.empty = None
		self.compileErrors = None
	
	def __getitem__ (self, var):
		if not self.namespace is None:
			try:
				val = self.namespace[var]
			except KeyError:
				val = ''
		else:
			val = None
		return val
	
	def __setProperty (self, expr):
		try:
			(var, val) = [_e.strip () for _e in expr.split ('=', 1)]
			if len (val) >= 2 and val[0] in '"\'' and val[-1] == val[0]:
				quote = val[0]
				self.properties[var] = val[1:-1].replace ('\\%s' % quote, quote).replace ('\\\\', '\\')
			elif val.lower () in ('true', 'on', 'yes'):
				self.properties[var] = True
			elif val.lower () in ('false', 'off', 'no'):
				self.properties[var] = False
			else:
				try:
					self.properties[var] = int (val)
				except ValueError:
					self.properties[var] = val
		except ValueError:
			var = expr.strip ()
			if var:
				self.properties[var] = True
			
	def __indent (self):
		if self.indent:
			self.code += ' ' * self.indent
	
	def __code (self, code):
		self.__indent ()
		self.code += '%s\n' % code
		if code:
			if code[-1] == ':':
				self.empty = True
			else:
				self.empty = False
			
	def __deindent (self):
		if self.empty:
			self.__code ('pass')
		self.indent -= 1
	
	def __compileError (self, start, errtext):
		if not self.compileErrors:
			self.compileErrors = ''
		self.compileErrors += '** %s: %s ...\n\n\n' % (errtext, self.content[start:start + 60])

	def __replacer (self, mtch):
		rc = []
		for ch in mtch.group (0):
			try:
				rc.append (self.rplcMap[ch])
			except KeyError:
				rc.append ('\\x%02x' % ord (ch))
		return ''.join (rc)
	
	def __escaper (self, s):
		return s.replace ('\'', '\\\'')

	def __compileString (self, s):
		self.__code ('__result.append (\'%s\')' % re.sub (self.rplc, self.__replacer, s))
			
	def __compileExpr (self, s):
		self.__code ('__result.append (str (%s))' % s)

	def __compileCode (self, token, arg):
		if not token is None:
			if arg:
				self.__code ('%s %s:' % (token, arg))
			else:
				self.__code ('%s:' % token)
		elif arg:
			self.__code (arg)
					
	def __compileContent (self):
		self.code = ''
		if self.precode:
			self.code += self.precode
			if self.code[-1] != '\n':
				self.code += '\n'
		pos = 0
		clen = len (self.content)
		mtch = self.codeStart.search (self.content)
		if not mtch is None:
			start = mtch.end ()
			mtch = self.codeEnd.search (self.content, start)
			if not mtch is None:
				(end, pos) = mtch.span ()
				self.code += self.content[start:end] + '\n'
			else:
				self.__compileError (0, 'Unfinished code segment')
		self.indent = 0
		self.empty = False
		self.code += '__result = []\n'
		while pos < clen:
			mtch = self.token.search (self.content, pos)
			if mtch is None:
				start = clen
				end = clen
			else:
				(start, end) = mtch.span ()
				groups = mtch.groups ()
				if groups[1]:
					start += len (groups[1])
			if start > pos:
				self.__compileString (self.content[pos:start])
			pos = end
			if not mtch is None:
				tstart = start
				if not groups[2] is None:
					token = groups[2]
					arg = ''
					if token != '#':
						if pos < clen and self.content[pos] == '(':
							pos += 1
							level = 1
							quote = None
							escape = False
							start = pos
							end = -1
							while pos < clen and level > 0:
								ch = self.content[pos]
								if escape:
									escape = False
								elif ch == '\\':
									escape = True
								elif not quote is None:
									if ch == quote:
										quote = None
								elif ch in '\'"':
									quote = ch
								elif ch == '(':
									level += 1
								elif ch == ')':
									level -= 1
									if level == 0:
										end = pos
								pos += 1
							if start < end:
								arg = self.content[start:end]
							else:
								self.__compileError (tstart, 'Unfinished statement')
						if pos < clen and self.content[pos] == '\n':
							pos += 1
					if token == '#':
						while pos < clen and self.content[pos] != '\n':
							pos += 1
						if pos < clen:
							pos += 1
					elif token in ('property', 'pragma'):
						self.__setProperty (arg)
					elif token in ('include', ):
						try:
							included = self.include (arg)
							if included:
								self.content = self.content[:pos] + included + self.content[pos:]
								clen += len (included)
						except error, e:
							self.__compileError (tstart, 'Failed to include "%s": %s' % (arg, e.msg))
					elif token in ('if', 'else', 'elif', 'for', 'while', 'try', 'except', 'finally', 'with'):
						if token in ('else', 'elif', 'except', 'finally'):
							if self.indent > 0:
								self.__deindent ()
							else:
								self.__compileError (tstart, 'Too many closeing blocks')
						if (arg and token in ('if', 'elif', 'for', 'while', 'except', 'with')) or \
						   (not arg and token in ('else', 'try', 'finally')):
							self.__compileCode (token, arg)
						elif arg:
							self.__compileError (tstart, 'Extra arguments for #%s detected' % token)
						else:
							self.__compileError (tstart, 'Missing statement for #%s' % token)
						self.indent += 1
					elif token in ('pass', 'break', 'continue'):
						if arg:
							self.__compileError (tstart, 'Extra arguments for #%s detected' % token)
						else:
							self.__compileCode (None, token)
					elif token in ('do', ):
						if arg:
							self.__compileCode (None, arg)
						else:
							self.__compileError (tstart, 'Missing code for #%s' % token)
					elif token in ('end', ):
						if arg:
							self.__compileError (tstart, 'Extra arguments for #end detected')
						if self.indent > 0:
							self.__deindent ()
						else:
							self.__compileError (tstart, 'Too many closing blocks')
					elif token in ('stop', ):
						pos = clen
				elif not groups[3] is None:
					expr = groups[3]
					if expr == '$':
						self.__compileString ('$')
					else:
						if len (expr) >= 2 and expr[0] == '{' and expr[-1] == '}':
							expr = expr[1:-1]
						self.__compileExpr (expr)
				elif not groups[5] is None:
					expr = groups[5]
					if expr[0] == '[':
						self.__compileExpr ('_[\'%s\']' % self.__escaper (expr[1:-1]))
					elif expr[0] == '{':
						self.__compileExpr ('_ (\'%s\')' % self.__escaper (expr[1:-1]))
				elif not groups[0] is None:
					self.__compileString (groups[0])
		if self.indent > 0:
			self.__compileError (0, 'Missing %d closing #end statement(s)' % self.indent)
		if self.compileErrors is None:
			if self.postcode:
				if self.code and self.code[-1] != '\n':
					self.code += '\n'
				self.code += self.postcode
			self.compiled = compile (self.code, '<template>', 'exec')
	
	def include (self, arg):
		raise error ('Subclass responsible for implementing "include (%r)"' % arg)

	def property (self, var):
		try:
			return self.properties[var]
		except KeyError:
			return None

	def compile (self):
		if self.compiled is None:
			try:
				self.__compileContent ()
				if self.compiled is None:
					raise error ('Compilation failed: %s' % self.compileErrors)
			except Exception, e:
				raise error ('Failed to compile [%r] %r:\n%s\n' % (type (e), e.args, self.code))

	def fill (self, namespace, lang = None, mc = None):
		if self.compiled is None:
			self.compile ()
		if namespace is None:
			self.namespace = {}
		else:
			self.namespace = namespace.copy ()
		if not lang is None:
			self.namespace['lang'] = lang
		self.namespace['property'] = self.properties
		if mc is None:
			mc = self.emptyCatalog
		mc.setLang (lang)
		self.namespace['_'] = mc
		try:
			exec self.compiled in self.namespace
		except Exception, e:
			raise error ('Execution failed [%s]: %s' % (e.__class__.__name__, str (e)))
		result = ''.join (self.namespace['__result'])
		if not lang is None:
			nresult = []
			for line in result.split ('\n'):
				mtch = self.langID.search (line)
				if mtch is None:
					nresult.append (line)
				else:
					(pre, lid) = mtch.groups ()
					if lid.lower () == lang:
						nresult.append (pre + line[mtch.end ():])
			result = '\n'.join (nresult)
		result = result.replace ('\\\n', '')
		self.namespace['result'] = result
		return result
#}}}

#
# 10.) Spare
#
#{{{
#}}}
#
# 11.) SSH Wrapper/command issuer
#
#{{{
class Remote:
	def __init__ (self, addr):
		self.addr = addr
		self.wrap = 'wrap'
		self.ssh = 'ssh'
		self.scp = 'scp'
		self.timeout = 30
	
	def __wraprc (self, pgm):
		os.environ['WRAPRC'] = 'timeout=%s,program=%s,0+,2-' % (self.timeout, pgm)

	def __ssh (self, cmd):
		self.__wraprc (self.ssh)
		n = os.system ('%s "%s" %s' % (self.wrap, self.addr, cmd))
		if n:
			log (LV_ERROR, '__ssh', 'Failed to execute "%s" for "%s" (%d)' % (cmd, self.addr, n))
			return False
		return True

	def __sshread (self, cmd):
		self.__wraprc (self.ssh)
		pp = os.popen ('%s "%s" %s' % (self.wrap, self.addr, cmd), 'r')
		data = pp.read ()
		n = pp.close ()
		if n:
			log (LV_ERROR, '__sshread', 'Failed to execute "%s" for "%s" (%d)' % (cmd, self.addr, n))
			data = None
		return data
	
	def __sshwrite (self, cmd, data):
		self.__wraprc (self.ssh)
		pp = os.popen ('%s "%s" %s' % (self.wrap, self.addr, cmd), 'w')
		pp.write (data)
		n = pp.close ()
		if n:
			log (LV_ERROR, '__sshwrite', 'Failed to execute "%s" for "%s" (%d)' % (cmd, self.addr, n))
			return False
		return True
	
	def __scp (self, src, dest, toremote):
		self.__wraprc (self.scp)
		cmd = '%s -qp' % self.wrap
		if type (src) in types.StringTypes:
			src = [ src ]
		else:
			cmd += ' -d'
		for s in src:
			if toremote:
				cmd += ' "%s"' % s
			else:
				cmd += ' "%s:%s"' % (self.addr, s)
		if toremote:
			cmd += ' "%s:%s"' % (self.addr, dest)
		else:
			cmd += ' "%s"' % dest
			
		n = os.system (cmd)
		if not n:
			return True
		return False
	
	def listdir (self, directory = None, mask = None):
		rc = None
		cmd = 'ls -a -1'
		if directory is not None:
			cmd += ' "%s"' % directory
		data = self.__sshread (cmd)
		if data and len (data) > 0:
			rc = data.split ('\n')
			for unwanted in [ '', '.', '..' ]:
				if unwanted in rc:
					rc.remove (unwanted)
			rc.sort ()
			if mask:
				reg = re.compile (mask)
				temp = []
				for fname in rc:
					if reg.match (fname):
						temp.append (fname)
				rc = temp
		return rc
	
	def getcmd (self, cmd):
		return self.__sshread (cmd)

	def putcmd (self, cmd, data):
		return self.__sshwrite (cmd, data)

	def putfile (self, local, remote):
		return self.__scp (local, remote, True)
	
	def getfile (self, remote, local):
		return self.__scp (remote, local, False)
		
	def removefile (self, remote):
		return self.__ssh ('rm "%s"' % remote)
	
	def do (self, cmd):
		return self.__ssh (cmd)

class RemotePassword (Remote):
	opensshPath = '/opt/agnitas.com/software/openssh'
	opensshPassfile = mkpath (base, '.ssh_password')
	def __init__ (self, addr, prompt = None, passphrase = None, passfile = None):
		Remote.__init__ (self, addr)
		self.ssh = mkpath (self.opensshPath, 'bin', 'ssh')
		self.scp = mkpath (self.opensshPath, 'bin', 'scp')
		if prompt is not None and passphrase is not None:
			report = []
			if passfile is None:
				passfile = self.opensshPassfile
			if not fileExistance (passfile, '%s\t%s\n' % (prompt, passphrase), 0600, report):
				raise error ('failed to create password file: %s' % ', '.join (report))
#}}}
#
# 12.) Simple DNS class
#
#{{{
try:
	import	DNS
	
	class sDNS:
		initialized = False
		
		def __init__ (self):
			if not self.initialized:
				DNS.DiscoverNameServers ()
				sDNS.initialized = True
			self.req = DNS.Request ()
		
		def __req (self, name, qtype, func):
			try:
				ans = self.req.req (name = name, qtype = qtype)
				return func ([_a['data'] for _a in ans.answers])
			except DNS.Error:
				return None
		
		def __a (self, ans):
			return ans
		def getAddress (self, name):
			return self.__req (name, 'A', self.__a)
		
		def __ptr (self, ans):
			return ans
		def getHostname (self, ipaddr):
			parts = ipaddr.split ('.')
			parts.reverse ()
			name = '%s.in-addr.arpa' % '.'.join (parts)
			return self.__req (name, 'PTR', self.__ptr)
		
		def __mx (self, ans):
			return [_a[1] for _a in ans]
		def getMailexchanger (self, name):
			return self.__req (name, 'MX', self.__mx)
		
		def __txt (self, ans):
			rc = []
			for a in ans:
				rc += a
			return rc
		def getText (self, name):
			return self.__req (name, 'TXT', self.__txt)
except ImportError:
	class sDNS:
		class Helper:
			def __init__ (self, command, matcher):
				self.command = command
				self.matcher = matcher
			
			def available (self):
				return os.path.isfile (self.command) and os.access (self.command, os.X_OK)

			def __call__ (self, typ, parm, correct = None):
				match = self.matcher[typ]
				if type (match) in (types.ListType, types.TupleType) and len (match) == 2:
					(patternFormat, pos) = match
				else:
					patternFormat = match
					pos = 0
				pp = subprocess.Popen ([self.command, '-t', typ, parm], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
				(pout, perr) = pp.communicate (None)
				self.status = [pp.returncode, pout, perr]
				if pp.returncode != 0:
					raise error ('Failed to query %s' % parm)
				rc = []
				pattern = re.compile (patternFormat % {'parm': parm})
				for line in pout.split ('\n'):
					mtch = pattern.match (line)
					if not mtch is None:
						data = mtch.groups ()
						if len (data) > pos:
							data = data[pos]
							if not correct is None:
								data = correct (data)
						rc.append (data)
				return rc

		helpers = [Helper ('/usr/bin/host', {
				'a': '^%(parm)s has address ([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)$',
				'ptr': '^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+\\.in-addr\\.arpa domain name pointer ([^ ]+)$',
				'mx': '^%(parm)s mail is handled by [0-9]+ ([^ ]+)$',
				'txt': ('^%(parm)s (descriptive )?text "([^"]*)"$', 1)}),
			   Helper ('/usr/bin/hostx', {
			   	'a': '^%(parm)s[ \t]+A[ \t]+([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)$',
				'ptr': '^Name: +([^ ]+)$',
				'mx': '^%(parm)s[ \t]+MX[ \t]+[0-9]+[ \t]+([^ ]+)$',
				'txt': '^%(parm)s[ \t]+TXT[ \t]+"([^"]*)"$'})
			  ]
		
		def __init__ (self):
			helper = None
			for h in self.helpers:
				if h.available ():
					helper = h
					break
			if helper is None:
				raise error ('No external command for DNS queries found')
			self.helper = helper
			self.status = None
	
		def __std (self, s):
			if s.endswith ('.'):
				return s[:-1]
			return s
	
		def getAddress (self, name):
			return self.helper ('a', name)
		
		def getHostname (self, ipaddr):
			return self.helper ('ptr', ipaddr, self.__std)
		
		def getMailexchanger (self, name):
			return self.helper ('mx', name, self.__std)
		
		def getText (self, name):
			return self.helper ('txt', name)
#}}}
#
# 13.) Wrapper for Statd
#
#{{{
def statdProxy (hostname):
	return xmlrpclib.ServerProxy ('http://%s:8300' % hostname, allow_none = True)
#}}}
#
# 14.) IP Access control
#
#{{{
class IPEntry:
	def __init__ (self, pattern, allow):
		self.allow = allow
		self.parsed = [[0, 255], [0, 255], [0, 255], [0, 255]]
		elem = pattern.split ('.')
		n = 0
		while n < 4 and n < len (elem):
			r = elem[n].split ('-')
			if len (r) == 1:
				self.parsed[n] = [int (r[0]), int (r[0])]
			else:
				self.parsed[n] = [int (r[0]), int (r[1])]
			n += 1
	
	def match (self, ip):
		if ip[0] >= self.parsed[0][0] and ip[0] <= self.parsed[0][1] and \
		   ip[1] >= self.parsed[1][0] and ip[1] <= self.parsed[1][1] and \
		   ip[2] >= self.parsed[2][0] and ip[2] <= self.parsed[2][1] and \
		   ip[3] >= self.parsed[3][0] and ip[3] <= self.parsed[3][1]:
		   	return self.allow
		return not self.allow

class IPAccess:
	def __init__ (self, dflt):
		self.dflt = dflt
		self.acl = []
	
	def add (self, pattern, allow):
		try:
			e = IPEntry (pattern, allow)
			self.acl.append (e)
		except:
			log (LV_ERROR, 'ipaccess', 'Invalid pattern found: ' + pattern)
			
	def deny (self, pattern):
		self.add (pattern, False)
	
	def allow (self, pattern):
		self.add (pattern, True)
	
	def verify (self, addr):
		rc = False
		elem = addr.split ('.')
		if len (elem) == 4:
			try:
				ip = [int (elem[0]), int (elem[1]), int (elem[2]), int (elem[3])]
				for i in ip:
					if i < 0 or i > 255:
						raise ValueError ('IP %d out of range' % i)
				rc = self.dflt
				for a in self.acl:
					if a.match (ip):
						rc = a.allow
						break
			except ValueError:
				pass
		return rc
#}}}
#
# 15.) CSV Simplifier
#
#{{{
class _CSVBase (csv.Dialect):
	doublequote = True
	escapechar = '\\'
	lineterminator = '\r\n'
	quotechar = '"'
	quoting = csv.QUOTE_NONE
	skipinitialspace = True
class _CSVSemicolon (_CSVBase):
	delimiter = ';'
class _CSVSemicolon1 (_CSVSemicolon):
	quoting = csv.QUOTE_MINIMAL
class _CSVSemicolon2 (_CSVSemicolon):
	quoting = csv.QUOTE_ALL
class _CSVComma (_CSVBase):
	delimiter = ','
class _CSVComma1 (_CSVComma):
	quoting = csv.QUOTE_MINIMAL
class _CSVComma2 (_CSVComma):
	quoting = csv.QUOTE_ALL
class _CSVTAB (_CSVBase):
	delimiter = '\t'
class _CSVTAB1 (_CSVTAB):
	quoting = csv.QUOTE_MINIMAL
class _CSVTAB2 (_CSVTAB):
	quoting = csv.QUOTE_ALL
class _CSVBar (_CSVBase):
	delimiter = '|'
class _CSVBar1 (_CSVBar):
	quoting = csv.QUOTE_MINIMAL
class _CSVBar2 (_CSVBar):
	quoting = csv.QUOTE_ALL
class _CSVSpace (_CSVBase):
	delimiter = ' '
class _CSVSpace1 (_CSVSpace):
	quoting = csv.QUOTE_MINIMAL
class _CSVSpace2 (_CSVSpace):
	quoting = csv.QUOTE_ALL
class _CSVAuto (_CSVBase):
	pass
_csvregister = [
	('agn-default', _CSVSemicolon1),
	('agn-semicolon-none', _CSVSemicolon),
	('agn-semicolon-minimal', _CSVSemicolon1),
	('agn-semicolon-full', _CSVSemicolon2),
	('agn-comma-none', _CSVComma),
	('agn-comma-minimal', _CSVComma1),
	('agn-comma-full', _CSVComma2),
	('agn-tab-none', _CSVTAB),
	('agn-tab-minimal', _CSVTAB1),
	('agn-tab-full', _CSVTAB2),
	('agn-bar-none', _CSVBar),
	('agn-bar-minimal', _CSVBar1),
	('agn-bar-full', _CSVBar2),
	('agn-space-none', _CSVSpace),
	('agn-space-minimal', _CSVSpace1),
	('agn-space-full', _CSVSpace2)
]
CSVDialects = []
for( _csvname, _csvclass) in _csvregister:
	csv.register_dialect (_csvname, _csvclass)
	CSVDialects.append (_csvname)
CSVDefault = CSVDialects[0]
class CSVIO (object):
	def __init__ (self):
		self.fd = None
		self.foreign = False

	def __del__ (self):
		self.done ()
	
	def __enter__ (self):
		return self
	
	def __exit__ (self, exc_type, exc_value, traceback):
		self.close ()
	
	def open (self, stream, mode):
		if type (stream) in types.StringTypes:
			self.fd = open (stream, mode)
			self.foreign = False
		else:
			self.fd = stream
			self.foreign = True
	
	def done (self):
		if not self.fd is None:
			if not self.foreign:
				self.fd.close ()
			else:
				try:
					if not self.fd.closed:
						self.fd.flush ()
				except AttributeError:
					pass
			self.fd = None
	
	def close (self):
		self.done ()

class _CSVWriter (CSVIO):
	def __init__ (self, stream):
		CSVIO.__init__ (self)
		self.writer = None
		self.open (stream, 'w')
	
	def write (self, row):
		self.writer.writerow (row)
	
class CSVWriter (_CSVWriter):
	def __init__ (self, stream, dialect):
		_CSVWriter.__init__ (self, stream)
		self.writer = csv.writer (self.fd, dialect = dialect)

class CSVDictWriter (_CSVWriter):
	def __init__ (self, stream, fieldList, dialect, relaxed = False):
		_CSVWriter.__init__ (self, stream)
		if relaxed:
			extrasaction = 'ignore'
		else:
			extrasaction = 'raise'
		self.writer = csv.DictWriter (self.fd, fieldList, dialect = dialect, extrasaction = extrasaction)

class _CSVReader (CSVIO):
	def __init__ (self, stream):
		CSVIO.__init__ (self)
		self.reader = None
		self.open (stream, 'r')
	
	def __iter__ (self):
		return self.reader
	
	def read (self):
		try:
			row = self.reader.next ()
		except StopIteration:
			row = None
		return row

class CSVReader (_CSVReader):
	def __init__ (self, stream, dialect):
		_CSVReader.__init__ (self, stream)
		self.reader = csv.reader (self.fd, dialect = dialect)

class CSVDictReader (_CSVReader):
	def __init__ (self, stream, fieldList, dialect, restKey = None, restValue = None):
		_CSVReader.__init__ (self, stream)
		self.reader = csv.DictReader (self.fd, fieldList, dialect = dialect, restkey = restKey, restval = restValue)

class CSVAuto (object):
	validHeader = re.compile ('[0-9a-z_][0-9a-z_-]*', re.IGNORECASE)
	def __init__ (self, fname, dialect = 'agn-auto', linecount = 10):
		self.fname = fname
		self.dialect = dialect
		self.linecount = linecount
		self.header = None
		self.headerLine = None
	
	def __enter__ (self):
		a = self.__analyse ()
		_CSVAuto.delimiter = a.delimiter
		_CSVAuto.doublequote = a.doublequote
		_CSVAuto.escapechar = a.escapechar
		_CSVAuto.lineterminator = a.lineterminator
		_CSVAuto.quotechar = a.quotechar
		_CSVAuto.quoting = a.quoting
		_CSVAuto.skipinitialspace = a.skipinitialspace
		csv.register_dialect (self.dialect, _CSVAuto)
		rd = CSVReader (self.fname, dialect = self.dialect)
		head = rd.read ()
		rd.close ()
		self.headerLine = True
		for h in head:
			if self.validHeader.match (h) is None:
				self.headerLine = False
				break
		if self.headerLine:
			self.header = head
		else:
			self.header = None
		return self
	
	def __exit__ (self, exc_type, exc_value, traceback):
		csv.unregister_dialect (self.dialect)
		self.header = None
		self.headerLine = None
	
	def __analyse (self):
		err = None
		fd = open (self.fname, 'r')
		head = fd.readline ()
		datas = []
		if head == '':
			err = 'Empty input file "%s"' % self.fname
		else:
			n = 0
			while n < self.linecount:
				data = fd.readline ()
				if data == '':
					break
				datas.append (data)
		fd.close ()
		if err:
			raise error (err)
		temp = struct (delimiter = None, doublequote = False, escapechar = None, lineterminator = '\n', quotechar = None, quoting = csv.QUOTE_NONE, skipinitialspace = False)
		if len (head) > 1 and head[-2] == '\r':
			temp.lineterminator = '\r\n'
			head = head[:-2]
		else:
			head = head[:-1]
		if head[0] == '"' or head[0] == '\'':
			temp.quotechar = head[0]
			temp.quoting = csv.QUOTE_ALL
			n = 0
			while True:
				n = head.find (temp.quotechar, n + 1)
				if n != -1 and n + 1 < len (head):
					n += 1
					if head[n] == temp.quotechar:
						temp.doublequote = True
					else:
						temp.delimiter = head[n]
						if n + 1 < len (head) and (head[n + 1] == ' ' or head[n + 1] == '\t'):
							temp.skipinitialspace = True
						break
				else:
					break
		if temp.delimiter is None:
			use = [None, 0]
			most = [None, 0]
			for d in ',;|\t!@#$%^&*?':
				cnt1 = head.count (d)
				cnt2 = cnt1
				for data in datas:
					c = data.count (d)
					if c < cnt2:
						cnt2 = c
				if cnt1 > use[1] and cnt1 == cnt2:
					use = [d, cnt1]
				if cnt1 > most[1]:
					most = [d, cnt1]
			if use[0] is None:
				if most[0] is None:
					raise error ('No delimiter found')
				use = most
			temp.delimiter = use[0]
		if not temp.doublequote:
			for data in datas:
				if '""' in data or '\'\'' in data:
					temp.doublequote = True
					break
		for data in datas:
			if '\\' in data:
				temp.escapechar = '\\'
				break
		return temp
	
	def setup (self):
		self.__enter__ ()
	
	def done (self):
		self.__exit__ (None, None, None)
	
	def reader (self):
		if self.headerLine is None:
			self.setup ()
			temp = True
		else:
			temp = False
		try:
			rd = CSVReader (self.fname, dialect = self.dialect)
			if self.headerLine:
				rd.read ()
		finally:
			if temp:
				self.done ()
		return rd
	
	def writer (self, fname = None, force = False):
		if fname is None or fname == self.fname:
			if fname is None:
				fname = self.fname
			if not force:
				raise error ('Will not override source file w/o being forced to')
		if self.headerLine is None:
			self.setup ()
			temp = True
		else:
			temp = False
		try:
			wr = CSVWriter (fname, dialect = self.dialect)
			if self.headerLine:
				wr.write (self.header)
		finally:
			if temp:
				self.done ()
		return wr

#}}}
#
# 16.) Caching
#
#{{{
class Cache:
	toPattern = re.compile ('([0-9]+)([A-za-z])')
	toUnits = {
		's':	1,
		'm':	60,
		'h':	60 * 60,
		'd':	24 * 60 * 60,
		'w':	7 * 24 * 60 * 60
	}
	def __parse (self, to):
		if type (to) in (str, unicode):
			rc = 0
			for amount, unit in [(int (_c[0]), _c[1]) for _c in self.toPattern.findall (to)]:
				try:
					rc = amount * self.toUnits[unit]
				except KeyError:
					pass
		else:
			rc = to
		return rc
			
	class Entry (object):
		def __init__ (self, value, active):
			if active:
				self.created = time.time ()
			self.value = value
		
		def valid (self, now, timeout):
			return self.created + timeout >= now

	def __init__ (self, limit = 0, timeout = None):
		self.limit = limit
		self.timeout = self.__parse (timeout)
		self.active = self.timeout is not None
		self.count = 0
		self.cache = {}
		self.cacheline = collections.deque ()
	
	def __getitem__ (self, key):
		e = self.cache[key]
		if self.active and not e.valid (time.time (), self.timeout):
			self.remove (key)
			raise KeyError ('%r: expired' % (key, ))
		self.cacheline.remove (key)
		self.cacheline.append (key)
		return e.value
	
	def __setitem__ (self, key, value):
		if key in self.cache:
			self.cacheline.remove (key)
		else:
			if self.limit and self.count >= self.limit:
				drop = self.cacheline.popleft ()
				del self.cache[drop]
			else:
				self.count += 1
		self.cache[key] = self.Entry (value, self.active)
		self.cacheline.append (key)
	
	def __delitem__ (self, key):
		del self.cache[key]
		self.cacheline,remove (key)
		self.count -= 1
	
	def __len__ (self):
		return len (self.cache)
	
	def reset (self):
		self.count = 0
		self.cache = {}
		self.cacheline = collections.deque ()
	
	def remove (self, key):
		if key in self.cache:
			del self[key]

	def expire (self, blocksize = 1000):
		if self.active:
			now = time.time ()
			while True:
				toRemove = []
				for key, e in self.cache.items ():
					if not e.valid (now, self.timeout):
						toRemove.append (key)
						if blocksize and len (toRemove) == blocksize:
							break
				if not toRemove:
					break
				for key in toRemove:
					del self[key]
		if self.limit:
			while self.count > self.limit:
				del self[self.cacheline[0]]
#}}}
#
# 17.) Family
#
#{{{
class Family:
	class Child:
		_incarnation = 0
		def __init__ (self, name = None):
			self._name = name
			if self._name is None:
				self.__class__._incarnation += 1
				self._name = 'Child-%d' % self.__class__._incarnation
			self._pid = -1
			self._status = -1
			self._running = True

		def __catch (self, sig, stack):
			self._running = False
		
		def setupHandler (self):
			signal.signal (signal.SIGTERM, self.__catch)
			signal.signal (signal.SIGINT, signal.SIG_IGN)
			signal.signal (signal.SIGHUP, signal.SIG_IGN)
			signal.signal (signal.SIGPIPE, signal.SIG_IGN)
			signal.signal (signal.SIGCHLD, signal.SIG_DFL)
		
		def exited (self):
			if os.WIFEXITED (self._status):
				return os.WEXITSTATUS (self._status)
			return -1
		
		def terminated (self):
			if os.WIFSIGNALED (self._status):
				return os.WTERMSIG (self._status)
			return -1
		
		def reset (self):
			self._pid = -1
			self._status = -1
			self._running = True
		
		def active (self):
			if self._pid > 0:
				return True
			elif self._pid == 0:
				return self._running
			return False

		def start (self):
			raise error ('Subclass responsible for implementing "start"')

	def __init__ (self):
		signal.signal (signal.SIGCHLD, self.__catch)
		self.children = []
		self.cur = 0
	
	def __catch (self, sig, stack):
		pass
	
	def __iter__ (self):
		self.cur = 0
		return self
	
	def __next__ (self):
		if self.cur == len (self.children):
			raise StopIteration ()
		rc = self.children[self.cur]
		self.cur += 1
		return rc
	next = __next__

	def reset (self):
		if self.children:
			self.debug ('Reset: %s' % ', '.join ([_c._name for _c in self.children]))
		else:
			self.debug ('Reset with no children')
		self.children = []
		
	def add (self, child):
		if child in self.children:
			if not child.active ():
				self.debug ('Reactived %s' % child._name)
				child.reset ()
			else:
				self.debug ('Add already active %s' % child._name)
		else:
			self.debug ('Add %s' % child._name)
			child.reset ()
			self.children.append (child)
	
	def start (self):
		for child in self.children:
			if child._pid == -1 and child._status == -1:
				self.debug ('Starting %s' % child._name)
				child._status = 0
				child._pid = os.fork ()
				if child._pid == 0:
					child.setupHandler ()
					try:
						ec = child.start ()
						if not type (ec) in (types.IntType, types.LongType):
							ec = 0
						self.debug ('%s: Exiting with %d' % (child._name, ec))
					except Exception, e:
						self.debug ('%s: Exiting due to exception: %s' % (child._name, str (e)))
						ec = 127
					os._exit (ec)
	
	def __kill (self, sig):
		for child in self.children:
			if child._pid > 0:
				try:
					os.kill (child._pid, sig)
					self.debug ('%s: signalled with %d' % (child._name, sig))
				except OSError, e:
					self.debug ('%s: failed signalling with %d: %s' % (child._name, sig, str (e)))
	
	def stop (self):
		self.__kill (signal.SIGTERM)
	
	def kill (self):
		self.__kill (signal.SIGKILL)
	
	def wait (self, timeout = -1):
		active = [_c for _c in self.children if _c._pid > 0]
		if timeout > 0:
			start = time.time ()
		while len (active) > 0:
			if timeout >= 0:
				flag = os.WNOHANG
			else:
				flag = 0
			try:
				(pid, status) = os.waitpid (-1, flag)
				if pid > 0:
					for child in active:
						if child._pid == pid:
							child._pid = -1
							child._status = status
							active.remove (child)
							self.debug ('%s: exited with %d' % (child._name, status))
							break
			except OSError, e:
				self.debug ('Wait aborted with %s' % str (e))
				pid = -1
				if e.args[0] == errno.ECHILD:
					for child in active:
						child._pid = -1
						child._status = 127
					active = []
			if timeout == 0:
				break
			elif timeout > 0 and pid <= 0 and len (active) > 0:
				if start + timeout < time.time ():
					break
				time.sleep (1)
		return len (active)
	
	def active (self):
		return self.wait (0)
	
	def clean (self):
		cleaned = []
		for child in [_c for _c in self.children if _c._pid == -1]:
			cleaned.append (child)
			self.children.remove (child)
			self.debug ('Removed %s' % child._name)
		return cleaned
	
	def debug (self, m):
		pass
#}}}
