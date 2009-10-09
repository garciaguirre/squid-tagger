#!/usr/bin/env python3.1

import configparser, optparse, os, postgresql.api, re, sys, _thread

class Logger:
	__slots__ = frozenset(['_syslog'])

	def __init__(self):
		config.section('log')
		if config['silent'] == 'yes':
			self._syslog = None
		else:
			import syslog
			self._syslog = syslog
			self._syslog.openlog('squidTag')

	def info(self, message):
		if self._syslog:
			self._syslog.syslog(self._syslog.LOG_INFO, message)

	def notice(self, message):
		if self._syslog:
			self._syslog.syslog(self._syslog.LOG_NOTICE, message)

class tagDB:
	__slots__ = frozenset(['_prepared', '_check_stmt', '_db'])

	def __init__(self):
		self._prepared = set()
		self._db = False
		self._check_stmt = self._curs().prepare("select redirect_url, regexp from site_rule where site <@ tripdomain($1) and netmask >> $2::text::inet order by array_length(site, 1) desc")

	def _curs(self):
		if not self._db:
			config.section('database')
			self._db = postgresql.open(
				'pq://{0}:{1}@{2}/{3}'.format(
					config['user'],
					config['password'],
					config['host'],
					config['database'],
			) )
		return(self._db)

	def check(self, ip_address, site):
		return self._check_stmt(site, ip_address)

class CheckerThread:
	__slots__ = frozenset(['_db', '_lock', '_lock_queue', '_log', '_queue'])

	def __init__(self, db, log):
		self._db = db
		self._log = log
		# Spin lock. Loop acquires it on start then releases it when holding queue
		# lock. This way the thread proceeds without stops while queue has data and
		# gets stalled when no data present. The lock is released by queue writer
		# after storing something into the queue
		self._lock = _thread.allocate_lock()
		self._lock_queue = _thread.allocate_lock()
		self._lock.acquire()
		self._queue = []
		_thread.start_new_thread(self._start, ())

	def _start(self):
		while True:
			self._lock.acquire()
			self._lock_queue.acquire()
			# yes this should be written this way, and yes, this is why I hate threading
			if len(self._queue) > 1 and self._lock.locked():
				self._lock.release()
			req = self._queue.pop(0)
			self._lock_queue.release()
			self._log.info('trying %s\n'%req[1])
			result = self._db.check(req[2], req[1])
			for row in result:
				if row != None and row[0] != None:
					if row[1] != None:
						self._log.info('trying regexp "{0}" versus "{1}"\n'.format(row[1], req[3]))
						if re.compile(row[1]).match(req[3]):
							writeline('%s 302:%s\n'%(req[0], row[0]))
							break
						else:
							continue
					else:
						writeline('%s 302:%s\n'%(req[0], row[0]))
						break
			writeline('%s -\n'%req[0])

	def check(self, line):
		request = re.compile('^([0-9]+)\ (http|ftp):\/\/([-\w.:]+)\/([^ ]*)\ ([0-9.]+)\/(-|[\w\.]+)\ (-|\w+)\ (-|GET|HEAD|POST).*$').match(line)
		if request:
			id = request.group(1)
			site = request.group(3)
			url_path = request.group(4)
			ip_address = request.group(5)
			self._lock_queue.acquire()
			self._queue.append((id, site, ip_address, url_path))
			if self._lock.locked():
				self._lock.release()
			self._lock_queue.release()
			self._log.info('request %s queued (%s)\n'%(id, line))
		else:
			self._log.info('bad request\n')
			writeline(line)

def writeline(string):
	log.info('sending: %s'%string)
	sys.stdout.write(string)
	sys.stdout.flush()

class Config:
	__slots__ = frozenset(['_config', '_section'])

	def __init__(self):
		parser = optparse.OptionParser()
		parser.add_option('-c', '--config', dest = 'config',
			help = 'config file location', metavar = 'FILE',
			default = '/usr/local/etc/squid-tagger.conf')

		(options, args) = parser.parse_args()

		if not os.access(options.config, os.R_OK):
			print("Can't read {}: exitting".format(options.config))
			sys.exit(2)

		self._config = configparser.ConfigParser()
		self._config.readfp(open(options.config))

	def section(self, section):
		self._section = section

	def __getitem__(self, name):
		return self._config.get(self._section, name)

config = Config()

log = Logger()
db = tagDB()
checker = CheckerThread(db,log)

while True:
	line = sys.stdin.readline()
	if len(line) == 0:
		break
	checker.check(line)
