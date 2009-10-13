#!/usr/bin/env python3.1

import configparser, optparse, os, postgresql.api, re, sys, _thread

# wrapper around syslog, can be muted
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

# wrapper around database
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
				'pq://{}:{}@{}/{}'.format(
					config['user'],
					config['password'],
					config['host'],
					config['database'],
			) )
		return(self._db)

	def check(self, site, ip_address):
		return(self._check_stmt(site, ip_address))

# abstract class with basic checking functionality
class Checker:
	__slots__ = frozenset(['_db', '_log', '_queue'])

	def __init__(self):
		self._db = tagDB()
		self._log = Logger()

	def process(self, id, site, ip_address, url_path):
		self._log.info('trying {}\n'.format(site))
		result = self._db.check(site, ip_address)
		#reply = '{}://{}/{}'.format(req[4], req[1], req[3])
		reply = '-'
		for row in result:
			if row != None and row[0] != None:
				if row[1] != None:
					self._log.info('trying regexp "{}" versus "{}"\n'.format(row[1], url_path))
					if re.compile(row[1]).match(url_path):
						reply = '302:' + row[0]
						break
					else:
						continue
				else:
					reply = '302:' + row[0]
					break
		self.writeline('{} {}\n'.format(id, reply))

	def check(self, line):
		request = re.compile('^([0-9]+)\ (http|ftp):\/\/([-\w.:]+)\/([^ ]*)\ ([0-9.]+)\/(-|[\w\.]+)\ (-|\w+)\ (-|GET|HEAD|POST).*$').match(line)
		if request:
			id = request.group(1)
			#proto = request.group(2)
			site = request.group(3)
			url_path = request.group(4)
			ip_address = request.group(5)
			self.insert(id, site, ip_address, url_path)

			self._log.info('request {} queued ({})\n'.format(id, line))
		else:
			self._log.info('bad request\n')
			self.writeline(line)

	def insert(self, id, site, ip_address, url_path):
		self._queue.append((id, site, ip_address, url_path))

	def writeline(self, string):
		self._log.info('sending: ' + string)
		sys.stdout.write(string)
		sys.stdout.flush()

# threaded checking facility
class CheckerThread(Checker):
	__slots__ = frozenset(['_lock', '_lock_queue'])

	def __init__(self):
		Checker.__init__(self)
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
			self.process(req[0], req[1], req[2], req[3])

	def insert(self, id, site, ip_address, url_path):
		self._lock_queue.acquire()
		Checker.insert(self, id, site, ip_address, url_path)
		if self._lock.locked():
			self._lock.release()
		self._lock_queue.release()

# this classes processes config file and substitutes default values
class Config:
	__slots__ = frozenset(['_config', '_default', '_section'])
	_default = {
		'reactor': {
			'reactor': 'thread',
		},
		'log': {
			'silent': 'no',
		},
		'database': {
			'host': 'localhost',
			'database': 'squidTag',
	},}

	# function to read in config file
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

	# function to select config file section or create one
	def section(self, section):
		if not self._config.has_section(section):
			self._config.add_section(section)
		self._section = section

	# function to get config parameter, if parameter doesn't exists the default
	# value or None is substituted
	def __getitem__(self, name):
		if not self._config.has_option(self._section, name):
			if self._section in self._default:
				if name in self._default[self._section]:
					self._config.set(self._section, name, self._default[self._section][name])
				else:
					self._config.set(self._section, name, None)
			else:
				self._config.set(self._section, name, None)
		return(self._config.get(self._section, name))

# initializing and reading in config file
config = Config()

config.section('reactor')
if config['reactor'] == 'thread':
	checker = CheckerThread()

while True:
	line = sys.stdin.readline()
	if len(line) == 0:
		break
	checker.check(line)
