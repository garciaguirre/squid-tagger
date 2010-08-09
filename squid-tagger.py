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
	__slots__ = frozenset(('_check_stmt', '_db'))

	def __init__(self):
		config.section('database')
		self._db = postgresql.open(
			'pq://{}:{}@{}/{}'.format(
				config['user'],
				config['password'],
				config['host'],
				config['database'],
		) )
		self._check_stmt = self._db.prepare("select redirect_url, regexp from site_rule where site <@ tripdomain($1) and netmask >> $2::text::inet order by array_length(site, 1) desc")

	def check(self, site, ip_address):
		return(self._check_stmt(site, ip_address))

# abstract class with basic checking functionality
class Checker:
	__slots__ = frozenset(['_db', '_log'])

	def __init__(self):
		self._db = tagDB()
		self._log = Logger()
		self._log.info('started\n')

	def process(self, id, site, ip_address, url_path, line = None):
		self._log.info('trying {}\n'.format(site))
		result = self._db.check(site, ip_address)
		#reply = '{}://{}/{}'.format(req[4], req[1], req[3])
		reply = '-'
		for row in result:
			if row != None and row[0] != None:
				if row[1] != None:
					self._log.info('trying regexp "{}" versus "{}"\n'.format(row[1], url_path))
					try:
						if re.compile(row[1]).match(url_path):
							reply = row[0].format(url_path)
							break
						else:
							continue
					except:
						self._log.info("can't compile regexp")
				else:
					reply = row[0].format(url_path)
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
			self.process(id, site, ip_address, url_path, line)
			return(True)
		else:
			self._log.info('bad request\n')
			self.writeline(line)
			return(False)

	def writeline(self, string):
		self._log.info('sending: ' + string)
		sys.stdout.write(string)
		sys.stdout.flush()

	def loop(self):
		while True:
			line = sys.stdin.readline()
			if len(line) == 0:
				break
			self.check(line)

# threaded checking facility
class CheckerThread(Checker):
	__slots__ = frozenset(['_lock', '_lock_exit', '_lock_queue', '_queue'])

	def __init__(self):
		# basic initialisation
		Checker.__init__(self)

		# Spin lock. Loop acquires it on start then releases it when holding queue
		# lock. This way the thread proceeds without stops while queue has data and
		# gets stalled when no data present. The lock is released by queue writer
		# after storing something into the queue
		self._lock = _thread.allocate_lock()
		self._lock_exit = _thread.allocate_lock()
		self._lock_queue = _thread.allocate_lock()
		self._lock.acquire()
		self._queue = []
		_thread.start_new_thread(self._start, ())

	def _start(self):
		while True:
			self._lock.acquire()
			with self._lock_queue:
				# yes this should be written this way, and yes, this is why I hate threading
				if len(self._queue) > 1:
					if self._lock.locked():
						self._lock.release()
				req = self._queue.pop(0)
			Checker.process(self, req[0], req[1], req[2], req[3])
			with self._lock_queue:
				if len(self._queue) == 0:
					if self._lock_exit.locked():
						self._lock_exit.release()

	def process(self, id, site, ip_address, url_path, line):
		with self._lock_queue:
			self._queue.append((id, site, ip_address, url_path))
			self._log.info('request {} queued ({})\n'.format(id, line))
			if not self._lock_exit.locked():
				self._lock_exit.acquire()
			if self._lock.locked():
				self._lock.release()

	def loop(self):
		while True:
			line = sys.stdin.readline()
			if len(line) == 0:
				break
			self.check(line)
		self._lock_exit.acquire()

# kqueue enabled class for BSD's
class CheckerKqueue(Checker):
	__slots__ = frozenset(['_kq', '_select', '_queue'])

	def __init__(self):
		# basic initialisation
		Checker.__init__(self)

		# importing select module
		import select
		self._select = select

		# kreating kqueue
		self._kq = self._select.kqueue()
		assert self._kq.fileno() != -1, "Fatal error: can't initialise kqueue."

		# watching sys.stdin for data
		self._kq.control([self._select.kevent(sys.stdin, self._select.KQ_FILTER_READ, self._select.KQ_EV_ADD)], 0)

		# creating data queue
		self._queue = []

	def loop(self):
		# Wait for data by default
		timeout = None
		eof = False
		buffer = ''
		while True:
			# checking if there is any data or witing for data to arrive
			kevs = self._kq.control(None, 1, timeout)

			for kev in kevs:
				if kev.filter == self._select.KQ_FILTER_READ and kev.data > 0:
					# reading data in
					new_buffer = sys.stdin.read(kev.data)
					# if no data was sent - we have reached end of file
					if len(new_buffer) == 0:
						eof = True
					else:
						# adding current buffer to old buffer remains
						buffer += new_buffer
						# splitting to lines
						lines = buffer.split('\n')
						# last line that was not terminate by newline returns to buffer
						buffer = lines[-1]
						# an only if there was at least one newline
						if len(lines) > 1:
							for line in lines[:-1]:
								# add data to the queue
								if self.check(line + '\n'):
									# don't wait for more data, start processing
									timeout = 0

				# detect end of stream and exit if possible
				if kev.flags >> 15 == 1:
					self._kq.control([self._select.kevent(sys.stdin, self._select.KQ_FILTER_READ, self._select.KQ_EV_DELETE)], 0)
					eof = True

			if len(kevs) == 0:
				if len(self._queue) > 0:
					# get one request and process it
					req = self._queue.pop(0)
					Checker.process(self, req[0], req[1], req[2], req[3])
					if len(self._queue) == 0:
						# wait for data - we have nothing to process
						timeout = None

			# if queue is empty and we reached end of stream - we can exit
			if len(self._queue) == 0 and eof:
				break

	def process(self, id, site, ip_address, url_path, line):
		# simply adding data to the queue
		self._queue.append((id, site, ip_address, url_path))
		self._log.info('request {} queued ({})\n'.format(id, line))

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

		assert os.access(options.config, os.R_OK), "Fatal error: can't read {}".format(options.config)

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
elif config['reactor'] == 'plain':
	checker = Checker()
elif config['reactor'] == 'kqueue':
	checker = CheckerKqueue()

checker.loop()
