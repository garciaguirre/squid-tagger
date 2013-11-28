#!/usr/bin/env python

from __future__ import division, print_function, unicode_literals

import gevent.monkey
gevent.monkey.patch_all()

import fcntl, gevent.core, gevent.pool, gevent.queue, gevent.socket, os, psycopg2, re, sys

# //inclusion start
# Copyright (C) 2010 Daniele Varrazzo <daniele.varrazzo@gmail.com>
# and licensed under the MIT license:

def gevent_wait_callback(conn, timeout=None):
	"""A wait callback useful to allow gevent to work with Psycopg."""
	while 1:
		state = conn.poll()
		if state == psycopg2.extensions.POLL_OK:
			break
		elif state == psycopg2.extensions.POLL_READ:
			gevent.socket.wait_read(conn.fileno(), timeout=timeout)
		elif state == psycopg2.extensions.POLL_WRITE:
			gevent.socket.wait_write(conn.fileno(), timeout=timeout)
		else:
			raise psycopg2.OperationalError("Bad result from poll: %r" % state)

if not hasattr(psycopg2.extensions, 'set_wait_callback'):
	raise ImportError("support for coroutines not available in this Psycopg version (%s)" % psycopg2.__version__)
	psycopg2.extensions.set_wait_callback(gevent_wait_callback)

# //inclusion end

# this classes processes config file and substitutes default values
class Config:
	__slots__ = frozenset(['_config', '_default', '_section', 'options'])
	_default = {
		'log': {
			'silent': 'no',
		},
		'database': {
			'database': 'squidTag',
	},}

	# function to read in config file
	def __init__(self):
		import ConfigParser, optparse, os

		parser = optparse.OptionParser()
		parser.add_option('-c', '--config', dest = 'config',
			help = 'config file location', metavar = 'FILE',
			default = '/usr/local/etc/squid-tagger.conf')
		parser.add_option('-d', '--dump', dest = 'dump',
			help = 'dump database', action = 'store_true', metavar = 'bool',
			default = False)
		parser.add_option('-f', '--flush-database', dest = 'flush_db',
			help = 'flush previous database on load', default = False,
			action = 'store_true', metavar = 'bool')
		parser.add_option('-l', '--load', dest = 'load',
			help = 'load database', action = 'store_true', metavar = 'bool',
			default = False)
		parser.add_option('-D', '--dump-conf', dest = 'dump_conf',
			help = 'dump filtering rules', default = False, metavar = 'bool',
			action = 'store_true')
		parser.add_option('-L', '--load-conf', dest = 'load_conf',
			help = 'load filtering rules', default = False, metavar = 'bool',
			action = 'store_true')

		(self.options, args) = parser.parse_args()

		assert os.access(self.options.config, os.R_OK), "Fatal error: can't read {}".format(self.options.config)

		self._config = ConfigParser.ConfigParser()
		self._config.readfp(open(self.options.config))

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

import logging, logging.handlers

# wrapper around logging handler to make it queue records and don't stall when sending them

class SysLogHandlerQueue(logging.handlers.SysLogHandler):
	__slots__ = frozenset(['_running', '_tail', '_worker'])

	def __init__(self):
		logging.handlers.SysLogHandler.__init__(self, '/dev/log')
		self._tail = gevent.queue.Queue()
		self._worker = None

	def emit(self, record):
		try:
			self._tail.put(record)
		except (KeyboardInterrupt, SystemExit):
			raise
		except:
			self.handleError(record)
		if self._worker == None:
			# in case queue is empty we will spawn new worker
			# all workers are logged so we can kill them on close()
			self._worker = gevent.spawn(self._writer)

	def _writer(self):
		# here we are locking the queue so we can be sure we are the only one
		while not self._tail.empty():
			logging.handlers.SysLogHandler.emit(self, self._tail.get())
		self._worker = None

	def close(self):
		if self._worker != None:
			gevent.kill(self._worker)
		logging.handlers.SysLogHandler.close(self)

logger = logging.getLogger('squidTag')
logger.setLevel(logging.INFO)
handler = SysLogHandlerQueue()
handler.setFormatter(logging.Formatter(str('squidTag[%(process)s]: %(message)s')))
logger.addHandler(handler)

# tiny wrapper around a file to make reads from it geventable
# or should i move this somewhere?

class FReadlineQueue(gevent.queue.Queue):
	# storing fileno descriptor, leftover
	__slots__ = frozenset(['_fn', '_tail'])

	def __init__(self, fd):
		# initialising class
		gevent.queue.Queue.__init__(self)
		self._fn = fd.fileno()
		# using empty tail
		self._tail = ''
		# putting file to nonblocking mode
		gevent.os.make_nonblocking(fd)
		# starting main loop
		gevent.spawn(self._frobber)

	def _frobber(self):
		while True:
			# reading one buffer from stream
			buf = gevent.os.nb_read(self._fn, 4096)
			# EOF found
			if len(buf) == 0:
				break
			# splitting stream by line ends
			rows = buf.decode('l1').split('\n')
			# adding tail to the first element if there is some tail
			if len(self._tail) > 0:
				rows[0] = self._tail + rows[0]
			# popping out last (incomplete) element
			self._tail = rows.pop(-1)
			# dropping all complete elements to the queue
			for row in rows:
				self.put_nowait(row)
				logger.info('< ' + row)
		# sending EOF
		self.put_nowait(None)

stdin = FReadlineQueue(sys.stdin)

# wrapper against file handler that makes possible to queue some writes without stalling

class FWritelineQueue(gevent.queue.JoinableQueue):
	# storing fileno, leftover
	__slots__ = frozenset(['_fn', '_tail'])

	def __init__(self, fd):
		# initialising class
		gevent.queue.JoinableQueue.__init__(self)
		# storing fileno
		self._fn = fd.fileno()
		# putting file to nonblocking mode
		gevent.os.make_nonblocking(fd)
		# using empty tail
		self._tail = None

	def __del__(self):
		# purge queue before deleting
		if not self.empty():
			self.join()

	def put(self, item, block=True, timeout=None):
		# calling real put
		gevent.queue.JoinableQueue.put(self, item, block, timeout)
		# starting main loop
		gevent.spawn(self._frobber)

	def _frobber(self):
		# checking leftover
		while True:
			if self._tail == None:
				try:
					self._tail = str(self.get_nowait()).encode('utf-8') + '\n'
				except gevent.queue.Empty:
					self._tail = None
					return
			# writing tail
			written = gevent.os.nb_write(self._fn, self._tail)
			length = len(self._tail)
			if written == length:
				self._tail = None
			elif written < length:
				self._tail = self._tail[written:]

# wrapper around database
class tagDB(object):
	__slots__ = frozenset(['_cursor', '_db'])

	def __init__(self):
		config.section('database')
		if config['host'] == None:
			self._db = psycopg2.connect(
				database = config['database'],
				user = config['user'],
				password = config['password']
			)
		else:
			self._db = psycopg2.connect(
				database = config['database'],
				host = config['host'],
				user = config['user'],
				password = config['password']
			)
		self._cursor = self._db.cursor()

	def _field_names(self):
		names = []
		for record in self._cursor.description:
			names.append(record.name)
		return(names)

	def check(self, site, ip_address):
		self._cursor.execute("select * from (select redirect_url, regexp from site_rule where site <@ tripdomain(%s) and netmask >>= %s order by array_length(site, 1) desc) a group by redirect_url, regexp", [site, ip_address])
		return(self._cursor.fetchall())

	def dump(self):
		self._cursor.execute("select untrip(site) as site, tag::text, regexp from urls order by site, tag")
		return(self._field_names(), self._cursor.fetchall())

	def load(self, data):
		if config.options.flush_db:
			self._cursor.execute('delete from urls;')
		bundle = []
		for row in data:
			if len(row) == 2:
				bundle.append([row[0], row[1], None])
			else:
				bundle.append([row[0], row[1], row[2]])
		self._cursor.executemany("insert into urls (site, tag, regexp) values (tripdomain(%s), %s, %s)", bundle)
		self._cursor.execute("update urls set regexp = NULL where regexp = ''")
		self._db.commit()

	def load_conf(self, csv_data):
		self._cursor.execute('delete from rules;')
		bundle = []
		for row in csv_data:
			bundle.append([row[0], row[1], int(row[2]), int(row[3]), row[4], row[5], row[6]])
		self._cursor.executemany("insert into rules (netmask, redirect_url, from_weekday, to_weekday, from_time, to_time, tag) values (%s::text::cidr, %s, %s, %s, %s::text::time, %s::text::time, %s::text::text[])", bundle)
		self._db.commit()

	def dump_conf(self):
		self._cursor.execute("select netmask, redirect_url, from_weekday, to_weekday, from_time, to_time, tag::text from rules")
		return(self._field_names(), self._cursor.fetchall())

# abstract class with basic checking functionality
class Checker(object):
	__slots__ = frozenset(['_db', '_log', '_queue', '_request', '_stdout'])

	def __init__(self, queue, logger):
		self._db = tagDB()
		self._log = logger
		self._log.info('started')
		self._request = re.compile('^([0-9]+)\ ((http|ftp):\/\/)?([-\w.]+)(:[0-9]+)?(\/([^ ]*))?\ ([0-9.:]+)\/(-|[\w\.]+)\ (-|\w+)\ (-|GET|HEAD|POST|CONNECT).*$')
		self._queue = queue
		self._stdout = FWritelineQueue(sys.stdout)

	def process(self, id, site, ip_address, url_path, line = None):
		#self._log.info('trying {}'.format(site))
		result = self._db.check(site, ip_address)
		reply = None
		#self._log.info('got {} lines from database'.format(len(result)))
		for row in result:
			if row != None and row[0] != None:
				if row[1] != None and url_path != None:
					self._log.info('trying regexp "{}" versus "{}"'.format(row[1], url_path))
					try:
						if re.compile(row[1]).match(url_path):
							reply = row[0].format(host = site, path = url_path)
						else:
							continue
					except:
						self._log.info("can't compile or execute regexp")
				else:
					reply = row[0].format(host = site, path = url_path)
			if reply != None:
				self.writeline('{} {}'.format(id, reply))
				return(True)
		self.writeline('{}'.format(id))

	def loop(self):
		while True:
			line = self._queue.get()
			if line == None:
				break
			#self._log.info('request: ' + line)
			request = self._request.match(line)
			if request:
				id = request.group(1)
				#proto = request.group(3)
				site = request.group(4)
				url_path = request.group(7)
				ip_address = request.group(8)
				self.process(id, site, ip_address, url_path, line)
			else:
				self._log.info('bad request')
				self.writeline(line)

	def writeline(self, string):
		self._log.info('> ' + string)
		self._stdout.put(string)

if config.options.dump or config.options.load or config.options.dump_conf or config.options.load_conf:
	import csv

	tagdb = tagDB()
	data_fields = ['site', 'tag', 'regexp']
	conf_fields = ['netmask', 'redirect_url', 'from_weekday', 'to_weekday', 'from_time', 'to_time', 'tag']

	if config.options.dump or config.options.dump_conf:
		csv_writer = csv.writer(sys.stdout)
		if config.options.dump:
			dump = tagdb.dump()
		elif config.options.dump_conf:
			dump = tagdb.dump_conf()

		csv_writer.writerow(dump[0])
		for line in dump[1]:
			csv_writer.writerow(line)

	elif config.options.load or config.options.load_conf:
		csv_reader = csv.reader(sys.stdin)
		first_row = next(csv_reader)

		if config.options.load:
			fields = data_fields
			load = tagdb.load
		elif config.options.load_conf:
			fields = conf_fields
			load = tagdb.load_conf

		assert first_row == fields, 'File must contain csv data with theese columns: ' + repr(fields)
		load(csv_reader)

else:
	# main loop
	Checker(stdin, logger).loop()
