#!/usr/bin/env python3.1

import configparser, csv, optparse, os, postgresql.api, re, sys

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
	__slots__ = frozenset(['_prepared', '_db'])

	def __init__(self):
		self._prepared = set()
		config.section('database')
		self._db = postgresql.open(
			'pq://{}:{}@{}/{}'.format(
				config['user'],
				config['password'],
				config['host'],
				config['database'],
		) )

	def load(self, csv_data):
		with self._db.xact():
			config.section('loader')
			if config['drop_database']:
				self._db.execute('delete from urls;')
				if config['drop_site']:
					self._db.execute('delete from site;');
			insertreg = self._db.prepare("select set($1, $2, $3)")
			insert = self._db.prepare("select set($1, $2)")
			for row in csv_data:
				if len(row[2]) > 0:
					insertreg(row[0], row[1], row[2])
				else:
					insert(row[0], row[1])
		self._db.execute('vacuum analyze site;')
		self._db.execute('vacuum analyze urls;')

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
			'user': 'squidTag',
			'password': 'password',
			'host': 'localhost',
			'database': 'squidTag',
		},
		'loader': {
			'drop_database': False,
			'drop_site': False,
	},}

	# function to read in config file
	def __init__(self):
		parser = optparse.OptionParser()
		parser.add_option('-c', '--config', dest = 'config',
			help = 'config file location', metavar = 'FILE',
			default = '/usr/local/etc/squid-tagger.conf')
		parser.add_option('-d', '--drop-database', dest = 'drop_database',
			help = 'signals loader to drop previous database',
			action = 'store_true')
		parser.add_option('-D', '--drop-site', dest = 'drop_site',
			help = 'signals loader to drop not only url definitions but site index too',
			action = 'store_true')

		(options, args) = parser.parse_args()

		if options.drop_database:
			self._default['loader']['drop_database'] = True

		if options.drop_site:
			self._default['loader']['drop_site'] = True

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
		if not self._section in self._default or not name in self._default[self._section]:
			return None
		if not type(self._default[self._section][name]) == type(True):
			if not self._config.has_option(self._section, name):
				self._config.set(self._section, name, self._default[self._section][name])
			return(self._config.get(self._section, name))
		else:
			if not self._config.has_option(self._section, name):
				self._config.set(self._section, name, repr(self._default[self._section][name]))
			return(self._config.getboolean(self._section, name))

# initializing and reading in config file
config = Config()

tagdb = tagDB()

csv_reader = csv.reader(sys.stdin)
first_row = next(csv_reader)
if not first_row == ['site', 'tags', 'regexp']:
	print('File must contain csv data with three columns: "site", "tags" and "regexp".')
	sys.exit(1)
tagdb.load(csv_reader)
