#!/usr/bin/env python3.1

import configparser, csv, optparse, os, postgresql.api, sys

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
	__slots__ = frozenset(['_prepared', '_dump_stmt', '_db'])

	def __init__(self):
		self._prepared = set()
		self._db = False
		self._dump_stmt = self._curs().prepare("select untrip(site), tag, regexp from urls natural join site natural join tag order by site, tag")

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

	def dump(self):
		return(self._dump_stmt())

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

tagdb = tagDB()

csv_writer = csv.writer(sys.stdout)
csv_writer.writerow(['site', 'tags', 'regexp'])
for row in tagdb.dump():
	csv_writer.writerow([row[0], '{' + ','.join(row[1]) + '}', row[2]])
