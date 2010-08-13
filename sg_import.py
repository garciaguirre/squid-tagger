#!/usr/bin/env python3.1

# This script converts SquidGuard database into format that can be imported to
# squid-tagger. It should be run in SquidGuard database directory and it would
# produce csv stream that can be redirected to squid-tagger for imports:

# cd /var/db/squidGuard ; path/to/sg_import.py | path/to/squid-tagger.py -l -f

# This one will flush squid-tagger's database and load selected SquidGuard
# database.

import codecs, csv, os, re, sys

data = {}

for (path, names, files) in os.walk('.'):
	tag = path.lstrip('./')
	for file in files:
		if file in ('domains', 'expressions', 'urls'):
			with codecs.open(path + os.sep + file, 'r', 'L1') as source:
				for full_line in source:
					line = full_line.strip()
					if not re.compile('^(#|$)').match(line):
						regexp = None
						if file == 'expressions':
							regexp = line
							line = None
						if file == 'urls':
							(line, sep, regexp) = line.partition('/')
							regexp = '^' + re.escape(regexp)
						if line in data:
							if regexp in data[line]:
								data[line][regexp].add(tag)
							else:
								data[line][regexp] = set([tag])
						else:
							data[line] = {regexp: set([tag])}

cw = csv.writer(sys.stdout)
cw.writerow(['site', 'tags', 'regexp'])

for domain in data:
	for regexp in data[domain]:
		cw.writerow([domain, '{' + ','.join(data[domain][regexp]) + '}', regexp])
