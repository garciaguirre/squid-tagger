Here is a sample squid configuration:

url_rewrite_program /path/to/squid-tagger/squid-tagger.py
url_rewrite_children 1
url_rewrite_concurrency 1024

You need to create database with specific user for squid-tagger like this:

CREATE ROLE squidtag WITH login password 'password';
CREATE DATABASE squidtag WITH OWNER squidtag;

After that database should be populated with:

psql -f /path/to/squid-tagger/database.sql -U squidtag squidtag

You also should create config file needed by squid-tagger to access database,
by default squid-tagger searches this file in /usr/local/etc/ on behalf of
bsd-style config file locations. But this can be overridden with -c command
line switch.

Sample configuration file is also included. Note that you should set file
ownership to squid and rewoke any reading privileges from group and others.

squid-tagger logs all messages through the syslog facility. They can be
obtained and saved with:

[/etc/syslog.conf]
!squidTag
*.* /var/log/squidTag

[/etc/newsyslog.conf]
/var/log/squidTag 644 7 1024 * J

Remeber to create the file and restart syslog afterwise.
