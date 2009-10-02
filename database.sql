CREATE PROCEDURAL LANGUAGE plpgsql;

-- this function adds tag to domain
CREATE FUNCTION mark(domain text, new_tag text) RETURNS void
	LANGUAGE plpgsql STRICT
	AS $$
declare
	my_site text[];
	my_site_id smallint;
	my_tag text[];
	my_tag_id smallint;
begin
	my_site := tripdomain(domain);

	-- selecting site id from table or adding site to the table
	select id_site from site where my_site = site into my_site_id;
	if not found then
		insert into site (site) values (my_site);
		select id_site from site where my_site = site into my_site_id;
	end if;

	-- selecting tags site already have and adding new tag to them
	-- note that tags should be sorted to eliminate permutations
	select tag from urls natural join tag where id_site = my_site_id into my_tag;
	if not found then
		-- no records found - creating new tag
		my_tag := array[new_tag];
	else
		-- joining tags
		select array_agg(tag)
			from (select distinct unnest(my_tag || array[new_tag]) as tag order by tag asc) a
			into my_tag;
		-- deleting old site specification
		delete from urls where id_site = my_site_id;
	end if;

	-- selecting new tag id or adding tag to the table
	select id_tag from tag where my_tag = tag into my_tag_id;
	if not found then
		insert into tag (tag) values(my_tag);
		select id_tag from tag where my_tag = tag into my_tag_id;
	end if;

	-- adding new site specification
	insert into urls (id_site, id_tag) values (my_site_id, my_tag_id);
end;$$;

-- this function adds tag to site by site id
CREATE FUNCTION mark(my_site_id smallint, new_tag text) RETURNS void
	LANGUAGE plpgsql STRICT
	AS $$
declare
	-- maybe check should be added to make sure supplied site id really exists
	my_tag text[];
	my_tag_id smallint;
begin
	-- selecting tags site already have and adding new tag to them
	-- note that tags should be sorted to eliminate permutations
	select tag from urls natural join tag where id_site = my_site_id into my_tag;
	if not found then
		-- no records found - creating new tag
		my_tag := array[new_tag];
	else
		-- joining tags
		select array_agg(tag)
			from (select distinct unnest(my_tag || array[new_tag]) as tag order by tag asc) a
			into my_tag;
		-- deleting old site specification
		delete from urls where id_site = my_site_id;
	end if;

	-- selecting new tag id or adding tag to the table
	select id_tag from tag where my_tag = tag into my_tag_id;
	if not found then
		insert into tag (tag) values(my_tag);
		select id_tag from tag where my_tag = tag into my_tag_id;
	end if;

	-- adding new site specification
	insert into urls (id_site, id_tag) values (my_site_id, my_tag_id);
end;$$;

-- transforms domain into ordered array for indexing
CREATE FUNCTION tripdomain(url text) RETURNS text[]
	LANGUAGE plpgsql IMMUTABLE STRICT
	AS $_$
declare
	result text[];
	splitted text[];
	x integer;
	length integer;
begin
	splitted := string_to_array($1, '.');
	length := array_length(splitted, 1);
	x := 1;
	loop
		exit when splitted[x] is null;
		result[x] := splitted[x] || ':' || length - x;
		x := x + 1;
	end loop;
	return result;
end;$_$;

-- transforms ordered array into domain
create function untrip(site text[]) returns text
	language plpgsql immutable strict
	as $_$
declare
	x integer;
	splitted text[];
	pair text[];
begin
	x := array_length(site, 1);
	loop 
		exit when site[x] is null;
		pair := string_to_array(site[x], ':');
		splitted[0 - pair[2]::integer] := pair[1];
		x := x - 1;
	end loop;
	return array_to_string(splitted, '.');
end;
$_$;

-- table to hold all rules
CREATE TABLE rules (
	netmask cidr NOT NULL,
	redirect_url text DEFAULT 'about::blank'::text NOT NULL,
	from_weekday smallint DEFAULT 0 NOT NULL,
	to_weekday smallint DEFAULT 6 NOT NULL,
	from_time time without time zone DEFAULT '00:00:00'::time without time zone NOT NULL,
	to_time time without time zone DEFAULT '23:59:59'::time without time zone NOT NULL,
	id_tag smallint NOT NULL
);

ALTER TABLE ONLY rules
	ADD CONSTRAINT rules_pkey PRIMARY KEY (netmask);

-- table to hold site arrays
-- maybe full original domain should be added with trigger to leave tripdomain function away
CREATE TABLE site (
	id_site serial,
	site text[] NOT NULL,
	domain not null
);

ALTER TABLE ONLY site
	ADD CONSTRAINT site_id PRIMARY KEY (id_site);

CREATE UNIQUE INDEX site_s ON site USING btree (site);

CREATE INDEX site_sg ON site USING gin (site);

-- table to hold tag combinations
CREATE TABLE tag (
	id_tag serial,
	tag text[] NOT NULL
);

ALTER TABLE ONLY tag
	ADD CONSTRAINT tag_id PRIMARY KEY (id_tag);

CREATE INDEX tag_g ON tag USING gin (tag);

CREATE UNIQUE INDEX tag_s ON tag USING btree (tag);

-- table to hold tag - site links
CREATE TABLE urls (
	date_added timestamp without time zone DEFAULT ('now'::text)::timestamp(0) without time zone NOT NULL,
	id_site smallint NOT NULL,
	id_tag smallint NOT NULL
);

ALTER TABLE ONLY urls
	ADD CONSTRAINT urls_pkey PRIMARY KEY (date_added);

CREATE UNIQUE INDEX urls_id_site ON urls USING btree (id_site);

-- rule to join all tables into one to simplify access
-- automaticall uses current day and time data
CREATE VIEW site_rule AS
SELECT a.redirect_url, a.netmask, b.site
FROM ((
	SELECT rules.redirect_url, tag.tag AS rule_tag, rules.netmask
	FROM (rules NATURAL JOIN tag)
	WHERE ((((('now'::text)::time without time zone >= rules.from_time)
		AND (('now'::text)::time without time zone <= rules.to_time))
		AND (date_part('dow'::text, now()) >= (rules.from_weekday)::double precision))
		AND (date_part('dow'::text, now()) <= (rules.to_weekday)::double precision))
) a JOIN (
	SELECT site.site, tag.tag AS url_tag
	FROM ((urls NATURAL JOIN tag) NATURAL JOIN site)
) b ON ((b.url_tag && a.rule_tag)));
