CREATE PROCEDURAL LANGUAGE plpgsql;

-- general array sorting functions
-- sorts array
CREATE or replace FUNCTION sort(original anyarray) RETURNS anyarray
	LANGUAGE sql IMMUTABLE STRICT
	AS $_$
select array_agg(item) as result from (select unnest($1) as item order by item) a;
$_$;

-- sorts array and removes duplicates
CREATE or replace FUNCTION usort(original anyarray) RETURNS anyarray
	LANGUAGE sql IMMUTABLE STRICT
	AS $_$
select array_agg(item) as result from (select distinct unnest($1) as item order by item) a;
$_$;

-- transforms domain into ordered array for indexing
CREATE or replace FUNCTION tripdomain(url text) RETURNS text[]
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
create or replace function untrip(site text[]) returns text
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

-- this functions returns id of site
create or replace function get_site(my_site text[]) returns integer
	language plpgsql strict
	as $$
declare
	site_id integer;
begin
	select id_site from site where my_site = site into site_id;
	if not found then
		insert into site (site) values (my_site);
		select id_site from site where my_site = site into site_id;
	end if;
	return site_id;
end;
$$;

create or replace function get_site(domain text) returns integer
	language sql immutable strict
	as $$
select get_site(tripdomain($1)) as result;
$$;

-- this function inserts or updates record with tags to site by site id with regexp
CREATE or replace FUNCTION mark(my_id_site integer, my_id_tag integer, my_regexp text) RETURNS integer
	LANGUAGE plpgsql STRICT
	AS $$
declare
	-- maybe check should be added to make sure supplied site id really exists
	my_tag text[];
begin
	-- selecting tags site already have and adding new tag to them
	-- note that tags should be sorted to eliminate permutations
	select coalesce(tag, '{}'::text[]) from urls natural left join tag
		where id_site = my_id_site and regexp = my_regexp into my_tag;
	if not found then
		-- no records found - creating new tag
		insert into urls (id_site, id_tag, regexp) values (my_id_site, my_id_tag, my_regexp);
	else
		-- joining tags
		select usort(my_tag || tag) from tag where id_tag = my_id_tag into my_tag;
		-- updating existing record
		update urls set id_tag = get_tag(my_tag)
			where id_site = my_id_site and regexp = my_regexp;
	end if;
	return my_id_site;
end;
$$;

-- this function adds tag to site by site id
CREATE or replace FUNCTION mark(my_id_site integer, new_tag text) RETURNS integer
	LANGUAGE plpgsql STRICT
	AS $$
declare
	-- maybe check should be added to make sure supplied site id really exists
	my_tag text[];
begin
	-- selecting tags site already have and adding new tag to them
	-- note that tags should be sorted to eliminate permutations
	select coalesce(tag, '{}'::text[]) from urls natural left join tag
		where id_site = my_id_site and regexp is null into my_tag;
	if not found then
		-- no records found - creating new tag
		insert into urls (id_site, id_tag) values (my_id_site, get_tag(array[new_tag]));
	else
		-- joining tags
		select usort(my_tag || array[new_tag]) into my_tag;
		-- updating existing record
		update urls set id_tag = get_tag(my_tag) where id_site = my_id_site and regexp is null;
	end if;
	return my_id_site;
end;
$$;

-- this function adds tag to domain
CREATE or replace FUNCTION mark(domain text, new_tag text) RETURNS integer
	LANGUAGE sql immutable STRICT
	AS $$
select mark(get_site($1), $2) as result;
$$;

-- this function sets tags for site without regexp
CREATE or replace FUNCTION set(my_id_site integer, my_id_tag integer) RETURNS integer
	LANGUAGE sql STRICT
	AS $$
delete from urls where $1 = id_site and regexp is NULL;
insert into urls (id_site, id_tag) values ($1, $2);
select $1;
$$;

-- this function sets tags for site/regexp pair
CREATE or replace FUNCTION set(my_id_site integer, my_id_tag integer, my_regexp text) RETURNS integer
	LANGUAGE sql STRICT
	AS $$
delete from urls where $1 = id_site and $3 = regexp;
insert into urls (id_site, id_tag, regexp) values ($1, $2, $3);
select $1;
$$;

-- this function stores new data for site/regexp pair
create or replace function set(domain text, tags text, regexp text) returns integer
	language sql immutable strict
	as $$
select set(get_site($1), get_tag($2::text[]), $3);
$$;

-- this function stores new data for site/regexp pair
create or replace function set(domain text, tags text) returns integer
	language sql immutable strict
	as $$
select set(get_site($1), get_tag($2::text[]));
$$;

-- this function returns id of tag array
create or replace function get_tag(my_tag text[]) returns integer
	language plpgsql strict
	as $$
declare
	tag_id integer;
begin
	select id_tag from tag where usort(my_tag) = tag into tag_id;
	if not found then
		insert into tag (tag) values (usort(my_tag));
		select id_tag from tag where usort(my_tag) = tag into tag_id;
	end if;
	return tag_id;
end;
$$;

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
	ADD CONSTRAINT rules_tag_f FOREIGN KEY (id_tag) REFERENCES tag(id_tag) MATCH FULL
	ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

-- table to hold site arrays
CREATE TABLE site (
	id_site serial,
	site text[] NOT NULL
);

ALTER TABLE ONLY site
	ADD CONSTRAINT site_pkey PRIMARY KEY (id_site);

CREATE UNIQUE INDEX site_u ON site (usort(site));

CREATE INDEX site_g ON site USING gin (site);

-- table to hold tag combinations
CREATE TABLE tag (
	id_tag serial,
	tag text[] NOT NULL
);

ALTER TABLE ONLY tag
	ADD CONSTRAINT tag_pkey PRIMARY KEY (id_tag);

CREATE UNIQUE INDEX tag_u ON tag (usort(tag));

CREATE INDEX tag_g ON tag USING gin (tag);

-- table to hold tag - site links
CREATE TABLE urls (
	date_added timestamp without time zone DEFAULT ('now'::text)::timestamp(0) without time zone NOT NULL,
	id_site smallint NOT NULL,
	id_tag smallint NOT NULL,
	regexp text
);

CREATE UNIQUE INDEX urls_pkey ON urls USING btree (id_site, regexp);

CREATE INDEX urls_id_tag ON urls USING btree (id_tag);

ALTER TABLE ONLY urls
	ADD CONSTRAINT urls_site_f FOREIGN KEY (id_site) REFERENCES site(id_site) MATCH FULL
	ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY urls
	ADD CONSTRAINT urls_tag_f FOREIGN KEY (id_tag) REFERENCES tag(id_tag) MATCH FULL
	ON UPDATE RESTRICT ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

-- rule to join all tables into one to simplify access
-- automaticall uses current day and time data
CREATE VIEW site_rule AS
SELECT a.redirect_url, a.netmask, b.site, b.regexp
FROM ((
	SELECT rules.redirect_url, tag.tag AS rule_tag, rules.netmask
	FROM rules NATURAL JOIN tag
	WHERE ('now'::text)::time without time zone >= rules.from_time
		AND ('now'::text)::time without time zone <= rules.to_time
		AND date_part('dow'::text, now()) >= (rules.from_weekday)::double precision
		AND date_part('dow'::text, now()) <= (rules.to_weekday)::double precision
) a JOIN (
	SELECT site.site, tag.tag AS url_tag, regexp
	FROM urls NATURAL JOIN tag NATURAL JOIN site
) b ON (b.url_tag && a.rule_tag));
