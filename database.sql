-- general array sorting and domain processing functions
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

-- general database structure
-- table to hold all rules
CREATE TABLE rules (
	netmask cidr NOT NULL,
	redirect_url text DEFAULT 'about::blank'::text NOT NULL,
	from_weekday smallint DEFAULT 0 NOT NULL,
	to_weekday smallint DEFAULT 6 NOT NULL,
	from_time time without time zone DEFAULT '00:00:00'::time without time zone NOT NULL,
	to_time time without time zone DEFAULT '23:59:59'::time without time zone NOT NULL,
	tag text[] NOT NULL
);

-- table to hold tag - site links
CREATE TABLE urls (
	date_added timestamp without time zone DEFAULT ('now'::text)::timestamp(0) without time zone NOT NULL,
	site text[] NOT NULL,
	tag text[] NOT NULL,
	regexp text
);

create unique index urls_rst on urls (regexp, usort(site), usort(tag));

-- rule to join all tables into one to simplify access
-- automaticall uses current day and time data
create view site_rule as
select redirect_url, netmask, site, regexp
from rules join urls
on (urls.tag && rules.tag)
where ('now'::text)::time without time zone >= from_time
	and ('now'::text)::time without time zone <= to_time
	and date_part('dow'::text, now()) >= (from_weekday)::double precision
	and date_part('dow'::text, now()) <= (to_weekday)::double precision;

CREATE PROCEDURAL LANGUAGE plpgsql;

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
