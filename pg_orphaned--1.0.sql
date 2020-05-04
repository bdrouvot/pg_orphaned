CREATE FUNCTION pg_list_orphaned(
	IN dbname text default null,
	OUT dbname text,
	OUT path text,
	OUT name text,
	OUT size bigint,
	OUT mod_time timestamptz,
	OUT relfilenode bigint,
	OUT reloid bigint)
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME','pg_list_orphaned'
LANGUAGE C VOLATILE;
