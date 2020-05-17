/*-------------------------------------------------------------------------
 *
 * pg_orphaned.c
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "access/twophase.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include <sys/stat.h>
#include "utils/relfilenodemap.h"
#include "commands/dbcommands.h"
#include "mb/pg_wchar.h"
#include "regex/regex.h"
#if PG_VERSION_NUM < 110000
#include "catalog/pg_collation.h"
#include "catalog/catalog.h"
#include "utils/timestamp.h"
#include "utils/memutils.h"
#include "catalog/pg_tablespace.h"
#else
#include "catalog/pg_collation_d.h"
#include "catalog/pg_tablespace_d.h"
#include "utils/rel.h"
#endif
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/relmapper.h"
#include "catalog/indexing.h"
#include "utils/inval.h"
#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#include "access/genam.h"
#include "utils/snapmgr.h"
#else
#include "utils/tqual.h"
#include "access/htup_details.h"
#endif

PG_MODULE_MAGIC;
Datum pg_list_orphaned(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_list_orphaned);

PG_FUNCTION_INFO_V1(pg_remove_orphaned);
Datum pg_remove_orphaned(PG_FUNCTION_ARGS);

List   *list_orphaned_relations=NULL;
static void pg_list_orphaned_internal(FunctionCallInfo fcinfo);
static void search_orphaned(List **flist, Oid dboid, const char *dbname, const char *dir, Oid reltablespace);
static void pg_build_orphaned_list(Oid dbOid);
static Oid RelidByRelfilenodeDirty(Oid reltablespace, Oid relfilenode);

/* Hash table for information about each relfilenode <-> oid pair */
static HTAB *RelfilenodeMapHashDirty = NULL;

/* built first time through in InitializeRelfilenodeMap */
static ScanKeyData relfilenode_skey_dirty[2];

typedef struct
{
	Oid                     reltablespace;
	Oid                     relfilenode;
} RelfilenodeMapKey;

typedef struct
{
	RelfilenodeMapKey key;          /* lookup key - must be first */
	Oid                     relid;                  /* pg_class.oid */
} RelfilenodeMapEntry;

typedef struct OrphanedRelation {
	char	   *dbname;
	char	   *path;
	char	   *name;
	int size;
	TimestampTz mod_time;
	Oid relfilenode;
	Oid reloid;
} OrphanedRelation;

/*
 *  Flush mapping entries when pg_class is updated in a relevant fashion.
 *  Same as RelfilenodeMapInvalidateCallback in relfilenodemap.c
 */
static void
RelfilenodeMapInvalidateCallbackDirty(Datum arg, Oid relid)
{
	HASH_SEQ_STATUS status;
	RelfilenodeMapEntry *entry;

	/* callback only gets registered after creating the hash */
	Assert(RelfilenodeMapHashDirty != NULL);

	hash_seq_init(&status, RelfilenodeMapHashDirty);
	while ((entry = (RelfilenodeMapEntry *) hash_seq_search(&status)) != NULL)
	{
		/*
		 * If relid is InvalidOid, signalling a complete reset, we must remove
		 * all entries, otherwise just remove the specific relation's entry.
		 * Always remove negative cache entries.
		 */
		if (relid == InvalidOid ||      /* complete reset */
			entry->relid == InvalidOid ||   /* negative cache entry */
			entry->relid == relid)  /* individual flushed relation */
		{
			if (hash_search(RelfilenodeMapHashDirty,
							(void *) &entry->key,
							HASH_REMOVE,
							NULL) == NULL)
						elog(ERROR, "hash table corrupted");
		}
	}
}

/*
 * Initialize cache, either on first use or after a reset.
 * Same as InitializeRelfilenodeMap in relfilenodemap.c
 */
static void
InitializeRelfilenodeMapDirty(void)
{
	HASHCTL         ctl;
	int                     i;

	/* Make sure we've initialized CacheMemoryContext. */
	if (CacheMemoryContext == NULL)
		CreateCacheMemoryContext();

	/* build skey */
	MemSet(&relfilenode_skey_dirty, 0, sizeof(relfilenode_skey_dirty));

	for (i = 0; i < 2; i++)
	{
		fmgr_info_cxt(F_OIDEQ,
					&relfilenode_skey_dirty[i].sk_func,
					CacheMemoryContext);
		relfilenode_skey_dirty[i].sk_strategy = BTEqualStrategyNumber;
		relfilenode_skey_dirty[i].sk_subtype = InvalidOid;
		relfilenode_skey_dirty[i].sk_collation = InvalidOid;
	}

	relfilenode_skey_dirty[0].sk_attno = Anum_pg_class_reltablespace;
	relfilenode_skey_dirty[1].sk_attno = Anum_pg_class_relfilenode;

	/* Initialize the hash table. */
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(RelfilenodeMapKey);
	ctl.entrysize = sizeof(RelfilenodeMapEntry);
	ctl.hcxt = CacheMemoryContext;

	/*
	 * Only create the RelfilenodeMapHashDirty now, so we don't end up partially
	 * initialized when fmgr_info_cxt() above ERRORs out with an out of memory
	 * error.
	 */
	RelfilenodeMapHashDirty =
		hash_create("RelfilenodeMap cache", 64, &ctl,
			HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Watch for invalidation events. */
	CacheRegisterRelcacheCallback(RelfilenodeMapInvalidateCallbackDirty,
									(Datum) 0);
}


/*
 * Map a relation's (tablespace, filenode) to a relation's oid and cache the
 * result.
 *
 * This is the same as the existing RelidByRelfilenode in relfilenodemap.c but
 * it is done by using a DirtySnapshot as we want to see relation being created
 *
 * Returns InvalidOid if no relation matching the criteria could be found.
 */
Oid
RelidByRelfilenodeDirty(Oid reltablespace, Oid relfilenode)
{
	RelfilenodeMapKey key;
	RelfilenodeMapEntry *entry;
	bool            found;
	SysScanDesc scandesc;
	Relation        relation;
	HeapTuple       ntp;
	ScanKeyData skey[2];
	Oid                     relid;
	SnapshotData DirtySnapshot;

	InitDirtySnapshot(DirtySnapshot);
	if (RelfilenodeMapHashDirty == NULL)
		InitializeRelfilenodeMapDirty();

	/* pg_class will show 0 when the value is actually MyDatabaseTableSpace */
	if (reltablespace == MyDatabaseTableSpace)
		reltablespace = 0;

	MemSet(&key, 0, sizeof(key));
	key.reltablespace = reltablespace;
	key.relfilenode = relfilenode;

	/*
	 * Check cache and return entry if one is found. Even if no target
	 * relation can be found later on we store the negative match and return a
	 * InvalidOid from cache. That's not really necessary for performance
	 * since querying invalid values isn't supposed to be a frequent thing,
	 * but it's basically free.
	 */
	entry = hash_search(RelfilenodeMapHashDirty, (void *) &key, HASH_FIND, &found);

	if (found)
		return entry->relid;

	/* ok, no previous cache entry, do it the hard way */

	/* initialize empty/negative cache entry before doing the actual lookups */
	relid = InvalidOid;

	if (reltablespace == GLOBALTABLESPACE_OID)
	{
		/*
		 * Ok, shared table, check relmapper.
		 */
		relid = RelationMapFilenodeToOid(relfilenode, true);
	}
	else
	{
		/*
		 * Not a shared table, could either be a plain relation or a
		 * non-shared, nailed one, like e.g. pg_class.
		 */
		/* check for plain relations by looking in pg_class */
#if PG_VERSION_NUM >= 120000
		relation = table_open(RelationRelationId, AccessShareLock);
#else
		relation = heap_open(RelationRelationId, AccessShareLock);
#endif
		/* copy scankey to local copy, it will be modified during the scan */
		memcpy(skey, relfilenode_skey_dirty, sizeof(skey));

		/* set scan arguments */
		skey[0].sk_argument = ObjectIdGetDatum(reltablespace);
		skey[1].sk_argument = ObjectIdGetDatum(relfilenode);

		scandesc = systable_beginscan(relation,
							ClassTblspcRelfilenodeIndexId,
							true,
							&DirtySnapshot,
							2,
							skey);

		found = false;

		while (HeapTupleIsValid(ntp = systable_getnext(scandesc)))
		{
#if PG_VERSION_NUM >= 120000
			Form_pg_class classform = (Form_pg_class) GETSTRUCT(ntp);

			if (found)
				elog(ERROR,
					"unexpected duplicate for tablespace %u, relfilenode %u",
					reltablespace, relfilenode);
			found = true;

			Assert(classform->reltablespace == reltablespace);
			Assert(classform->relfilenode == relfilenode);
			relid = classform->oid;
#else
			if (found)
				elog(ERROR,
					"unexpected duplicate for tablespace %u, relfilenode %u",
					reltablespace, relfilenode);
			found = true;
			relid = HeapTupleGetOid(ntp);
#endif
		}

		systable_endscan(scandesc);
#if PG_VERSION_NUM >= 120000
		table_close(relation, AccessShareLock);
#else
		heap_close(relation, AccessShareLock);
#endif
		/* check for tables that are mapped but not shared */
		if (!found)
			relid = RelationMapFilenodeToOid(relfilenode, false);
	}

	/*
	 * Only enter entry into cache now, our opening of pg_class could have
	 * caused cache invalidations to be executed which would have deleted a
	 * new entry if we had entered it above.
	 */
	entry = hash_search(RelfilenodeMapHashDirty, (void *) &key, HASH_ENTER, &found);
	if (found)
		elog(ERROR, "corrupted hashtable");
	entry->relid = relid;

	return relid;
}

Datum
pg_list_orphaned(PG_FUNCTION_ARGS)
{

	pg_build_orphaned_list(MyDatabaseId);
	pg_list_orphaned_internal(fcinfo);
	return (Datum) 0;
}


Datum
pg_remove_orphaned(PG_FUNCTION_ARGS)
{

	Oid                     dbOid;
	ListCell   *cell;

	dbOid = MyDatabaseId;
	pg_build_orphaned_list(dbOid);

	for (cell = list_head(list_orphaned_relations); cell != NULL; cell = lnext(cell))
	{
		char  orphaned_file[MAXPGPATH + 21 + sizeof(TABLESPACE_VERSION_DIRECTORY) + sizeof(Oid)] = {0};
		OrphanedRelation  *orph = (OrphanedRelation *)lfirst(cell);
		strcat(orphaned_file, orph->path);
		strcat(orphaned_file, "/");
		strcat(orphaned_file, orph->name);
		durable_unlink(orphaned_file,ERROR);
	}

	PG_RETURN_VOID();
}

void
pg_list_orphaned_internal(FunctionCallInfo fcinfo)
{

	ReturnSetInfo   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate *tupstore;
	TupleDesc           tupdesc;
	MemoryContext   per_query_ctx;
	MemoryContext   oldcontext;
	ListCell   *cell;

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/*
	 * Build a tuple descriptor for our result type
	 */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	for (cell = list_head(list_orphaned_relations); cell != NULL; cell = lnext(cell))
	{
		OrphanedRelation  *orph = (OrphanedRelation *)lfirst(cell);

		Datum           values[7];
		bool            nulls[7];
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(orph->dbname);
		values[1] = CStringGetTextDatum(orph->path);
		values[2] = CStringGetTextDatum(orph->name);
		values[3] = Int64GetDatum(orph->size);
		values[4] = TimestampTzGetDatum(orph->mod_time);
		values[5] = Int64GetDatum(orph->relfilenode);
		values[6] = Int64GetDatum(orph->reloid);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	tuplestore_donestoring(tupstore);
}


void
search_orphaned(List **flist, Oid dboid, const char* dbname, const char* dir, Oid reltablespace)
{

	Oid                     oidrel = InvalidOid;
	Oid                     relfilenode = InvalidOid;
	char *relfilename;
	DIR                *dirdesc;
	struct dirent *de;
	OrphanedRelation *orph;

	dirdesc = AllocateDir(dir);
	if (!dirdesc)
		return ;

	while ((de = ReadDir(dirdesc, dir)) != NULL)
	{
		char            path[MAXPGPATH * 2];
		struct stat attrib;

		/* Skip hidden files */
		if (de->d_name[0] == '.')
			continue;

		/* Get the file info */
		snprintf(path, sizeof(path), "%s/%s", dir, de->d_name);
		if (stat(path, &attrib) < 0)
			ereport(ERROR,
				(errcode_for_file_access(),
				errmsg("could not stat directory \"%s\": %m", dir)));

		/* Ignore anything but regular files */
		if (!S_ISREG(attrib.st_mode))
			continue;

		/* Ignore non digit files */
		if (strstr(de->d_name, "_") == NULL && isdigit((unsigned char) *(de->d_name))) {
			orph = palloc(sizeof(*orph));
			relfilename = strdup(de->d_name);
			relfilenode = (Oid) strtoul(relfilename, &relfilename, 10);
			oidrel = RelidByRelfilenodeDirty(reltablespace, relfilenode);
			if (!OidIsValid(oidrel)) {
				orph->dbname = strdup(dbname);
				orph->path = strdup(dir);
				orph->name = strdup(de->d_name);
				orph->size = (int64) attrib.st_size;
				orph->mod_time = time_t_to_timestamptz(attrib.st_mtime);
				orph->relfilenode = relfilenode;
				orph->reloid = oidrel;
				*flist = lappend(*flist, orph);
			}
		/* 
		 * unless is this a temp table?
		 * In that case ensure we are connected to the database we want to get orphaned from
		 * to avoid false positive 
		 * temp table format on disk is: t%d_%u
		 * so that we check it starts with a t
                 * and then check the format with a regex
		 */
		} else if (de->d_name[0] == 't') {
			int i;
			pg_wchar   *wstr;
			int        wlen;
			pg_wchar   *regwstr;
			int        regwlen;
			int     r;
			char *regex = "^t[0-9]*_[0-9]";
			int  regcomp_result;
			char            errMsg[100];
			regex_t* preg = palloc(sizeof(regex_t));
			char	   *t;
			char       *tokptr = NULL;
			char *temprel;

			regwstr = palloc((strlen(regex) + 1) * sizeof(pg_wchar));
			regwlen = pg_mb2wchar_with_len(regex, regwstr, strlen(regex));

			regcomp_result = pg_regcomp(preg,
							regwstr,
							regwlen,
							REG_ADVANCED | REG_NOSUB,
							DEFAULT_COLLATION_OID);

			pfree (regwstr);

			if (regcomp_result == REG_OKAY) {
				wstr = palloc((strlen(de->d_name) + 1) * sizeof(pg_wchar));
				wlen = pg_mb2wchar_with_len(de->d_name, wstr, strlen(de->d_name));
				r = pg_regexec(preg, wstr, wlen, 0, NULL, 0, NULL, 0);
				if (r != REG_NOMATCH) {
					temprel = pstrdup(de->d_name);
					for (i = 0, t = strtok_r(temprel, "_", &tokptr); t != NULL; i++, t = strtok_r(NULL, "_", &tokptr))
					{
						if (i == 1) {
							orph = palloc(sizeof(*orph));
							relfilename = strdup(t);
							relfilenode = (Oid) strtoul(relfilename, &relfilename, 10);
							oidrel = RelidByRelfilenodeDirty(reltablespace, relfilenode);
							if (!OidIsValid(oidrel)) {
								orph->dbname = strdup(dbname);
								orph->path = strdup(dir);
								orph->name = strdup(de->d_name);
								orph->size = (int64) attrib.st_size;
								orph->mod_time = time_t_to_timestamptz(attrib.st_mtime);
								orph->relfilenode = relfilenode;
								orph->reloid = oidrel;
								*flist = lappend(*flist, orph);
							}
						}	
					}
				}
				pfree (wstr);
			} else {
				/* regex didn't compile */
				pg_regerror(regcomp_result, preg, errMsg, sizeof(errMsg));
						ereport(ERROR,
							(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
							errmsg("invalid regular expression: %s", errMsg)));
			}
			pg_regfree (preg);
		}
	}
	FreeDir(dirdesc);
}


void
pg_build_orphaned_list(Oid dbOid)
{

	const char *dbName = NULL;
	DIR                *dirdesc;
	struct dirent *direntry;
	char            dirpath[MAXPGPATH];
	char            dir[MAXPGPATH + 21 + sizeof(TABLESPACE_VERSION_DIRECTORY)];
	Oid                     reltbsnode = InvalidOid;
	char *reltbsname;
	MemoryContext   mctx;

	dbName=get_database_name(MyDatabaseId);
	mctx = MemoryContextSwitchTo(TopMemoryContext);

	list_free_deep(list_orphaned_relations);
	list_orphaned_relations=NIL;

	/* default tablespace */
	snprintf(dir, sizeof(dir), "base/%u", dbOid);
	search_orphaned(&list_orphaned_relations, dbOid, dbName, dir, 0);

	/* Scan the non-default tablespaces */
	snprintf(dirpath, MAXPGPATH, "pg_tblspc");
	dirdesc = AllocateDir(dirpath);

	while ((direntry = ReadDir(dirdesc, dirpath)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(dir, sizeof(dir), "pg_tblspc/%s/%s/%u",
			direntry->d_name, TABLESPACE_VERSION_DIRECTORY, dbOid);

		reltbsname = strdup(direntry->d_name);
		reltbsnode = (Oid) strtoul(reltbsname, &reltbsname, 10);

		search_orphaned(&list_orphaned_relations, dbOid, dbName, dir, reltbsnode);
	}
	FreeDir(dirdesc);
	MemoryContextSwitchTo(mctx);
}
