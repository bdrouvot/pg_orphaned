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
#include "utils/memutils.h"
#include "utils/timestamp.h"
#else
#include "catalog/pg_collation_d.h"
#endif


PG_MODULE_MAGIC;
Datum pg_list_orphaned(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_list_orphaned);

List   *list_orphaned_relations=NULL;
static void pg_list_orphaned_internal(FunctionCallInfo fcinfo);
static void search_orphaned(List **flist, const char *dbname, const char *dir, Oid reltablespace);

typedef struct OrphanedRelation {
	char	   *dbname;
	char	   *path;
	char	   *name;
	int size;
	TimestampTz mod_time;
	Oid relfilenode;
	Oid reloid;
} OrphanedRelation;

Datum
pg_list_orphaned(PG_FUNCTION_ARGS)
{

	text *dbNameText = NULL;
	const char *dbName = NULL;
	DIR                *dirdesc;
	struct dirent *direntry;
	char            dirpath[MAXPGPATH];
	char            dir[MAXPGPATH + 21 + sizeof(TABLESPACE_VERSION_DIRECTORY)];
	Oid                     dbOid;
	Oid                     reltbsnode = InvalidOid;
	char *reltbsname;
	MemoryContext   mctx;

	if (PG_ARGISNULL(0))
	{
		dbName=get_database_name(MyDatabaseId);
	} else {
		dbNameText = PG_GETARG_TEXT_PP(0);
		dbName = text_to_cstring(dbNameText);
	}

	dbOid = get_database_oid(dbName, false);
	mctx = MemoryContextSwitchTo(TopMemoryContext);

	list_free_deep(list_orphaned_relations);
	list_orphaned_relations=NIL;

	/* default tablespace */
	snprintf(dir, sizeof(dir), "base/%u", dbOid);
	search_orphaned(&list_orphaned_relations, dbName, dir, 0);

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

		search_orphaned(&list_orphaned_relations, dbName, dir, reltbsnode);
	}
	FreeDir(dirdesc);
	MemoryContextSwitchTo(mctx);

	pg_list_orphaned_internal(fcinfo);
	return (Datum) 0;
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
search_orphaned(List **flist, const char* dbname, const char* dir, Oid reltablespace)
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
			oidrel = RelidByRelfilenode(reltablespace, relfilenode);
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
		} else if (de->d_name[0] == 't' && get_database_oid(dbname, false) == MyDatabaseId) {
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
							oidrel = RelidByRelfilenode(reltablespace, relfilenode);
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
