pg_orphaned
===================

Features
--------

Allow to list orphaned files thanks to a *pg_list_orphaned* function.

Introduction
============

If you are not aware of orphaned files, you can have a look to [this blog post first.](https://blog.dbi-services.com/can-there-be-orphaned-data-files-in-postgresql/)

While you could get a list of the orphaned files with a query (as the one into the blog post mentioned above), you could get
false positive due to in progress transactions (means started and not committed or rolled back yet) that are creating files (like create table, relation rewrites...).

This extension is taking care of such corner cases by using a dirty snapshot while looking for the relfilnode(s) in pg_class.

Installation
============

Compiling
---------

The extension can be built using the standard PGXS infrastructure. For this to
work, the ``pg_config`` program must be available in your $PATH. Instruction to
install follows:

    $ git clone
    $ cd pg_orphaned
    $ make
    $ make install
    $ psql DB -c "CREATE EXTENSION pg_orphaned;"

Examples
=======

Example 1:
----------

```
postgres=# create database test;
CREATE DATABASE
postgres=# \c test
You are now connected to database "test" as user "postgres".

test=# create table bdt as select * from generate_series(1,40000000);
SELECT 40000000

test=# select * from pg_list_orphaned() order by relfilenode;
 dbname | path | name | size | mod_time | relfilenode | reloid
--------+------+------+------+----------+-------------+--------
(0 rows

test=# begin;
BEGIN
test=# create table bdtorph as select * from generate_series(1,40000000);
SELECT 40000000
test=# create index orphidx on bdtorph(generate_series);
CREATE INDEX

test=# select pg_relation_filepath ('bdtorph');
 pg_relation_filepath
----------------------
 base/294991/294997
(1 row)

test=# select pg_relation_filepath ('orphidx');
 pg_relation_filepath
----------------------
 base/294991/295000
(1 row)

$ backend has been killed -9

test=# select pg_relation_filepath ('orphidx');
server closed the connection unexpectedly
        This probably means the server terminated abnormally
        before or while processing the request.

$ reconnect and search for orphaned files

test=# select pg_relation_filepath ('orphidx');
ERROR:  relation "orphidx" does not exist
LINE 1: select pg_relation_filepath ('orphidx');

test=#
test=# select pg_relation_filepath ('bdtorph');
ERROR:  relation "bdtorph" does not exist
LINE 1: select pg_relation_filepath ('bdtorph');

test=# select * from pg_list_orphaned() order by relfilenode;
 dbname |    path     |   name   |    size    |        mod_time        | relfilenode | reloid
--------+-------------+----------+------------+------------------------+-------------+--------
 test   | base/294991 | 294997.1 |  376176640 | 2020-05-03 16:18:36+00 |      294997 |      0
 test   | base/294991 | 294997   | 1073741824 | 2020-05-03 16:16:03+00 |      294997 |      0
 test   | base/294991 | 295000   |  898490368 | 2020-05-03 16:20:16+00 |      295000 |      0
(3 rows)
```

Example 2:
----------
```
test=# CREATE TABLESPACE bdttbs location '/usr/local/pgsql12.2-orphaned/bdttbs';
CREATE TABLESPACE
test=# begin;
BEGIN
test=# create table bdtorph tablespace bdttbs as select * from generate_series(1,40000000);
SELECT 40000000

test=# select pg_relation_filepath ('bdtorph');
              pg_relation_filepath
------------------------------------------------
 pg_tblspc/303184/PG_12_201909212/303183/303185
(1 row)

$ backend has been killed -9

test=# select pg_relation_filepath ('bdtorph');
server closed the connection unexpectedly
        This probably means the server terminated abnormally
        before or while processing the request.

$ reconnect and search for orphaned files

test=# select pg_relation_filepath ('bdtorph');
ERROR:  relation "bdtorph" does not exist
LINE 1: select pg_relation_filepath ('bdtorph');

test=# select * from pg_list_orphaned() order by relfilenode;
 dbname |                  path                   |   name   |    size    |        mod_time        | relfilenode | reloid
--------+-----------------------------------------+----------+------------+------------------------+-------------+--------
 test   | pg_tblspc/303184/PG_12_201909212/303183 | 303185   | 1073741824 | 2020-05-03 17:28:49+00 |      303185 |      0
 test   | pg_tblspc/303184/PG_12_201909212/303183 | 303185.1 |  376176640 | 2020-05-03 17:30:18+00 |      303185 |      0
(2 rows)
```
Example 3:
----------
```
test=# begin;
BEGIN
test=# create temp table bdtorphtemp as select * from generate_series(1,40000000);
SELECT 40000000

test=# select pg_relation_filepath ('bdtorphtemp');
 pg_relation_filepath
-----------------------
 base/311377/t4_311380
(1 row)

$ backend has been killed -9

test=# select pg_relation_filepath ('bdtorphtemp');
server closed the connection unexpectedly
        This probably means the server terminated abnormally
        before or while processing the request.

$ reconnect and search for orphaned files

test=# select pg_relation_filepath ('bdtorphtemp');
ERROR:  relation "bdtorphtemp" does not exist
LINE 1: select pg_relation_filepath ('bdtorphtemp');

test=# select * from pg_list_orphaned() order by relfilenode;
 dbname |    path     |    name     |    size    |        mod_time        | relfilenode | reloid
--------+-------------+-------------+------------+------------------------+-------------+--------
 test   | base/311377 | t4_311380.1 |  376176640 | 2020-05-03 17:35:03+00 |      311380 |      0
 test   | base/311377 | t4_311380   | 1073741824 | 2020-05-03 17:34:59+00 |      311380 |      0
(2 rows)
```
Example 4 (deal with in progress transaction):
----------
```
Session 1:

postgres=# begin;
BEGIN
postgres=*# create table bdtinpgro (a int);
CREATE TABLE

Session 2 would report a false orphaned file if using a query like:

postgres=# select * from pg_ls_dir ( '/home/postgres/pgorph/pg_installed/data/base/13580' ) as file where file ~ '^[0-9]*$' and file::text not in (select oid::text from pg_class );
 file
-------
 16408
(1 row)

while the extension would not report this false positive:

postgres=# select * from pg_list_orphaned();
 dbname | path | name | size | mod_time | relfilenode | reloid
--------+------+------+------+----------+-------------+--------
(0 rows)
```
Remarks
=======
* double check carefully before taking any actions on those files
* has been tested from version 10 to 13
* pg_list_orphaned does the search for the database it is connected to
* at the tilme of this writing (08/2021) there is a [commitfest entry](https://commitfest.postgresql.org/34/3228/) to avoid orphaned files

License
=======

pg_orphaned is free software distributed under the PostgreSQL license.

Copyright (c) 2020, Bertrand Drouvot.
