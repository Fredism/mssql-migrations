Creates scripts to migrate database models and data from a source db to a target.

Generates 5 scripts:

patch.sql
create.sql
seed.sql
update.sql
alter.sql

To be run in that order. 

The scripts are idempotent (besides patch*), meaning you can rerun them with no side-effects.

**patch.sql**:  
+ runs DROP, ADD, and ALTER COLUMN where the target does not match the source  
+ runs DROP CONSTRAINT where target not in source  
+ not idempotent but regenerating the scripts will result in a patch.sql with only the remaining (if any) statements  

**create.sql**:
+ runs conditional CREATE statements for schemas, tables, functions, indexes, views, and stored procedures   

**seed.sql**
+ runs conditional INSERT statements for tables in the specified schemas

**update.sql**:
+ conditionally generates INSERT/UPDATE statements for target tables specified that have been touched prior to the last INSERT/UPDATE

**alter.sql**:
+ runs conditional ADD statements on constraints, runs ALTER on functions, views, and stored procedures
