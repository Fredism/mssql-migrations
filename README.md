Creates scripts to migrate database models and data from a source db to a target.

Generates 4 scripts:

create.sql
alter.sql
seed.sql
update.sql

To be run in that order. 

The scripts are idempotent, meaning you can rerun them with no side-effects.

**create.sql**: runs conditional CREATE statements for schemas, tables, functions, indexes, views, and stored procedures  

**alter.sql**: runs conditional ALTER statements for columns (DROP, ADD, ALTER) + constraints, always runs ALTER on functions, views, and stored procedures  

**seed.sql**: runs conditional INSERT statements for tables in the schemas specified by Appsettings.Data.Schemas  

**update.sql**: conditionally generates INSERT/UPDATE statements for target tables specified by Appsettings.Data.Schemas that have been touched prior to the last INSERT/UPDATE  
