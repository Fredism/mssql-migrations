using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using Migrate.Models;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Primitives;

namespace Migrate
{
    class Query
    {
        public static readonly string BatchSeperator = $"GO\n\n";
        // create
        public static string CreateSchema(string schema)
        {
            return string.Join("\n", new string[]
            {
                $"IF NOT EXISTS (SELECT schema_id FROM sys.schemas WHERE name = '{schema}')",
                "BEGIN",
                $"\tEXEC sp_executesql N'CREATE SCHEMA {schema}'",
                "END\n"
            });
        }
        //public static string CreateTable(SysTable table, SysColumn[] columns)
        //{
        //    var pKey = columns.ToList().Find(c => c.primary_key != null);
        //    return string.Join("\n", new string[]
        //    {
        //        $"create table {ToObjectName(table)} (",
        //        string.Join(",\n",
        //         columns.Select(column => {
        //            bool
        //            isString = column.data_type.Contains("varchar"),
        //            isNullable = Convert.ToBoolean(column.is_nullable),
        //            isIdentity = Convert.ToBoolean(column.is_identity);
        //            string maxLength = column.max_length == "-1"? "max": column.max_length;
        //            return  $"\t[{column.name}] [{column.data_type}]{(isString? $"({maxLength})": "")} {(isIdentity? "identity(1, 1) ": "")}{(isNullable? "": "not")} null{(column.default_constraint_name == null? "": $" constraint [{column.default_constraint_name}] default {column.default_constraint_definition}")}";
        //         })
        //        ),
        //        pKey == null? "": $"\tconstraint [{pKey.primary_key}] primary key {pKey.index_type_desc} ( [{pKey.name}] asc )",
        //        ")"
        //    });
        //}
        public static string CreateTable(SysTable table, List<SysColumn> columns)
        {
            var pKey = columns.Find(c => c.primary_key != null);
            var cmd = new List<string>
            {
                $"IF (OBJECT_ID('{table.qualified_name}') IS NULL)",
                "BEGIN",
                    $"\tCREATE TABLE {table.qualified_name} (",
                    string.Join(",\n", columns.Select(column => "\t\t" + column.column_definition)),

            };
            if (pKey != null)
            {
                cmd.Add($"\t\tCONSTRAINT [{pKey.primary_key}] PRIMARY KEY {pKey.index_type_desc} ( [{pKey.name}] ASC )");
            }
            if (table.history_table != null)
            {
                var rowStart = columns.Find(c => c.generated_always_type == "1").name;
                var rowEnd = columns.Find(c => c.generated_always_type == "2").name;
                cmd.Add($"\t\t,PERIOD FOR SYSTEM_TIME ([{rowStart}], [{rowEnd}])");
                cmd.Add("\t)");
                cmd.Add("WITH (");
                cmd.Add($"SYSTEM_VERSIONING = ON ( HISTORY_TABLE = [{table.schema_name}].[{table.history_table}] )");
                cmd.Add(")");
            }
            else
            {
                cmd.Add("\t)");
            }
            cmd.Add("END\n");


            return string.Join("\n", cmd);
        }
        public static string CreateIndex(SysIndex index)
        {
            return string.Join("\n", new string[]
            {
                $"IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = '{index.name}' and object_id = OBJECT_ID('{index.qualified_table_name}'))",
                $"CREATE {index.type_desc} INDEX [{index.name}] ON {index.qualified_table_name} {index.columns}",
                (index.include == null? "": $"INCLUDE {index.include}\n") + BatchSeperator
            });
        }
        public static string CreateTableType(SysTableType tt)
        {
            return string.Join("\n", new string[]
            {
                $"IF (TYPE_ID('{tt.qualified_name}') IS NULL)",
                $"CREATE TYPE {tt.qualified_name} AS TABLE(",
                string.Join(",\n", tt.columns.Split(", ").Select(c => "\t" + c)),
                $")\n{BatchSeperator}"
            });
        }
        public static string CreateFunction(SysObject func)
        {
            return CreateObjectDefinition(func);
        }
        public static string CreateView(SysObject view)
        {
            return CreateObjectDefinition(view);
        }
        public static string CreateProc(SysObject proc)
        {
            return CreateObjectDefinition(proc);
        }
        private static string CreateObjectDefinition(SysObject @object)
        {
            return string.Join("\n", new string[] {
                $"IF (OBJECT_ID('{@object.qualified_name}') IS NOT NULL)",
                "\tSET NOEXEC ON",
                "GO\n",
                @object.object_definition,
                "GO\n",
                "SET NOEXEC OFF",
                BatchSeperator
            });
        }

        // drop
        public static string DropSchema(string schema)
        {
            return string.Join("\n", new string[]
            {
                $"IF EXISTS (SELECT schema_id FROM sys.schemas WHERE name = '{schema}')",
                $"\tDROP SCHEMA {schema}\n",
            });
        }
        public static string DropTable(SysTable table)
        {
            return string.Join("\n", new string[]
            {
                $"IF EXISTS (SELECT object_id FROM sys.tables WHERE object_id = OBJECT_ID('{table.qualified_name}'))",
                $"\tDROP TABLE {table.qualified_name}\n",
            });
        }

        // alter
        public static string AddColumn(SysColumn column)
        {
            return string.Join("\n", new string[]
            {
                $"ALTER TABLE {column.qualified_table_name}",
                $"ADD [{column.name}] {column.type_definition} {column.nullability}\n"
            });
        }
        public static string AlterColumn(SysColumn column)
        {
            return string.Join("\n", new string[]
            {
                $"ALTER TABLE {column.qualified_table_name}",
                $"ALTER COLUMN [{column.name}] {column.type_definition} {column.nullability}\n"
            });
        }
        public static string DropColumn(SysColumn column)
        {
            return string.Join("\n", new string[]
            {
                $"ALTER TABLE {column.qualified_table_name}",
                $"DROP COLUMN [{column.name}]\n"
            });
        }
        public static string DropIndex(SysIndex index)
        {
            return string.Join("\n", new string[]
            {
                $"IF EXISTS (SELECT * FROM sys.indexes WHERE name = '{index.name}' and object_id = OBJECT_ID('{index.qualified_table_name}'))",
                $"DROP INDEX [{index.name}] ON {index.qualified_table_name}\n",
            });
        }
        public static string AddPrimaryKey(SysConstraint key)
        {
            return string.Join("\n", new string[]
            {
                $"IF (OBJECT_ID('{key.qualified_name}') IS NULL)",
                "BEGIN",
                $"\tALTER TABLE {key.qualified_table_name}",
                $"\tADD CONSTRAINT [{key.name}] PRIMARY KEY ({key.columns})",
                "END\n"
            });
        }
        public static string DropPrimaryKey(SysConstraint key)
        {
            return string.Join("\n", new string[]
            {
                $"IF (OBJECT_ID('{key.qualified_name}') IS NOT NULL)",
                "BEGIN",
                $"\tALTER TABLE {key.qualified_table_name}",
                $"\tDROP CONSTRAINT [{key.name}]",
                "END\n"
            });
        }
        public static string AddForeignKey(SysForeignKey key)
        {
            return string.Join("\n", new string[]
            {
                $"IF (OBJECT_ID('{key.qualified_name}') IS NULL)",
                "BEGIN",
                $"\tALTER TABLE {key.qualified_parent_table} WITH {key.check_status} ADD CONSTRAINT [{key.constraint_name}] FOREIGN KEY([{key.parent_column}])",
                $"\tREFERENCES {key.qualified_referenced_table} ([{key.referenced_column}])",
                "END\n"
            });
        }
        public static string DropForeignKey(SysForeignKey key)
        {
            return string.Join("\n", new string[]
            {
                $"IF (OBJECT_ID('{key.qualified_name}') IS NOT NULL)",
                "BEGIN",
                $"\tALTER TABLE {key.qualified_parent_table}",
                $"\tDROP CONSTRAINT [{key.constraint_name}]",
                "END\n"
            });
        }
        public static string AddDefaultConstraint(SysConstraint constraint)
        {
            return string.Join("\n", new string[]
            {
                $"IF (OBJECT_ID('{constraint.qualified_name}') IS NULL)",
                "BEGIN",
                $"\tALTER TABLE {constraint.qualified_table_name}",
                $"\tADD CONSTRAINT [{constraint.name}] DEFAULT {constraint.definition} FOR [{constraint.columns}]",
                "END\n"
            });
        }
        public static string AddUniqueConstraint(SysConstraint constraint)
        {
            return string.Join("\n", new string[]
            {
                $"IF (OBJECT_ID('{constraint.qualified_name}') IS NULL)",
                "BEGIN",
                $"\tALTER TABLE {constraint.qualified_table_name}",
                $"\tADD CONSTRAINT [{constraint.name}] UNIQUE ({constraint.columns})",
                "END\n"
            });
        }
        public static string AddCheckConstraint(SysConstraint constraint)
        {
            return string.Join("\n", new string[]
            {
                $"IF (OBJECT_ID('{constraint.qualified_name}') IS NULL)",
                "BEGIN",
                $"\tALTER TABLE {constraint.qualified_table_name} WITH {constraint.check_status}",
                $"\tADD CONSTRAINT [{constraint.name}] CHECK {constraint.definition}",
                "END\n"
            });
        }
        public static string AlterFunc(SysObject func)
        {
            return AlterObject(func);
        }
        public static string AlterView(SysObject view)
        {
            return AlterObject(view);
        }
        public static string AlterProc(SysObject proc)
        {
            return AlterObject(proc);
        }
        private static string AlterObject(SysObject @object)
        {
            var pattern = $@"CREATE(\s+\w+\s+(?:\[?{@object.schema_name}\]?)?\.?\[?{@object.name}\]?)";
            var definition = Regex.Replace(@object.object_definition, pattern, "ALTER$1", RegexOptions.IgnoreCase);
            return string.Join("\n", new string[] {
                $"IF EXISTS (SELECT object_id FROM sys.objects WHERE object_id = OBJECT_ID('{@object.qualified_name}')"
                    + $" AND modify_date {(!@object.modify_date.HasValue? "IS NULL": $"> {DateTimeFromParts(@object.modify_date.Value)}")})",
                "\tSET NOEXEC ON",
                "GO\n",
                definition,
                "GO\n",
                "SET NOEXEC OFF",
                BatchSeperator
            });
        }
        private static string DateTimeFromParts(DateTime dt)
        {
            return $"DATETIMEFROMPARTS({dt.Year}, {dt.Month}, {dt.Day}, {dt.Hour}, {dt.Minute}, {dt.Second}, {dt.Millisecond})";
        }

        // select
        private static string TablePredicate(IEnumerable<string> tables)
        {
            var conditions = tables.Select(name =>
            {
                var parts = name.Split(".");
                var schema = parts[0];
                var table = parts[1];

                return $" (schema_name(t.schema_id) = '{schema}' and t.name = '{table}') ";
            });
            return string.Join("or \n", conditions);
        }
        public static string GetTables(IEnumerable<int> schemas = null, IEnumerable<string> includes = null, IEnumerable<string> excludes = null)
        {
            var schemaPredicate = FilterSchemas(schemas);
            var predicate = schemaPredicate;
            if (includes != null)
            {
                var inCondition = TablePredicate(includes);

                if (string.IsNullOrEmpty(predicate))
                {
                    predicate = $"and ({inCondition})";
                }
                else
                {
                    predicate = $"and (({inCondition}) or ({schemaPredicate.Replace("and ", "")}))";
                }
            }

            if (excludes != null)
            {
                var exCondition = TablePredicate(excludes);

                if (string.IsNullOrEmpty(predicate))
                {
                    predicate = $"and not ({exCondition})";
                }
                else
                {
                    predicate += $" and not ({exCondition})";
                }
            }
            return $@"
                select 
                    t.name,
                    t.object_id,
                    t.schema_id,
                    schema_name = schema_name(t.schema_id),
                    t.create_date,
                    has_identity = (select 1 from sys.columns c where c.object_id = t.object_id and c.is_identity = '1'),
                    history_table = object_name(t.history_table_id)
                    from sys.tables t
                    where t.temporal_type <> 1
                    {predicate ?? ""}
                    order by t.create_date
            ";
        }
        public static string GetColumns(IEnumerable<int> schemas)
        {
            return $@"
                select 
                    c.name,
		            c.object_id,
		            object_name = object_name(c.object_id),
		            schema_name = object_schema_name(c.object_id),
		            data_type = type_name(c.user_type_id),
		            max_length = case 
                        when type_name(c.user_type_id) = 'datetime2' then scale
						when type_name(c.user_type_id) like 'n%char%' and max_length <> -1 then max_length / 2
                        else max_length end,
                    precision = c.precision,
					scale = c.scale,
		            is_nullable,
		            is_identity,
		            primary_key = i.name,
		            index_type_desc = i.type_desc,
                    c.generated_always_type,
		            default_constraint_name = dc.name,
		            default_constraint_definition = cast(dc.definition as varchar(max))
		            from sys.columns c
		            join sys.tables t on t.object_id = c.object_id
		            left join sys.index_columns ic on ic.object_id = c.object_id and ic.column_id = c.column_id and ic.index_id = 1
		            left join sys.indexes i on i.object_id = c.object_id and i.index_id = ic.index_column_id and i.is_primary_key = 1
		            left join sys.default_constraints dc on dc.object_id = c.default_object_id
                    where t.schema_id in ({string.Join(", ", schemas)})
            ";
        }
        public static string GetForeignKeys(IEnumerable<int> schemas)
        {
            return $@"
                select constraint_id = f.object_id,
	            constraint_name = f.name,
	            schema_name = schema_name(f.schema_id),
	            f.parent_object_id,
	            parent_object_name = object_name(f.parent_object_id),
	            parent_column = pc.name,
	            f.referenced_object_id,
                referenced_schema_name = object_schema_name(rc.object_id),
	            referenced_object_name = object_name(f.referenced_object_id),
	            referenced_column = rc.name,
                is_not_trusted = cast(f.is_not_trusted as varchar)
	            from sys.foreign_keys f
	            join sys.foreign_key_columns fc on fc.constraint_object_id = f.object_id
	            join sys.columns pc on pc.object_id = fc.parent_object_id and pc.column_id = fc.parent_column_id
	            join sys.columns rc on rc.object_id = fc.referenced_object_id and rc.column_id = fc.referenced_column_id
                where f.schema_id in ({string.Join(", ", schemas)})
            ";
        }
        public static string GetTableTypes(IEnumerable<int> schemas = null)
        {
            return $@"
                select 
	                name,
	                object_id,
	                schema_id,
	                schema_name,
	                columns = stuff(columns, len(columns), 1, N'') from (
                select 
		                tt.name,
		                tt.type_table_object_id as [object_id],
		                tt.schema_id,
		                schema_name = schema_name(tt.schema_id),
		                columns = (select c.name + ' ' + type_name(c.user_type_id) + 
					                case 
						                when type_name(c.user_type_id) like '%varchar' 
							                then '(' + case when c.max_length = -1 then 'max' else cast(c.max_length as nvarchar) end + ')'
						                when type_name(c.user_type_id) = 'datetime2'
							                then '(' + cast(c.scale as nvarchar)+ ')'
						                else '' 
					                end + ' ' +
					                case 
						                when c.is_nullable = 1 
							                then '' 
						                else 'not '
					                end + 'null' + ', '
				                from sys.columns c where c.object_id = tt.type_table_object_id for xml path(''))
		                from sys.table_types tt
                        {(schemas == null ? "" : $"where tt.schema_id in ({string.Join(", ", schemas)})")}
		                group by tt.type_table_object_id, tt.name, tt.schema_id
                ) t
            ";
        }
        public static string GetFunctions(IEnumerable<int> schemas = null)
        {
            return $@"
                select name,
	                object_id,
	                object_definition = object_definition(object_id),
	                schema_id,
	                schema_name = schema_name(schema_id),
                    type,
	                create_date,
	                modify_date
	                from sys.objects
	                where type in ('FN', 'IF', 'TF')
                    {FilterSchemas(schemas)}
            ";
        }
        public static string GetStoredProcedures(IEnumerable<int> schemas = null)
        {
            return $@"
                select name,
	                object_id,
	                object_definition = object_definition(object_id),
	                schema_id,
	                schema_name = schema_name(schema_id),
                    type,
	                create_date,
	                modify_date
	                from sys.objects
	                where type = 'P'
                    {FilterSchemas(schemas)}
            ";
        }
        public static string GetViews(IEnumerable<int> schemas = null)
        {
            return $@"
                select name,
	                object_id,
	                object_definition = object_definition(object_id),
	                schema_id,
	                schema_name = schema_name(schema_id),
                    type,
	                create_date,
	                modify_date
	                from sys.objects
	                where type = 'V'
                    {FilterSchemas(schemas)}
            ";
        }
        public static string GetConstraints(IEnumerable<int> schemas = null)
        {
            return $@"
                -- primary key constraints --
                select
	                pk.name,
	                pk.object_id,
                    pk.parent_object_id,
	                schema_name = object_schema_name(pk.object_id),
	                table_name = object_name(pk.parent_object_id),
	                pk.type,
	                columns = c.name,
	                definition = null,
                    is_not_trusted = null
	                from sys.key_constraints pk
	                join sys.index_columns ic on ic.object_id = pk.parent_object_id and ic.index_id = pk.unique_index_id
	                join sys.columns c on c.column_id = ic.column_id and c.object_id = ic.object_id
	                join sys.tables t on t.object_id = pk.parent_object_id
	                where pk.type = 'PK'
	                and t.temporal_type <> 1
                    and schema_id(object_schema_name(pk.object_id)) in ({string.Join(", ", schemas)})

                union

                -- unique constraints --
                select
	                name,
	                object_id,
	                parent_object_id,
	                schema_name,
	                table_name,
	                type,
	                columns = stuff(columns, len(columns), 1, N''),
	                definition = null,
                    is_not_trusted = null from (
                select
	                uq.name,
	                uq.object_id,
                    uq.parent_object_id,
	                schema_name = schema_name(uq.schema_id),
	                table_name = object_name(uq.parent_object_id),
	                uq.type,
	                columns = (
		                select '[' + c.name + '] ' +
			                case when ic.is_descending_key = 0
				                then 'asc' else 'desc' end + ', '
			                from sys.index_columns ic
			                join sys.columns c on c.column_id = ic.column_id and c.object_id = ic.object_id
			                where ic.object_id = uq.parent_object_id and ic.index_id = uq.unique_index_id
			                for xml path(N'')
	                ),
	                definition = null,
                    is_not_trusted = null
	                from
	                sys.key_constraints uq
	                join sys.tables t on t.object_id = uq.parent_object_id
	                where uq.type = 'UQ'
	                and t.temporal_type <> 1
                    and uq.schema_id in ({string.Join(", ", schemas)})
	                group by uq.unique_index_id, uq.object_id, uq.name, uq.parent_object_id, uq.schema_id, uq.type
                ) t

                union

                -- default constraints --
                select
	                dc.name,
	                dc.object_id,
                    dc.parent_object_id,
	                schema_name = object_schema_name(dc.object_id),
	                table_name = object_name(dc.parent_object_id),
	                type = rtrim(dc.type),
	                columns = c.name,
	                dc.definition,
                    is_not_trusted = null
	                from sys.default_constraints dc
	                join sys.columns c on c.column_id = dc.parent_column_id and c.object_id = dc.parent_object_id
	                where schema_id(object_schema_name(dc.object_id)) in ({string.Join(", ", schemas)})
                
                union

                --  check constraints --
                select
	                name,
	                object_id,
                    parent_object_id,
	                schema_name = schema_name(schema_id),
	                table_name = object_name(parent_object_id),
	                type = rtrim(type),
	                columns = null,
	                definition,
	                is_not_trusted
	                from sys.check_constraints
	                where schema_id in ({string.Join(", ", schemas)})
            ";
        }
        public static string GetIndexes(IEnumerable<int> schemas)
        {
            return $@"
                select
	                name,
	                object_id,
                    index_id,
	                schema_name,
	                table_name,
	                type,
                    type_desc,
	                columns = concat('(', stuff(columns, len(columns), 2, N''), ')'),
	                include = case when include is null then null else concat('(', stuff(include, len(include), 2, N''), ')') end from (
                select
	                i.name,
	                i.object_id,
                    i.index_id,
	                schema_name = object_schema_name(i.object_id),
	                table_name = object_name(i.object_id),
	                i.type,
                    i.type_desc,
	                columns = (select '[' + kc.name + '] ' +
		                case when ic.is_descending_key = 0
			                then 'asc' else 'desc' end + ', '
		                from sys.index_columns ic
		                left join sys.columns kc on kc.column_id = ic.column_id and kc.object_id = ic.object_id
		                where ic.index_id = i.index_id and ic.object_id = i.object_id and ic.is_included_column = 0
		                for xml path(N'')
	                ),
	                include = (select '[' + inc.name + ']' + ', '
		                from sys.index_columns ic
		                left join sys.columns inc on inc.column_id = ic.column_id and inc.object_id = ic.object_id
		                where ic.index_id = i.index_id and ic.object_id = i.object_id and ic.is_included_column = 1
		                for xml path(N'')
	                )
	                from sys.indexes i
	                join sys.tables t on t.object_id = i.object_id
	                where
	                i.type not in (0, 1)
                    and schema_id(object_schema_name(i.object_id)) in ({string.Join(", ", schemas)})
	                --and t.temporal_type <> 1
	                and i.is_unique <>  1
	                and i.is_unique_constraint <>  1
	                group by i.object_id, i.index_id, i.name, i.type, i.type_desc
                ) t
            ";
        }
        public static string GetLastUpdate()
        {
            return $@"
                select
                    o.object_id[id],
                    (
                        select max(dates) from
                        (
                            select dates = max(us.last_user_update)
                            union
                            select dates = max(stats_date(id, indid))
                        ) as dateAcross
                    ) as date
                    from sys.sysindexes i
                    join sys.objects o on i.id = o.object_id
                    join sys.dm_db_index_usage_stats us on us.object_id = o.object_id and us.database_id = db_id(db_name())
                    where o.type = 'U' and 
                        (stats_date(id, indid) is not null or us.last_user_update is not null)
                    group by o.object_id
            ";
        }

        // misc
        public static string SetNoCount(string onOff)
        {
            return $"SET NOCOUNT {onOff}\n";
        }
        public static string ToggleConstraintForEach(bool check)
        {
            return !check ?
                "EXEC sp_msforeachtable \"ALTER TABLE ? NOCHECK CONSTRAINT all\"\n"
                : "EXEC sp_msforeachtable \"ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all\"\n";
        }

        public static string ToggleIdInsertForEach(string onOff)
        {
            return string.Join("\n", new string[]
            {
                //$"EXEC sp_msforeachtable @command1 = \"PRINT '?'; SET IDENTITY_INSERT ? {onOff}\",",
                $"EXEC sp_msforeachtable @command1 = \"SET IDENTITY_INSERT ? {onOff}\",",
                "@whereand = ' AND EXISTS (SELECT 1 from sys.columns WHERE object_id = o.id",
                "AND is_identity = 1) and o.type = ''U'''\n",
            });
        }

        public static string ToggleIdInsert(string table, string onOff)
        {
            return string.Join("\n", new string[]
            {
                $"SET IDENTITY_INSERT {table} {onOff}\n",
            });
        }
        
        public static RowCollection ConvertRows(List<Dictionary<string, object>> rows)
        {
            var result = new RowCollection();
            foreach(var row in rows)
            {
                var rowData = row.ToDictionary(pair => pair.Key, pair => ConvertObject(pair.Value));
                result.Add(rowData);
            }
            return result;
        }

        public static string UpdateIfNull(SysColumn column, string value = null)
        {
            return string.Join("\n", new string[]
            {
                $"UPDATE {column.qualified_table_name}",
                $"SET {column.name} = { value ?? DefaultValue(column.data_type) }",
                $"WHERE {column.name} IS NULL\n"
            });
        }

        public static string UpdateIf(SysTable table, IEnumerable<SysColumn> columns, IDictionary<string, string> data)
        {
            var pKey = columns.ToList().Find(c => c.primary_key != null);
            var pKeyColumn = pKey.name;
            var pKeyValue = data[pKeyColumn];
            return string.Join("\n", new string[]
            {
                $"IF NOT EXISTS (SELECT {pKeyColumn} FROM {table.qualified_name} WHERE {pKeyColumn} = {pKeyValue})",
                $"\tINSERT INTO {table.qualified_name} ({string.Join(", ", columns.Select(c => c.name))})",
                $"\tVALUES ({string.Join(", ", columns.Select(c => data[c.name]))})",
                "ELSE",
                $"\tUPDATE {table.qualified_name} SET",
                $"\t{string.Join(", ", columns.Where(c => c.primary_key == null).Select(c => $"{c.name} = {data[c.name]}"))} WHERE",
                $"\t{pKeyColumn} = {data[pKeyColumn]}\n"
            });
        }

        public static string SeedIf(SysTable table, IEnumerable<SysColumn> columns, RowCollection rows)
        {
            var tableName = table.qualified_name;
            return string.Join("\n", new string[] {
                $"IF NOT EXISTS (SELECT * FROM {tableName})",
                "BEGIN",
                DistributeSeed(tableName, columns, rows),
                "END\n"
            });
        }
        // made to get around sql 1000 
        private static string DistributeSeed(string tableName, IEnumerable<SysColumn> columns, RowCollection rows)
        {
            int rounds = rows.Count() / 1000;
            int round = 0;
            var builder = new StringBuilder();
            while (round <= rounds)
            {
                var set = rows.Skip(round * 1000).Take(1000);
                if (set.Count() > 0)
                {
                    builder.Append($"\tINSERT INTO {tableName} ({string.Join(", ", columns.Select(c => c.name))})\n");
                    builder.Append("\tVALUES \n");
                    builder.Append(string.Join(",\n", set.Select(row =>
                        "(" + string.Join(", ", columns.Select(c => row[c.name])) + ")"
                    )));
                    if(round != rounds) builder.Append("\n");
                }
                round++;
            }

            return builder.ToString();
        }
        private static string FilterSchemas(IEnumerable<int> schemas)
        {
            return schemas == null ?
                        "" : $"and schema_id in ({string.Join(", ", schemas)})";
        }
        private static string DefaultValue(string data_type)
        {
            switch(data_type)
            {
                case "bit":
                case "tinyint":
                case "smallint":
                case "int":
                case "bigint":
                case "decimal":
                case "numeric":
                case "smallmoney":
                case "money":
                case "float":
                case "real":
                    return "0";
                case "datetime":
                case "datetime2":
                case "smalldatetime":
                case "date":
                case "time":
                case "datetimeoffset":
                    return "'1970-01-01 00:00:00.000'";
                default:
                    return "''";
            }
        }
        private static string ConvertObject(object data)
        {
            switch (data)
            {
                case bool b:
                    return b ? "1" : "0";
                case int i:
                    return i.ToString();
                case long l:
                    return l.ToString();
                case decimal d:
                    return d.ToString();
                case char c:
                    return $"'{(c == '\'' ? "''" : c.ToString())}'";
                case string s:
                    return $"'{s.Replace("'", "''")}'";
                case DateTime dt:
                    return $"CAST(N'{dt:yyyy-MM-dd HH:mm:ss.fff}' AS DATETIME)";
                default:
                    return "NULL";
            }
        }
    }
}
