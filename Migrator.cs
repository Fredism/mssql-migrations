using Migrate.Models;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.IO;
using System.Data;
using ObjectConfiguration = Migrate.Models.AppSettings.MigrationSettings.ObjectConfiguration;

namespace Migrate
{
    public enum SQLTypes
    {
        [Description("U")]
        UserTable,
        [Description("PK")]
        PrimaryKey,
        [Description("F")]
        ForeignKey,
        [Description("D")]
        DefaultConstraint,
        [Description("UQ")]
        UniqueConstraint,
        [Description("C")]
        CheckConstraint,
        [Description("FN")]
        ScalarFunction,
        [Description("IF")]
        InlineTableFunction,
        [Description("TF")]
        TableFunction,
        [Description("V")]
        View,
        [Description("P")]
        StoredProcedure,

    }

    class Migrator
    {
        private AppSettings settings;
        private string path = Helpers.GetAppRootDir();
        private readonly string sourceConn;
        private readonly string targetConn;
        private readonly string sourceDb;
        private readonly string targetDb;
        private readonly int sourceId;
        private readonly int targetId;

        private HashSet<string> modelSchemas; // schemas to model objects
        private HashSet<string> dataSchemas; // schemas to copy the data from
        private Dictionary<string, int> sourceSchemas;
        private Dictionary<string, int> targetSchemas;

        // maps qualified_name to object_id
        private Dictionary<string, string> sourceObjects;
        private Dictionary<string, string> targetObjects;

        // maps object_id
        private Dictionary<string, string> sourceTypes;
        private Dictionary<string, string> targetTypes;
        private Dictionary<string, SysTable> sourceTables;
        private Dictionary<string, SysTable> targetTables;
        private Dictionary<string, List<SysColumn>> sourceColumns;
        private Dictionary<string, List<SysColumn>> targetColumns;
        private Dictionary<string, SysForeignKey> sourceKeys;
        private Dictionary<string, SysForeignKey> targetKeys;
        private Dictionary<string, SysConstraint> sourceConstraints;
        private Dictionary<string, SysConstraint> targetConstraints;
        private Dictionary<string, SysIndex> sourceIndexes;
        private Dictionary<string, SysIndex> targetIndexes;
        private Dictionary<string, SysTableType> sourceTableTypes;
        private Dictionary<string, SysTableType> targetTableTypes;
        private Dictionary<string, SysObject> sourceProcs;
        private Dictionary<string, SysObject> targetProcs;
        private Dictionary<string, SysObject> sourceFuncs;
        private Dictionary<string, SysObject> targetFuncs;
        private Dictionary<string, SysObject> sourceViews;
        private Dictionary<string, SysObject> targetViews;

        private List<int> dataSchemaIds
        {
            get
            {
                return sourceSchemas.Keys.Where(schema => dataSchemas.Contains(schema)).Select(schema => sourceSchemas[schema]).ToList();
            }
        }
        private List<int> sourceSchemaIds
        {
            get
            {
                return sourceSchemas.Values.ToList();
            }
        }
        private List<int> targetSchemaIds
        {
            get
            {
                return targetSchemas.Values.ToList();
            }
        }
        // Takes two connection strings
        public Migrator(AppSettings config)
        {
            if(config?.ConnectionStrings == null || string.IsNullOrEmpty(config.ConnectionStrings.Source))
            {
                throw new Exception("connection string not specified");
            }
            settings = config;
            sourceConn = config.ConnectionStrings.Source;
            targetConn = config.ConnectionStrings.Target;
            sourceDb = Helpers.GetDbName(sourceConn);
            targetDb = Helpers.GetDbName(targetConn);
            if (sourceDb == null || targetDb == null) throw new Exception("source or target not specified");


            sourceId = GetDatabaseId(sourceDb, sourceConn);
            targetId = GetDatabaseId(targetDb, targetConn);

            if (sourceId == -1)
            {
                throw new Exception("source id could not be read");
            }
            else if (targetId == -1)
            {
                throw new Exception("target id could not be read");
            }

            sourceObjects = new Dictionary<string, string>();
            targetObjects = new Dictionary<string, string>();
            sourceTypes = new Dictionary<string, string>();
            targetTypes = new Dictionary<string, string>();
        }

        public void Migrate()
        {
            Load();
            Patch();
            Create();
            Alter();
            Seed();
            Update();
        }

        protected void Load()
        {
            var allSchemas = Helpers.ExecuteCommand<NameValue>(new SqlCommand("select name from sys.schemas"), sourceConn)
                    .Select(schema => schema.name).ToList();
            dataSchemas = ConfigureSchemas(settings.Data?.Schemas, allSchemas);
            modelSchemas = ConfigureSchemas(settings.Model?.Schemas, allSchemas);

            var schemas = dataSchemas.ToList();
            schemas.AddRange(modelSchemas.ToList());
            schemas = schemas.Distinct().ToList();

            if (schemas.Count() == 0) throw new Exception("no schemas to load");

            LoadSchemas(schemas);
            LoadTables();
            LoadColumns();
            LoadConstraints();
            LoadKeys();
            LoadIndexes();
            LoadTableTypes();
            LoadFunctions();
            LoadProcedures();
            LoadViews();
        }

        protected void Create()
        {
            var builder = new StringBuilder();
            builder.Append($"USE {targetDb};\n\n");

            foreach (var schema in sourceSchemas.Keys)
            {
                builder.Append(Query.CreateSchema(schema));
                builder.Append("GO\n\n");
            }
            var tables = sourceTables.Values;
            foreach (var table in tables)
            {
                builder.Append(Query.CreateTable(table, sourceColumns[table.object_id]));
                builder.Append("GO\n\n");
            }
            foreach (var index in sourceIndexes.Values)
            {
                builder.Append(Query.CreateIndex(index));
            }
            foreach (var tt in sourceTableTypes.Values)
            {
                builder.Append(Query.CreateTableType(tt));
            }
            foreach (var func in sourceFuncs.Values)
            {
                builder.Append(Query.CreateFunction(func));
            }
            foreach (var view in sourceViews.Values)
            {
                builder.Append(Query.CreateView(view));
            }
            foreach (var proc in sourceProcs.Values)
            {
                builder.Append(Query.CreateProc(proc));
            }

            File.WriteAllText($"{path}\\create.sql", builder.ToString());
        }

        protected void Alter()
        {
            var builder = new StringBuilder();
            builder.Append($"USE {targetDb};\n\n");

            // constraints
            var allConstraints = sourceConstraints.Values;
            var primaryKeys = allConstraints.Where(c => c.type == Enumerations.GetDescription(SQLTypes.PrimaryKey));
            var defaultConstraints = allConstraints.Where(c => c.type == Enumerations.GetDescription(SQLTypes.DefaultConstraint));
            var uniqueConstraints = allConstraints.Where(c => c.type == Enumerations.GetDescription(SQLTypes.UniqueConstraint));
            var checkConstraints = allConstraints.Where(c => c.type == Enumerations.GetDescription(SQLTypes.CheckConstraint));

            foreach (var key in primaryKeys)
            {
                builder.Append(Query.AddPrimaryKey(key));
                builder.Append("GO\n\n");
            }

            foreach (var constraint in defaultConstraints)
            {
                builder.Append(Query.AddDefaultConstraint(constraint));
                builder.Append("GO\n\n");
            }

            foreach (var constraint in uniqueConstraints)
            {
                builder.Append(Query.AddUniqueConstraint(constraint));
                builder.Append("GO\n\n");
            }

            foreach (var constraint in checkConstraints)
            {
                builder.Append(Query.AddCheckConstraint(constraint));
                builder.Append("GO\n\n");
            }

            // foreign keys
            foreach (var key in sourceKeys.Values)
            {
                builder.Append(Query.AddForeignKey(key));
                builder.Append("GO\n\n");
            }

            // funcs
            foreach (var func in sourceFuncs.Values)
            {
                if (!ObjectChanged(func, targetFuncs)) {
                    continue;
                }
                builder.Append(Query.AlterFunc(func));
            }
            // views
            foreach (var view in sourceViews.Values)
            {
                if (!ObjectChanged(view, targetViews))
                {
                    continue;
                }
                builder.Append(Query.AlterView(view));
            }
            // procs
            foreach (var proc in sourceProcs.Values)
            {
                if (!ObjectChanged(proc, targetProcs))
                {
                    continue;
                }
                builder.Append(Query.AlterProc(proc));
            }

            File.WriteAllText($"{path}\\alter.sql", builder.ToString());
        }

        protected void Seed()
        {
            var builder = new StringBuilder();
            builder.Append($"USE {targetDb};\n\n");

            builder.Append(Query.ToggleIdInsertForEach("OFF"));

            builder.Append(Query.ToggleConstraintForEach(false) + "\n\n");

            var tables = sourceTables.Values.Where(table => dataSchemas.Contains(table.schema_name));
            foreach (var table in tables)
            {
                var columns = sourceColumns[table.object_id];
                var rows = Helpers.ExecuteCommandToDictionary(new SqlCommand($"select * from {table.qualified_name}"), sourceConn);

                // only perform update on tables w/ primary keys
                if (columns.All(c => c.primary_key == null)) continue;
                // if nothing to seed
                else if (rows.Count() == 0) continue;

                var hasIdentity = table.has_identity != null;
                builder.Append($"-- {table.qualified_name} --\n\n");

                if (hasIdentity) builder.Append(Query.ToggleIdInsert(table.qualified_name, "ON"));
                builder.Append(Query.SeedIf(table, columns, rows));
                if (hasIdentity) builder.Append(Query.ToggleIdInsert(table.qualified_name, "OFF"));
                builder.Append("GO\n\n");
            }

            builder.Append(Query.ToggleConstraintForEach(true));

            File.WriteAllText($"{path}\\seed.sql", builder.ToString());
        }

        protected void Update()
        {
            var builder = new StringBuilder();
            builder.Append($"USE {targetDb};\n\n");

            var toUpdate = new List<SysTable>();
            var updateCmd = new SqlCommand(Query.GetLastUpdate());
            var sourceUpdates = Helpers.ExecuteCommand<DateValue>(updateCmd, sourceConn).ToDictionary(u => u.id, u => u.date);
            var targetUpdates = Helpers.ExecuteCommand<DateValue>(updateCmd, targetConn).ToDictionary(u => u.id, u => u.date);

            var tables = sourceTables.Values.Where(table => dataSchemas.Contains(table.schema_name));
            foreach (var table in tables)
            {
                if (!sourceUpdates.ContainsKey(table.object_id)) continue; // ignore this table
                else if (!targetObjects.ContainsKey(table.qualified_name)) continue; // target hasn't created this table
                else if (!targetUpdates.ContainsKey(targetObjects[table.qualified_name])) continue; // target has never been updated, should seed, update would be slow
                //else if (!targetUpdates.ContainsKey(targetObjects[table.qualified_name]))
                //{
                //    toUpdate.Add(table);
                //    continue;
                //}

                var sourceUpdate = sourceUpdates[table.object_id];
                var targetUpdate = targetUpdates[targetObjects[table.qualified_name]];

                if (targetUpdate <= sourceUpdate)
                {
                    toUpdate.Add(table);
                }
            }

            if (toUpdate.Count() > 0)
            {
                builder.Append(Query.ToggleIdInsertForEach("OFF"));

                builder.Append(Query.ToggleConstraintForEach(false) + "\n\n");
            }
            else
            {
                builder.Append("-- Nothing to update. --");
            }

            foreach (var table in toUpdate)
            {
                var columns = sourceColumns[table.object_id];
                var rows = Helpers.ExecuteCommandToDictionary(new SqlCommand($"select * from {table.qualified_name}"), sourceConn);

                // only perform update on tables w/ primary keys
                if (columns.All(c => c.primary_key == null)) continue;
                else if (rows.Count() == 0) continue;

                var hasIdentity = table.has_identity != null;
                builder.Append($"-- {table.qualified_name} --\n\n");

                if (hasIdentity) builder.Append(Query.ToggleIdInsert(table.qualified_name, "ON"));
                rows.ForEach(row =>
                {
                    builder.Append(Query.UpdateIf(table, columns, row));
                    builder.Append("\nGO\n\n");
                });
                if (hasIdentity) builder.Append(Query.ToggleIdInsert(table.qualified_name, "OFF"));
            };

            if (toUpdate.Count() > 0)
            {
                builder.Append("\n" + Query.ToggleConstraintForEach(true));
            }

            File.WriteAllText($"{path}\\update.sql", builder.ToString());
        }

        protected void Patch()
        {
            var builder = new StringBuilder();
            var opening = $"USE {targetDb};\n\n";
            builder.Append(opening);

            // drop foreign keys not in source
            var excluded = targetKeys.Values.Select(k => k.qualified_name).Except(sourceKeys.Values.Select(k => k.qualified_name));
            var toDrop = targetKeys.Values.Where(k => excluded.Contains(k.qualified_name));
            foreach (var key in toDrop)
            {
                builder.Append(Query.DropForeignKey(key));
                builder.Append("GO\n\n");
            }

            PatchColumns(builder);

            if (builder.ToString() == opening)
            {
                builder.Append("-- Nothing to patch. --");
            }

            File.WriteAllText($"{path}\\patch.sql", builder.ToString());
        }

        // drop/add/alter any columns
        private void PatchColumns(StringBuilder builder)
        {
            var source = sourceObjects.Keys.ToHashSet();
            var target = targetObjects.Keys.ToHashSet();
            var intersect = source.Intersect(target);

            foreach (var tableName in intersect)
            {
                var sourceId = sourceObjects[tableName];
                if (sourceTypes[sourceId] != Enumerations.GetDescription(SQLTypes.UserTable))
                    continue;
                var sourceCols = sourceColumns[sourceId];

                var targetId = targetObjects[tableName];
                var targetCols = targetColumns[targetId];

                var sourceColDictionary = sourceCols.ToDictionary(c => c.name);
                var targetColDictionary = targetCols.ToDictionary(c => c.name);

                var toAdd = new List<SysColumn>();
                var toAlter = new List<SysColumn>();
                var toDrop = targetCols.Where(c => !sourceColDictionary.ContainsKey(c.name)).ToList();

                foreach (var column in sourceCols)
                {
                    // target doesn't have column 
                    if (!targetColDictionary.ContainsKey(column.name))
                    {
                        toAdd.Add(column);
                    }
                    // column name exists in target, check for changes
                    else
                    {
                        var sourceColumn = column;
                        var targetColumn = targetColDictionary[column.name];
                        if (sourceColumn.type_definition != targetColumn.type_definition)
                        {
                            toAlter.Add(column);
                        }
                    }
                }

                if (toAdd.Count() > 0 || toAlter.Count() > 0 || toDrop.Count() > 0)
                {
                    builder.Append($"-- {tableName} --\n");

                    var targetObjConstraints = targetConstraints.Values.Where(c => c.parent_object_id == targetId);
                    var primaryKey = targetObjConstraints.Where(c => c.type == Enumerations.GetDescription(SQLTypes.PrimaryKey)).FirstOrDefault();

                    toDrop.ForEach(column =>
                    {
                        builder.Append(Query.DropColumn(column));
                        builder.Append("GO\n\n");
                    });
                    toAdd.ForEach(column =>
                    {
                        builder.Append(Query.AddColumn(column));
                        builder.Append("GO\n\n");
                    });
                    toAlter.ForEach(column =>
                    {
                        if(DependsOn(primaryKey, column)) {
                            builder.Append(Query.DropPrimaryKey(primaryKey));
                            builder.Append("GO\n\n");
                        }
                        builder.Append(Query.AlterColumn(column));
                        builder.Append("GO\n\n");
                    });
                }
            }
        }

        private bool DependsOn(SysConstraint constraint, SysColumn column)
        {
            if (constraint == null) return false;
            else if (constraint.columns.Contains(",") && constraint.columns.Split(",").Contains(column.name)) 
                return true;
            else if (constraint.columns == column.name) return true;

            return false;
        }

        private int GetDatabaseId(string name, string conn)
        {
            var cmd = new SqlCommand($"select database_id from sys.databases where name = '{name}'");
            var record = Helpers.ExecuteCommandToDictionary(cmd, conn).FirstOrDefault();
            if (record == null)
            {
                return -1;
            }
            return (int)record["database_id"];
        }

        private void LoadSchemas(IEnumerable<string> schemas)
        {
            var cmd = new SqlCommand($"select schema_id[id], name from sys.schemas where name in ({string.Join(", ", schemas.Select(schema => $"'{schema}'"))})");
            sourceSchemas = Helpers.ExecuteCommand<IdValue>(cmd, sourceConn).ToDictionary(s => s.name, s => s.id);
            targetSchemas = Helpers.ExecuteCommand<IdValue>(cmd, targetConn).ToDictionary(s => s.name, s => s.id);
        }

        private void LoadTables()
        {
            var sourceCmd = new SqlCommand(Query.GetTables(sourceSchemaIds));
            var targetCmd = new SqlCommand(Query.GetTables(targetSchemaIds));
            sourceTables = Helpers.ExecuteCommand<SysTable>(sourceCmd, sourceConn).ToDictionary(t => t.object_id);
            targetTables = Helpers.ExecuteCommand<SysTable>(targetCmd, targetConn).ToDictionary(t => t.object_id);

            foreach (var table in sourceTables.Values)
            {
                sourceObjects[table.qualified_name] = table.object_id;
                sourceTypes[table.object_id] = Enumerations.GetDescription(SQLTypes.UserTable);
            }
            foreach (var table in targetTables.Values)
            {
                targetObjects[table.qualified_name] = table.object_id;
                targetTypes[table.object_id] = Enumerations.GetDescription(SQLTypes.UserTable);
            }
        }

        private void LoadColumns()
        {
            var sourceCmd = new SqlCommand(Query.GetColumns(sourceSchemaIds));
            var targetCmd = new SqlCommand(Query.GetColumns(targetSchemaIds));
            var allSourceColumns = Helpers.ExecuteCommand<SysColumn>(sourceCmd, sourceConn);
            var allTargetColumns = Helpers.ExecuteCommand<SysColumn>(targetCmd, targetConn);

            sourceColumns = allSourceColumns.GroupBy(c => c.object_id).ToDictionary(c => c.Key, c => c.ToList());
            targetColumns = allTargetColumns.GroupBy(c => c.object_id).ToDictionary(c => c.Key, c => c.ToList());
        }

        private void LoadKeys()
        {
            var sourceCmd = new SqlCommand(Query.GetForeignKeys(sourceSchemaIds));
            var targetCmd = new SqlCommand(Query.GetForeignKeys(targetSchemaIds));

            sourceKeys = Helpers.ExecuteCommand<SysForeignKey>(sourceCmd, sourceConn).ToDictionary(k => k.constraint_id);
            targetKeys = Helpers.ExecuteCommand<SysForeignKey>(targetCmd, targetConn).ToDictionary(k => k.constraint_id);

            foreach (var key in sourceKeys.Values)
            {
                sourceObjects[key.qualified_name] = key.constraint_id;
                sourceTypes[key.constraint_id] = Enumerations.GetDescription(SQLTypes.ForeignKey);
            }
            foreach (var key in targetKeys.Values)
            {
                targetObjects[key.qualified_name] = key.constraint_id;
                targetTypes[key.constraint_id] = Enumerations.GetDescription(SQLTypes.ForeignKey);
            }
        }

        private void LoadIndexes()
        {
            var sourceCmd = new SqlCommand(Query.GetIndexes(sourceSchemaIds));
            var targetCmd = new SqlCommand(Query.GetIndexes(targetSchemaIds));
            sourceIndexes = Helpers.ExecuteCommand<SysIndex>(sourceCmd, sourceConn).ToDictionary(p => p.object_id + p.index_id);
            targetIndexes = Helpers.ExecuteCommand<SysIndex>(targetCmd, targetConn).ToDictionary(p => p.object_id + p.index_id);
        }

        private void LoadTableTypes()
        {
            var sourceCmd = new SqlCommand(Query.GetTableTypes(sourceSchemaIds));
            var targetCmd = new SqlCommand(Query.GetTableTypes(targetSchemaIds));
            sourceTableTypes = Helpers.ExecuteCommand<SysTableType>(sourceCmd, sourceConn).ToDictionary(p => p.object_id);
            targetTableTypes = Helpers.ExecuteCommand<SysTableType>(targetCmd, targetConn).ToDictionary(p => p.object_id);
        }

        private void LoadConstraints()
        {
            var sourceCmd = new SqlCommand(Query.GetConstraints(sourceSchemaIds));
            var targetCmd = new SqlCommand(Query.GetConstraints(targetSchemaIds));
            sourceConstraints = Helpers.ExecuteCommand<SysConstraint>(sourceCmd, sourceConn).ToDictionary(p => p.object_id);
            targetConstraints = Helpers.ExecuteCommand<SysConstraint>(targetCmd, targetConn).ToDictionary(p => p.object_id);
        }

        private void LoadFunctions()
        {
            var sourceCmd = new SqlCommand(Query.GetFunctions(sourceSchemaIds));
            var targetCmd = new SqlCommand(Query.GetFunctions(targetSchemaIds));
            sourceFuncs = Helpers.ExecuteCommand<SysObject>(sourceCmd, sourceConn).ToDictionary(p => p.object_id);
            targetFuncs = Helpers.ExecuteCommand<SysObject>(targetCmd, targetConn).ToDictionary(p => p.object_id);
            MapSourceObjects(sourceFuncs.Values);
            MapTargetObjects(targetFuncs.Values);
        }

        private void LoadProcedures()
        {
            var cmd = new SqlCommand(Query.GetStoredProcedures());
            sourceProcs = Helpers.ExecuteCommand<SysObject>(cmd, sourceConn).ToDictionary(p => p.object_id);
            targetProcs = Helpers.ExecuteCommand<SysObject>(cmd, targetConn).ToDictionary(p => p.object_id);
            MapSourceObjects(sourceProcs.Values);
            MapTargetObjects(targetProcs.Values);
        }

        private void LoadViews()
        {
            var sourceCmd = new SqlCommand(Query.GetViews(sourceSchemaIds));
            var targetCmd = new SqlCommand(Query.GetViews(targetSchemaIds));
            sourceViews = Helpers.ExecuteCommand<SysObject>(sourceCmd, sourceConn).ToDictionary(p => p.object_id);
            targetViews = Helpers.ExecuteCommand<SysObject>(targetCmd, targetConn).ToDictionary(p => p.object_id);
            MapSourceObjects(sourceViews.Values);
            MapTargetObjects(targetViews.Values);
        }

        private HashSet<string> ConfigureSchemas(ObjectConfiguration configuration, List<string> allSchemas)
        {
            if (configuration == null)
            {
                return new HashSet<string>();
            }
            else
            {
                var schemas = new List<string>();
                if (configuration.Include != null)
                {
                    schemas.AddRange(configuration.Include);
                }
                else if (configuration.Exclude != null)
                {
                    schemas.AddRange(allSchemas.Except(configuration.Exclude));
                }
                return schemas.ToHashSet();
            }
        }

        private void MapSourceObjects(IEnumerable<SysObject> objects)
        {
            MapObjects(objects, sourceObjects, sourceTypes);
        }

        private void MapTargetObjects(IEnumerable<SysObject> objects)
        {
            MapObjects(objects, targetObjects, targetTypes);
        }

        private void MapObjects(IEnumerable<SysObject> objects, Dictionary<string, string> objectDict, Dictionary<string, string> typeDict)
        {
            foreach (var @object in objects)
            {
                objectDict[@object.qualified_name] = @object.object_id;
                typeDict[@object.object_id] = @object.type;
            }
        }

        private bool ObjectChanged(SysObject @object, Dictionary<string, SysObject> targetSet)
        {
            if (!targetSet.ContainsKey(@object.qualified_name))
            {
                return false;
            }
            else
            {
                var targetObject = targetSet[@object.qualified_name];
                return targetObject.modify_date.HasValue && @object.modify_date > targetObject.modify_date;
            }
        }
    }
}
