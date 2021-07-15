using System;

namespace Migrate.Models
{
    class SysColumn
    {
        public static SysColumn DeepClone(SysColumn og)
        {
            SysColumn clone = new SysColumn();
            clone.name = og.name;
            clone.object_id = og.object_id;
            clone.object_name = og.object_name;
            clone.schema_id = og.schema_id;
            clone.schema_name = og.schema_name;
            clone.data_type = og.data_type;
            clone.max_length = og.max_length;
            clone.precision = og.precision;
            clone.scale = og.scale;
            clone.is_nullable = og.is_nullable;
            clone.is_identity = og.is_identity;
            clone.is_computed = og.is_computed;
            clone.definition = og.definition;
            clone.primary_key = og.primary_key;
            clone.index_type_desc = og.index_type_desc;
            clone.generated_always_type = og.generated_always_type;
            clone.default_constraint_name = og.default_constraint_name;
            clone.default_constraint_definition = og.default_constraint_definition;

            return clone;
        }
        public string name { get; set; }
        public string object_id { get; set; }
        public string object_name { get; set; }
        public string schema_id { get; set; }
        public string schema_name { get; set; }
        public string data_type { get; set; }
        public string max_length { get; set; }
        public string precision { get; set; }
        public string scale { get; set; }
        public string is_nullable { get; set; }
        public string is_identity { get; set; }
        public string is_computed { get; set; }
        public string definition { get; set; }
        public string primary_key { get; set; }
        public string index_type_desc { get; set; }
        public string generated_always_type { get; set; }
        public string default_constraint_name { get; set; }
        public string default_constraint_definition { get; set; }

        public string qualified_table_name
        {
            get
            {
                return $"[{schema_name}].[{object_name}]";
            }
        }

        public string type_definition
        {
            get
            {
                bool hasLength = data_type.Contains("char") || data_type == "datetime2" || data_type == "varbinary";
                bool hasPrecision = data_type == "decimal" || data_type == "numeric" || data_type == "float";
                bool hasScale = hasPrecision && data_type != "float";
                string maxLength = max_length == "-1" ? "max" : max_length;
                string size =
                    hasLength && !string.IsNullOrEmpty(maxLength) ? $"({maxLength})" :
                    hasPrecision ? hasPrecision && hasScale ? $"({precision}, {scale})" : $"({precision})" :
                    string.Empty;
                return $"[{data_type}]" + size;
            }
        }

        public string generability
        {

            get
            {
                switch(generated_always_type)
                {
                    case "1": return "GENERATED ALWAYS AS ROW START ";
                    case "2": return "GENERATED ALWAYS AS ROW END ";
                    default: return "";
                }
            }
        }

        public bool isNullable
        {
            get { return Convert.ToBoolean(is_nullable); }
        }

        public string nullability
        {
            get
            {
                return (isNullable ? "" : "NOT ") + "NULL";
            }
        }

        public string identity_definition
        {
            get
            {
                return Convert.ToBoolean(is_identity) ? "IDENTITY(1, 1) " : "";
            }
        }

        public string constraint_definition
        {
            get
            {
                return default_constraint_name == null ? "" : $" CONSTRAINT [{default_constraint_name}] DEFAULT {default_constraint_definition}";
            }
        }

        public string column_definition
        {
            get
            {
                if (Convert.ToBoolean(is_computed)) return $"[{name}] AS {definition}";
                return $"[{name}] {type_definition} {identity_definition}{generability}{nullability}{constraint_definition}";
            }
        }
    }
}
