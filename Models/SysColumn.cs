using System;
using System.Collections.Generic;
using System.Text;

namespace Migrate.Models
{
    class SysColumn
    {
        public string name { get; set; }
        public string object_id { get; set; }
        public string object_name { get; set; }
        public string schema_id { get; set; }
        public string schema_name { get; set; }
        public string data_type { get; set; }
        public string max_length { get; set; }
        public string is_nullable { get; set; }
        public string is_identity { get; set; }
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
                bool hasLength = data_type.Contains("varchar") || data_type == "datetime2";
                string maxLength = max_length == "-1" ? "max" : max_length;
                return $"[{data_type}]" + (!string.IsNullOrEmpty(maxLength) && hasLength ? $"({maxLength})" : "");
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

        public string nullability
        {
            get
            {
                bool isNullable = Convert.ToBoolean(is_nullable);
                return (isNullable ? "" : "NOT ") + "NULL";
            }
        }

        public string identity_definition
        {
            get
            {
                bool isIdentity = Convert.ToBoolean(is_identity);
                return isIdentity ? "IDENTITY(1, 1) " : "";
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
                return $"[{name}] {type_definition} {identity_definition}{generability}{nullability}{constraint_definition}";
            }
        }
    }
}
