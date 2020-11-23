using System;
using System.Collections.Generic;
using System.Text;

namespace Migrate.Models
{
    class SysForeignKey
    {
        public string constraint_id { get; set; }
        public string constraint_name { get; set; }
        public string schema_name { get; set; }
        public string parent_object_id { get; set; }
        public string parent_object_name { get; set; }
        public string parent_column { get; set; }
        public string referenced_object_id { get; set; }
        public string referenced_schema_name { get; set; }
        public string referenced_object_name { get; set; }
        public string referenced_column { get; set; }
        public string is_not_trusted { get; set; }

        public string qualified_name
        {
            get
            {
                return $"[{schema_name}].[{constraint_name}]";
            }
        }

        public string qualified_parent_table
        {
            get
            {
                return $"[{schema_name}].[{parent_object_name}]";
            }
        }

        public string qualified_referenced_table
        {
            get
            {
                return $"[{referenced_schema_name}].[{referenced_object_name}]";

            }
        }

        public string check_status
        {
            get
            {
                return is_not_trusted == "0" ? "CHECK" : "NOCHECK";
            }
        }
    }
}
