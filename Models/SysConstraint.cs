using System;
using System.Collections.Generic;
using System.Text;

namespace Migrate.Models
{
    class SysConstraint
    {
        public string name { get; set; }
        public string object_id { get; set; }
        public string parent_object_id { get; set; }
        public string schema_name { get; set; }
        public string table_name { get; set; }
        public string type { get; set; }
        public string columns { get; set; }
        public string definition { get; set; }
        public string is_not_trusted { get; set; }

        public string check_status { 
            get
            {
                return (is_not_trusted == "1" ? "NO" : "") + "CHECK";
            }
        }

        public string qualified_name
        {
            get
            {
                return $"[{schema_name}].[{name}]";
            }
        }
        public string qualified_table_name
        {
            get
            {
                return $"[{schema_name}].[{table_name}]";
            }
        }
    }
}
