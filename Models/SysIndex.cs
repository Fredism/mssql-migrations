using System;
using System.Collections.Generic;
using System.Text;

namespace Migrate.Models
{
    class SysIndex
    {
        public string name { get; set; }
        public string object_id { get; set; }
        public string index_id { get; set; }
        public string schema_name { get; set; }
        public string table_name { get; set; }
        public string type { get; set; }
        public string type_desc { get; set; }
        public string columns { get; set; }
        public string include { get; set; }

        public string qualified_table_name
        {
            get
            {
                return $"[{schema_name}].[{table_name}]";
            }
        }
    }
}
