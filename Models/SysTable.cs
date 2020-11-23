using System;
using System.Collections.Generic;
using System.Text;

namespace Migrate.Models
{
    class SysTable
    {
        public string name { get; set; }
        public string object_id { get; set; }
        public string schema_id { get; set; }
        public string schema_name { get; set; }
        public string has_identity { get; set; }
        public string history_table { get; set; }

        public string qualified_name
        {
            get
            {
                return $"[{schema_name}].[{name}]";
            }
        }
    }
}
