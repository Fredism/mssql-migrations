using System;
using System.Collections.Generic;
using System.Text;

namespace Migrate.Models
{
    class SysObject
    {
        public string name { get; set; }
        public string object_id { get; set; }
        public string object_definition { get; set; }
        public string schema_id { get; set; }
        public string schema_name { get; set; }
        public string type { get; set; }
        public DateTime create_date { get; set; }
        public DateTime? modify_date { get; set; }

        public string qualified_name
        {
            get
            {
                return $"[{schema_name}].[{name}]";
            }
        }
    }
}
