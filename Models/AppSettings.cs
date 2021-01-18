using System;
using System.Collections.Generic;
using System.Text;

namespace Migrate.Models
{
    class AppSettings
    {
        public string Path { get; set; }
        public bool ToJSON { get; set; }
        public MigrationSettings Model { get; set; }
        public MigrationSettings Data { get; set; }
        public ConnectionStringSettings ConnectionStrings { get; set; }

        public class MigrationSettings
        {
            public ObjectConfiguration Schemas { get; set; } // specify exact schema names
            public ObjectConfiguration Tables { get; set; } // specify exact table names

            public class ObjectConfiguration
            {
                public List<string> Include { get; set; }
                public List<string> Exclude { get; set; }
            }
        }

        public class ConnectionStringSettings
        {
            public string Source { get; set; }
            public string Target { get; set; }
        }
    }
}
