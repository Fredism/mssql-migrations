using System;
using System.Collections.Generic;

namespace Migrate.Models
{
    class DbModel
    {
        public class DbInfo
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }
        public DbInfo Database { get; set; }
        public IDictionary<string, int> Schemas { get; set; }
        public IDictionary<string, SysTable> Tables { get; set; }
        public IDictionary<string, List<SysColumn>> Columns { get; set; }
        public IDictionary<string, SysForeignKey> ForeignKeys { get; set; }
        public IDictionary<string, SysConstraint> Constraints { get; set; }
        public IDictionary<string, SysIndex> Indexes { get; set; }
        public IDictionary<string, SysTableType> TableTypes { get; set; }
        public IDictionary<string, SysObject> Procedures { get; set; }
        public IDictionary<string, SysObject> Functions { get; set; }
        public IDictionary<string, SysObject> Views { get; set; }
    }
}
