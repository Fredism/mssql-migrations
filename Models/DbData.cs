using System;
using System.Collections.Generic;
using System.Text;

namespace Migrate.Models
{
    class RowCollection : List<Dictionary<string, string>> { }
    class DataDictionary: Dictionary<string, RowCollection> { }
    class DbData
    {
        public DbData()
        {
            Seed = new DataDictionary();
            Update = new DataDictionary();
        }
        public DataDictionary Seed { get; set; }
        public DataDictionary Update { get; set; }
    }
}
