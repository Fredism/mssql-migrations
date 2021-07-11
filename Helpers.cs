using Migrate.Models;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace Migrate
{
    static class Enumerations
    {
        public static string GetDescription(Enum value)
        {
            return
                value
                    .GetType()
                    .GetMember(value.ToString())
                    .FirstOrDefault()
                    ?.GetCustomAttribute<DescriptionAttribute>()
                    ?.Description
                ?? value.ToString();
        }

        public static string Description(this Enum value)
        {
            return GetDescription(value);
        }
    }
    class Helpers
    {
        public static Dictionary<string, int> GetUpdateOrder(IEnumerable<int> schemas, string connString)
        {
            var tables = ExecuteCommand<SysTable>(new SqlCommand(Query.GetTables(schemas)), connString);
            var fkeys = ExecuteCommand<SysForeignKey>(new SqlCommand(Query.GetForeignKeys(schemas)), connString);

            var counts = fkeys.GroupBy(f => f.parent_object_name).OrderBy(g => g.Count()).ToDictionary(g => g.Key, g => g.Count());
            var counted = tables.ToDictionary(t => t.name, t => false);

            var weights = new Dictionary<string, int>();
            var leftovers = new List<string>();
            tables.OrderBy(t => !counts.ContainsKey(t.name) ? 0 : counts[t.name]).Select(t => t.name).ToList().ForEach(table =>
            {
                var tfkeys = fkeys.Where(f => f.parent_object_name == table).ToList();
                if (tfkeys.Count() > 0)
                {
                    if (tfkeys.Any(f => !weights.ContainsKey(f.referenced_object_name))) // this object has not been counted
                    {
                        var weight = 1;
                        tfkeys.ForEach(f =>
                        {
                            if (!weights.ContainsKey(f.referenced_object_name)) return;
                            weight += weights[f.referenced_object_name];
                        });
                        weights[table] = weight;
                        leftovers.Add(table);
                        return;
                    }
                    else
                    {
                        var weight = 1;
                        tfkeys.ForEach(f => weight += weights[f.referenced_object_name]);
                        weights[table] = weight;
                    }
                }
                else
                {
                    weights[table] = 1;
                }
                counted[table] = true;
            });

            leftovers.Reverse();
            leftovers.ForEach(table =>
            {
                var tfkeys = fkeys.Where(f => f.parent_object_name == table && counted.ContainsKey(f.referenced_object_name)).ToList();
                var weight = 1;
                tfkeys.ForEach(f =>
                {
                    weight += weights[f.referenced_object_name];
                });
                weights[table] = weight;
                counted[table] = true;
            });

            return weights;
        }

        public static string GetDbName(string connString)
        {
            var match = Regex.Match(connString, "Initial Catalog=(\\w+)");
            if (!match.Success) return null;
            return match.Captures.First().Value.Split('=')[1];
        }

        public static string GetAppRootDir()
        {
            return AppDomain.CurrentDomain.BaseDirectory.Split("\\bin")[0];
        }


        public static List<Dictionary<string, object>> ExecuteCommandToDictionary(SqlCommand cmd, string sConn)
        {
            List<Dictionary<string, object>> res = new List<Dictionary<string, object>>();

            using (var connection = new SqlConnection(sConn))
            {
                connection.Open();
                cmd.Connection = connection;
                var oDr = cmd.ExecuteReader();

                while (oDr.Read())
                {
                    var t = new Dictionary<string, object>();

                    for (int inc = 0; inc < oDr.FieldCount; inc++)
                    {
                        t[oDr.GetName(inc)] = oDr.GetValue(inc);
                    }
                    res.Add(t);
                }
                oDr.Close();

            }
            return res;
        }
        public static List<T> ExecuteCommand<T>(SqlCommand cmd, string sConn) where T : new()
        {
            List<T> res = new List<T>();

            using (var connection = new SqlConnection(sConn))
            {
                connection.Open();
                cmd.Connection = connection;
                var oDr = cmd.ExecuteReader();

                while (oDr.Read())
                {
                    T t = new T();
                    Type type = t.GetType();

                    for (int inc = 0; inc < oDr.FieldCount; inc++)
                    {
                        if (type.IsPrimitive)
                        {
                            t = (T)oDr.GetValue(0);
                            break;
                        }

                        PropertyInfo prop = type.GetProperty(oDr.GetName(inc));

                        if (prop != null)
                        {
                            var val = oDr.GetValue(inc);

                            if (val != null && !val.ToString().Equals(""))
                            {
                                var targetType = prop.PropertyType.IsGenericType && prop.PropertyType.GetGenericTypeDefinition().Equals(typeof(Nullable<>))
                                    ? Nullable.GetUnderlyingType(prop.PropertyType) : prop.PropertyType;
                                val = Convert.ChangeType(val, targetType);
                                prop.SetValue(t, val, null);
                            }
                        }
                    }
                    res.Add(t);
                }
                oDr.Close();

            }
            return res;
        }
    }
}
