using System;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Migrate.Models;


namespace Migrate
{
    class Program
    {
        private static IConfiguration config;
        static string rootDir = Helpers.GetAppRootDir();

        static void Main(string[] args)
        {
            var cmdLineArgs = new ConfigurationBuilder().AddCommandLine(args).Build()
                .AsEnumerable()
                .ToDictionary(pair => pair.Key.Replace("-", ""), pair => pair.Value);

            var jsonFile = cmdLineArgs.ContainsKey("config")? cmdLineArgs["config"]: "appsettings.json";
            
            var builder = new ConfigurationBuilder()
                .SetBasePath(rootDir)
                .AddJsonFile(jsonFile);

            config = builder.Build();

            var settings = config.GetSection("AppSettings").Get<AppSettings>();

            if (!string.IsNullOrEmpty(settings.Path))
            {
                if (!Path.IsPathRooted(settings.Path))
                {
                    settings.Path = Path.GetFullPath(rootDir + settings.Path);
                }
                else
                {
                    settings.Path = Path.GetFullPath(settings.Path);
                }
            }

            var migrator = new Migrator(settings);
            migrator.Migrate();
        }
    }
}