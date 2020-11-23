using Microsoft.Extensions.Configuration;
using Migrate.Models;

namespace Migrate
{
    class Program
    {
        private static IConfiguration config;
        static string path = Helpers.GetAppRootDir();

        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(path)
                .AddJsonFile("appsettings.json");

            config = builder.Build();

            var settings = config.GetSection("AppSettings").Get<AppSettings>();
            var migrator = new Migrator(settings);
            migrator.Migrate();
        }
    }
}