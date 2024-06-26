using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace functions.dto
{
    public class Connection
    {
        private string connectionString;

        public Connection(String databaseSelection)
        {
            var configuration = new ConfigurationBuilder()
                       .SetBasePath(Directory.GetCurrentDirectory())
                       .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                       .AddEnvironmentVariables()
                       .Build();

            // Lê os valores de configuração
            // string server = configuration["ConfDB:Server"];
            // string database = configuration["ConfDB:Database"];
            // string integratedSecurity = configuration["ConfDB:IntegratedSecurity"];

            // Constrói a connection string
            // connectionString = $"Server={server};Database={database};Integrated Security={integratedSecurity};";
            connectionString = configuration.GetConnectionString(databaseSelection);
        }

        public SqlConnection GetConnection()
        {
            return new SqlConnection(connectionString);
        }
    }
}
