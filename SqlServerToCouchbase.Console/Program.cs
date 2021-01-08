using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace SqlServerToCouchbase.Console
{
    class Program
    {
        static async Task Main(string[] args)
        {
            System.Console.WriteLine("Press ENTER to start migration.");
            System.Console.ReadLine();

            // UseSchemaForScope = false
            var tableNameToCollectionMapping = new Dictionary<string, string>
            {
                {"Production_ProductListPriceHistory", "Production_ProductListPrHist"},
                {"Production_ProductModelIllustration", "Production_ProductModelIllus"},
                {"Production_ProductModelProductDescriptionCulture", "Production_ProdMoProdDesCult"},
                {"Production_TransactionHistoryArchive", "Production_TransactHisArch"},
                {"HumanResources_EmployeeDepartmentHistory", "HumanResources_EmpDeptHist"},
                {"HumanResources_EmployeePayHistory", "HumanResources_EmpPayHistory"},
                {"Sales_SalesOrderHeaderSalesReason", "Sales_SalesOrderHeadSalRea"}
            };

            // UseSchemaForScope = true
            // var tableNameToCollectionMappingScoped = new Dictionary<string, string>
            // {
            //     {"ProductModelProductDescriptionCulture", "ProductModelProductDescCult"}
            // };

            // setup config object for SqlToCb
            var config = new SqlToCbConfig
            {
                SourceSqlConnectionString = "Server=localhost;Database=AdventureWorks2016;Trusted_Connection=True;",
                TargetBucket = "AdventureWorks",
                TargetBucketRamQuotaMB = 1024,
                TargetConnectionString = "couchbase://localhost",
                TargetPassword = "password",
                TargetUsername = "Administrator",
                TableNameToCollectionMapping = tableNameToCollectionMapping,
                UseSchemaForScope = false,
                UseDefaultScopeForDboSchema = true,
                DefaultPasswordForUsers = "Change*This*Password*123"
            };

            // setup DI for logging/HTTP
            var serviceProvider = new ServiceCollection()
                .AddLogging(builder => builder
                    .AddConsole()
                    .AddFilter(level => level >= LogLevel.Information)
                )
                .AddHttpClient()
                .AddSingleton<SqlToCbConfig>(config)
                .AddTransient<SqlToCb>()
                .BuildServiceProvider();

            // since this is a console app, just use locator to get a SqlToCb instance
            var convert = serviceProvider.GetService<SqlToCb>();

            // double check
            if(convert == null)
                throw new Exception("Something went wrong instantiating a SqlToCb object");

            try
            {
                await convert.Connect();

                await convert.Migrate(validateNames: true);

                await convert.Migrate(createBucket: true);
                
                System.Console.WriteLine("Bucket has been created. Press ENTER to continue.");
                System.Console.ReadLine();

                await convert.Migrate(createCollections: true);

                System.Console.WriteLine("Collections have been created. Press ENTER to continue.");
                System.Console.ReadLine();

                //await convert.Migrate(createUsers: true);

                //System.Console.WriteLine("Users have been created. Press ENTER to continue.");
                //System.Console.ReadLine();

                //await convert.Migrate(createIndexes: true, sampleForDemo: true);

                System.Console.WriteLine("Indexes have been created. Press ENTER to continue.");
                System.Console.ReadLine();

                await convert.Migrate(copyData: true, sampleForDemo: true);

                System.Console.WriteLine("Data has been copied. Press ENTER to continue.");
                System.Console.ReadLine();
            }
            finally
            {
                convert.Dispose();
            }
        }
    }
}
