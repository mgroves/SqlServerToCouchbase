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

            var config = new SqlToCbConfig
            {
                SourceSqlConnectionString = "Server=localhost;Database=AdventureWorks2016;Trusted_Connection=True;",
                TargetBucket = "AdventureWorks",
                TargetBucketRamQuotaMB = 1024,
                TargetConnectionString = "couchbase://localhost",
                TargetPassword = "password",
                TargetUsername = "Administrator",
                TableNameToCollectionMapping = tableNameToCollectionMapping
            };

            var logger = GetConsoleLogger();
            var convert = new SqlToCb(config, logger);

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

                await convert.Migrate(createIndexes: true);

                System.Console.WriteLine("Indexes have been created. Press ENTER to continue.");
                System.Console.ReadLine();

                await convert.Migrate(copyData: true);
            }
            finally
            {
                convert.Dispose();
            }
        }

        private static ILogger GetConsoleLogger()
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddLogging(builder => builder
                .AddConsole()
                .AddFilter(level => level >= LogLevel.Information)
            );
            var loggerFactory = serviceCollection.BuildServiceProvider().GetService<ILoggerFactory>();
            return loggerFactory.CreateLogger<Program>();
        }
    }
}
