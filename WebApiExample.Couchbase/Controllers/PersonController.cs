using System;
using System.Linq;
using System.Threading.Tasks;
using Couchbase.Extensions.DependencyInjection;
using Couchbase.KeyValue;
using Couchbase.Query;
using Couchbase.Transactions;
using Couchbase.Transactions.Config;
using Microsoft.AspNetCore.Mvc;
using WebApiExample.Couchbase.Models.Api;
using WebApiExample.Couchbase.Models.Db;

namespace WebApiExample.Couchbase.Controllers
{
    public class PersonController : Controller
    {
        private readonly INamedBucketProvider _bucketProvider;

        public PersonController(INamedBucketProvider bucketProvider)
        {
            _bucketProvider = bucketProvider;
        }

        [HttpGet("/persons/page/{pageNum}")]
        public async Task<IActionResult> GetPersonsPageAsync(int pageNum)
        {
            var pageSize = 10;
            var bucket = await _bucketProvider.GetBucketAsync();
            var bucketName = bucket.Name;
            var cluster = bucket.Cluster;

            var personPage = await cluster.QueryAsync<Person>($@"
                SELECT p.LastName, p.BusinessEntityID, p.FirstName
                FROM `{bucketName}`.Person.Person p
                WHERE p.LastName IS NOT MISSING
                ORDER BY p.LastName
                LIMIT {pageSize} OFFSET {(pageNum * pageSize)}
            ");

            return Ok(await personPage.Rows.ToListAsync());
        }

        [HttpGet("/person/{personId}")]
        public async Task<IActionResult> GetPersonByIdAsync(int personId)
        {
            var bucket = await _bucketProvider.GetBucketAsync();
            var scope = await bucket.ScopeAsync("Person");
            var coll = await scope.CollectionAsync("Person");
            var personDoc = await coll.GetAsync(personId.ToString());
            return Ok(personDoc.ContentAs<Person>());
        }


        [HttpGet("/personRaw/{personId}")]
        public async Task<IActionResult> GetPersonByIdRawAsync(int personId)
        {
            var bucket = await _bucketProvider.GetBucketAsync();
            var cluster = bucket.Cluster;
            var bucketName = bucket.Name;
            var personResult = await cluster.QueryAsync<Person>($@"
                SELECT p.* FROM `{bucketName}`.Person.Person p WHERE p.BusinessEntityID = $personId",
                new QueryOptions().Parameter("personId", personId));
            return Ok(await personResult.Rows.SingleOrDefaultAsync());
        }

        [HttpGet("/personExpanded/{personId}")]
        public async Task<IActionResult> GetPersonByIdExpandedAsync(int personId)
        {
            var bucket = await _bucketProvider.GetBucketAsync();
            var cluster = bucket.Cluster;
            var bucketName = bucket.Name;
            var personResult = await cluster.QueryAsync<Person>($@"
                SELECT p.*, EmailAddresses
                FROM `{bucketName}`.Person.Person p
                LEFT NEST `{bucketName}`.Person.EmailAddress EmailAddresses ON EmailAddresses.BusinessEntityID = p.BusinessEntityID
                WHERE p.BusinessEntityID = $personId",
                new QueryOptions().Parameter("personId", personId));
            return Ok(await personResult.Rows.SingleOrDefaultAsync());
        }

        [HttpPut("/person")]
        public async Task<IActionResult> UpdatePurchaseOrderAsync(PersonUpdateApi personUpdateApi)
        {
            // setup bucket, cluster, and collections
            var bucket = await _bucketProvider.GetBucketAsync();
            var scope = await bucket.ScopeAsync("Person");
            var personColl = await scope.CollectionAsync("Person");
            var emailColl = await scope.CollectionAsync("EmailAddress");

            // create transaction
            var cluster = bucket.Cluster;
            var transaction = Transactions.Create(cluster,
                TransactionConfigBuilder.Create()
                    .DurabilityLevel(DurabilityLevel.None)
                    .Build());

            try
            {
                await transaction.RunAsync(async (context) =>
                {
                    // update person and email documents
                    // based on values passed in API object
                    var personKey = personUpdateApi.PersonId.ToString();
                    var emailKey = personKey + "::" + personUpdateApi.EmailAddressId.ToString();
                    var person = await context.GetAsync(personColl, personKey);
                    var email = await context.GetAsync(emailColl, emailKey);

                    var personDoc = person.ContentAs<dynamic>();
                    var emailDoc = email.ContentAs<dynamic>();

                    personDoc.FirstName = personUpdateApi.FirstName;
                    personDoc.LastName = personUpdateApi.LastName;
                    emailDoc.EmailAddress = personUpdateApi.EmailAddress;

                    await context.ReplaceAsync(person, personDoc);
                    await context.ReplaceAsync(email, emailDoc);
                });
                return Ok($"Person {personUpdateApi.PersonId} name and email updated.");
            }
            catch (Exception ex)
            {
                return BadRequest("Something went wrong, transaction rolled back.");
            }
        }

        // sproc example - see ExampleStoredProcedure.n1ql
        [HttpGet("/getListSubcomponents/{listPriceMin}/{listPriceMax}")]
        public async Task<IActionResult> GetListSubcomponents(decimal listPriceMin, decimal listPriceMax)
        {
            var bucket = await _bucketProvider.GetBucketAsync();
            var cluster = bucket.Cluster;

            var options = new QueryOptions();
            options.Parameter("$listPriceMin", listPriceMin);
            options.Parameter("$listPriceMax", listPriceMax);

            var result = await cluster.QueryAsync<ListSubcomponent>(
                "SELECT l.* FROM ListSubcomponents($listPriceMin, $listPriceMax) l", options);

            return Ok(await result.Rows.ToListAsync());
        }
    }
}