using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using WebApiExample.SqlServer.Models.Api;
using WebApiExample.SqlServer.Models.Db;

namespace WebApiExample.SqlServer.Controllers
{
    public class PersonController : Controller
    {
        private readonly AdventureWorksContext _context;

        public PersonController(AdventureWorksContext context)
        {
            _context = context;
        }

        [HttpGet("/persons/page/{pageNum}")]
        public async Task<IActionResult> GetPersonsPageAsync(int pageNum)
        {
            var pageSize = 10;
            var personPage = await _context.Persons
                .OrderBy(p => p.LastName)
                .Skip(pageNum * pageSize)
                .Take(pageSize)
                .Select(p => new { p.BusinessEntityID, p.FirstName, p.LastName })
                .ToListAsync();
            return Ok(personPage);
        }

        [HttpGet("/person/{personId}")]
        public async Task<IActionResult> GetPersonByIdAsync(int personId)
        {
            var person = await _context.Persons
                .SingleOrDefaultAsync(p => p.BusinessEntityID == personId);

            return Ok(person);
        }


        [HttpGet("/personRaw/{personId}")]
        public async Task<IActionResult> GetPersonByIdRawAsync(int personId)
        {
            var person = await _context.Persons
                .FromSqlRaw(@"SELECT * FROM Person.Person WHERE BusinessEntityID = {0}", personId)
                .SingleOrDefaultAsync();

            return Ok(person);
        }
        
        [HttpGet("/personExpanded/{personId}")]
        public async Task<IActionResult> GetPersonByIdExpandedAsync(int personId)
        {
            var person = await _context.Persons
                //.Include(p => p.Addresses)
                .Include(p => p.EmailAddresses)
                .SingleOrDefaultAsync(p => p.BusinessEntityID == personId);
            
            return Ok(person);
        }

        [HttpPut("/person")]
        public async Task<IActionResult> UpdatePurchaseOrderAsync(PersonUpdateApi personUpdateApi)
        {
            var transaction = await _context.Database.BeginTransactionAsync();

            try
            {
                // find the person
                var person = await _context.Persons
                    .Include(p => p.EmailAddresses)
                    .SingleOrDefaultAsync(p => p.BusinessEntityID == personUpdateApi.PersonId);

                // update name
                person.FirstName = personUpdateApi.FirstName;
                person.LastName = personUpdateApi.LastName;

                // get the particular email address and update it
                // if the supplied ID is invalid, this will throw an exception
                var email = person.EmailAddresses.Single(e =>
                    e.EmailAddressID == personUpdateApi.EmailAddressId);
                email.EmailAddress = personUpdateApi.EmailAddress;

                await _context.SaveChangesAsync();

                // commit transaction
                await transaction.CommitAsync();

                return Ok($"Person {personUpdateApi.PersonId} name and email updated.");
            }
            catch (Exception ex)
            {
                await transaction.RollbackAsync();
                return BadRequest("Something went wrong, transaction rolled back");
            }
        }

        // sproc example - see ExampleStoredProcedure.sql
        [HttpGet("/getListSubcomponents/{listPriceMin}/{listPriceMax}")]
        public async Task<IActionResult> GetListSubcomponents(decimal listPriceMin, decimal listPriceMax)
        {
            var listPriceMinParam = new SqlParameter("@ListPriceMin", SqlDbType.Decimal) {Value = listPriceMin };
            var listPriceMaxParam = new SqlParameter("@ListPriceMax", SqlDbType.Decimal) {Value = listPriceMax };

            var result = await _context.ListSubcomponents
                .FromSqlRaw("EXECUTE dbo.ListSubcomponents @ListPriceMin, @ListPriceMax", listPriceMinParam, listPriceMaxParam)
                .ToListAsync();

            return Ok(result);
        }
    }
}