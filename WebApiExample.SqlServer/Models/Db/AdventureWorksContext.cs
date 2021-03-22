using Microsoft.EntityFrameworkCore;

namespace WebApiExample.SqlServer.Models.Db
{
    public class AdventureWorksContext : DbContext
    {
        /// <summary>
        /// A limited view of the Person entity
        /// </summary>
        public DbSet<Person> Persons { get; set; }

        /// <summary>
        /// ListSubcomponents DbSet is only for sproc purposes
        /// Do not attempt to write to it
        /// </summary>
        public DbSet<ListSubcomponent> ListSubcomponents { get; set; }

        public AdventureWorksContext(DbContextOptions<AdventureWorksContext> options) : base(options)
        {
            
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Person>().ToTable("Person", "Person")
                .HasKey("BusinessEntityID");
            modelBuilder.Entity<Email>().ToTable("EmailAddress", "Person").HasKey("EmailAddressID");
        }
    }
}