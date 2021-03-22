using System.Collections.Generic;

namespace WebApiExample.Couchbase.Models.Db
{
    public class Person
    {
        public int BusinessEntityID { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }

        public ICollection<Email> EmailAddresses { get; set; }
    }

    public class Email
    {
        public int EmailAddressID { get; set; }
        public string EmailAddress { get; set; }
    }
}