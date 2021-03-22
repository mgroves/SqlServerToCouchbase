using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace WebApiExample.SqlServer.Models.Db
{
    public class Person
    {
        [Key]
        public int BusinessEntityID { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }

        [ForeignKey("BusinessEntityID")]
        public ICollection<Email> EmailAddresses { get; set; }
    }

    public class Email
    {
        [Key]
        public int EmailAddressID { get; set; }
        public string EmailAddress { get; set; }
    }
}