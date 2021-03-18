namespace WebApiExample.Couchbase.Models.Api
{
    // API model used to update firstname, lastname, email of
    // a person entity
    public class PersonUpdateApi
    {
        public int PersonId { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public int EmailAddressId { get; set; }
        public string EmailAddress { get; set; }
    }
}