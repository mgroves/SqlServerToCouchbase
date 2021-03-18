namespace WebApiExample.Couchbase.Models.Db
{
    // this is just for UDF results
    // it's not an object to be used for writing to the DB
    public class ListSubcomponent
    {
        public int ProductID { get; set; }
        public string Name { get; set; }
        public string ProductNumber { get; set; }
        public bool FinishedGoodsFlag { get; set; }
        public decimal ListPrice { get; set; }
    }
}