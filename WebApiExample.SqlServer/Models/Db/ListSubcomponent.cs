using System.ComponentModel.DataAnnotations;

namespace WebApiExample.SqlServer.Models.Db
{
    // this is just for stored procedure results
    // it's not an actual entity for writing purposes
    public class ListSubcomponent
    {
        [Key]
        public int ProductID { get; set; }
        public string Name { get; set; }
        public string ProductNumber { get; set; }
        public bool FinishedGoodsFlag { get; set; }
        public decimal ListPrice { get; set; }
    }
}