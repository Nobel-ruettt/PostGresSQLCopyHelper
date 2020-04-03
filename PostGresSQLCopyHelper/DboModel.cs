using System;
using System.Collections.Generic;
using System.Text;

namespace PostGresSQLCopyHelper
{
    public class DboModel : IRepositoryItem
    {
        public string Id { get; set; }
        public DateTime EffectiveDate { get; set; }
        public double Rate { get; set; }
        public string CompanyId { get ; set ; }
        public string ProjectId { get ; set ; }
        public  string GetId()
        {
            return this.Id;
        }
    }
}
