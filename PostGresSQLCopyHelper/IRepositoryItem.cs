using System;
using System.Collections.Generic;
using System.Text;

namespace PostGresSQLCopyHelper
{
    public interface IRepositoryItem
    {
            string GetId();
            string CompanyId { get; set; }
            string ProjectId { get; set; }
    }
}
