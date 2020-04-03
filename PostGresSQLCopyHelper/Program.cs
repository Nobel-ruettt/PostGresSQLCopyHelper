using System;
using System.Collections.Generic;

namespace PostGresSQLCopyHelper
{
    class Program
    {
        static void Main(string[] args)
        {
            var items = new List<DboModel>();

            for(int i=0;i<=10;i++)
            {
                var item = new DboModel();
                item.Id = i.ToString();
                item.CompanyId = "A";
                item.ProjectId = "B";
                item.EffectiveDate = DateTime.UtcNow;
                item.Rate = 0.123;
                items.Add(item);
            }

            var MyDatabase = new PostgresDatabaseClient();

            MyDatabase.BulkUpsert("public", items);
        }
    }
}
