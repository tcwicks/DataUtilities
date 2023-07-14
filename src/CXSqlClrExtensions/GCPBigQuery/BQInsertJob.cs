using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CXSqlClrExtensions.GCPBigQuery
{
    internal class BQInsertJob
    {
        public BQInsertJob(List<BigQueryInsertRow> _InsertRows)
        {
            RowList = _InsertRows;
            RequireValidate = false;
        }
        public List<BigQueryInsertRow> RowList { get; set; }
        public bool RequireValidate { get; private set; }
        public void IsFailed(List<BigQueryInsertRow> _InsertRows)
        {
            RowList = _InsertRows;
            RequireValidate = true;
            NumFailures += 1;
        }
        public void Update(List<BigQueryInsertRow> _InsertRows)
        {
            RowList = _InsertRows;
        }
        public void IsFailed()
        {
            RequireValidate = true;
            NumFailures += 1;
        }
        public int NumFailures { get; private set; } = 0;
    }
}
