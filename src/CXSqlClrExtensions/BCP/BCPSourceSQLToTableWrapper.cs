using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CXSqlClrExtensions.BCP
{
    internal class BCPSourceSQLToTableWrapper
    {
 
        public string BCPPackageName { get; set; } = string.Empty;
        public string SQL { get; set; }
        public string SourceConnection { get; set; }
        public string DestinationConnection { get; set; }
        public string DestinationTable { get; set; }
        public string ErrorMessage { get; set; } = string.Empty;
        public int TimeoutSeconds { get; set; } = 3600;
        public int BCPBatchSize { get; set; } = 10000;
        public int ProgressNotificationCount { get; set; } = 0;

        public long RowsCopiedCount { get; private set; } = 0;

        public string Status()
        {
            StringBuilder sb;
            sb = new StringBuilder();
            sb.Append(BCPPackageName).Append(@" - Record Count: ").Append(RowsCopiedCount.ToString());
            return sb.ToString();
        }

        private SqlCommand m_RunningCmd = null;
        public void TryCancelRunningCommand()
        {
            try
            {
                if (m_RunningCmd != null)
                {
                    m_RunningCmd.Cancel();
                }
            }
            catch (Exception ex)
            {
            }
            finally
            {
                m_RunningCmd = null;
            }
        }

        private object SyncRoot = new object();
        private Queue<string> m_MessageQueue = new Queue<string>();
        private void MessageQueue_Enqueue(string message)
        {
            lock (SyncRoot)
            {
                m_MessageQueue.Enqueue(message);
            }
        }
        public string MessageQueue_Dequeue()
        {
            lock (SyncRoot)
            {
                if (m_MessageQueue.Count > 0)
                {
                    return m_MessageQueue.Dequeue();
                }
            }
            return null;
        }

        public void BCPSourceSQLToTable()
        {
            //WindowsIdentity currentIdentity = SqlContext.WindowsIdentity;
            //WindowsImpersonationContext impersonatedIdentity = currentIdentity.Impersonate();
            RowsCopiedCount = 0;
            try
            {
                using (SqlConnection srcConnenction = new SqlConnection(SourceConnection))
                {
                    srcConnenction.Open();
                    using (SqlConnection dstConnection = new SqlConnection(DestinationConnection))
                    {
                        dstConnection.Open();

                        using (SqlCommand cmd = new SqlCommand(SQL, srcConnenction))
                        {
                            m_RunningCmd = cmd;
                            cmd.CommandTimeout = TimeoutSeconds;
                            using (SqlDataReader rdr = cmd.ExecuteReader())
                            {

                                SqlBulkCopy bc;
                                bc = null;
                                try
                                {
                                    bc = new SqlBulkCopy(dstConnection);
                                    if (ProgressNotificationCount > 0)
                                    {
                                        bc.NotifyAfter = ProgressNotificationCount;
                                        bc.SqlRowsCopied += Bc_SqlRowsCopied;
                                    }
                                    bc.DestinationTableName = DestinationTable;
                                    bc.BatchSize = BCPBatchSize;
                                    bc.BulkCopyTimeout = 3600;
                                    bc.WriteToServer(rdr);
                                    RowsCopiedCount = bc.RowsCopiedCount();
                                }
                                finally
                                {
                                    if (ProgressNotificationCount > 0)
                                    {
                                        bc.SqlRowsCopied -= Bc_SqlRowsCopied;
                                    }
                                }
                            }
                            m_RunningCmd = null;
                        }
                        dstConnection.Close();
                    }
                    srcConnenction.Close();
                }

                using (SqlConnection dstConnection = new SqlConnection(DestinationConnection))
                {

                }
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.ToString();
                return;
            }
            finally
            {

                //impersonatedIdentity.Undo();
            }
        }

        private void Bc_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            MessageQueue_Enqueue(string.Concat(BCPPackageName, @" ", e.RowsCopied.ToString(), @" Complete"));
            RowsCopiedCount += e.RowsCopied;
        }
    }
}
