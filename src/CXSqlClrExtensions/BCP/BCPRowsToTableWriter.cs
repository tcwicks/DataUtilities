using ChillX.Core.Structures;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CXSqlClrExtensions.BCP
{
    internal class BCPRowsToTableWriter
    {
        public BCPRowsToTableWriter(ThreadSafeQueue<DataRow> _BCPDataQueue, string _BCPPackageName, string _DestinationConnection, string _DestinationTable, string _BCPSPlitValue, int _TimeoutSeconds = 360, int _BCPBatchSize = 10000, int _ProgressNotificationCount = 10000)
        {
            m_BCPDataQueue = _BCPDataQueue;
            BCPPackageName = _BCPPackageName;
            DestinationConnection = _DestinationConnection;
            DestinationTable = _DestinationTable;
            BCPSPlitValue = _BCPSPlitValue;
            TimeoutSeconds = _TimeoutSeconds;
            BCPBatchSize = _BCPBatchSize;
            ProgressNotificationCount = _ProgressNotificationCount;
        }
        private object SyncRoot = new object();
        public string BCPPackageName { get; private set; } = string.Empty;
        public string DestinationConnection { get; private set; }
        public string DestinationTable { get; private set; }
        public string BCPSPlitValue { get; private set; }
        public int TimeoutSeconds { get; private set; } = 360;
        public int BCPBatchSize { get; private set; } = 10000;

        public string ErrorMessage { get; set; } = string.Empty;

        public int ProgressNotificationCount { get; set; } = 0;

        private ThreadsafeCounterLong RowCounter { get; } = new ThreadsafeCounterLong();

        public long RowsCopiedCount { get { return RowCounter.Value; } }

        public string Status()
        {
            StringBuilder sb;
            sb = new StringBuilder();
            sb.Append(BCPPackageName).Append(@" - Record Count: ").Append(RowsCopiedCount.ToString());
            return sb.ToString();
        }

        private bool m_IsRunning = false;
        public bool IsRunning
        {
            get 
            {
                lock(SyncRoot) 
                { 
                    return m_IsRunning;
                }
            }
            private set 
            {
                lock(SyncRoot)
                {
                    m_IsRunning = value;
                }
            }
        }

        private bool m_ShutdownSignal = false;
        private bool ShutdownSignal
        {
            get
            {
                lock(SyncRoot)
                {
                    return m_ShutdownSignal;
                }
            }
            set
            {
                lock(SyncRoot)
                {
                    m_ShutdownSignal = value;
                }
            }
        }

        public void Shutdown()
        {
            ShutdownSignal = true;
        }

        public void Abort()
        {
            m_BCPDataQueue.Clear();
            ShutdownSignal = true;
            try
            {
                m_RunThread.Abort();
            }
            catch
            {
            }
            finally
            {

            }
        }

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
        public void MessageQueue_Clear()
        {
            lock (SyncRoot)
            {
                m_MessageQueue.Clear();
            }
        }

        private ThreadSafeQueue<DataRow> m_BCPDataQueue = new ThreadSafeQueue<DataRow>();

        private bool BCPData_Dequeue(List<DataRow> targetList, bool isFlushOperation = false)
        {
            int numItems;
            bool Success = false;
            if (isFlushOperation)
            {
                numItems = m_BCPDataQueue.DeQueue(targetList);
                Success = numItems > 0;
            }
            else
            {
                numItems = m_BCPDataQueue.DeQueue(BCPBatchSize, targetList, out Success, true);
            }
            return Success;
        }

        public bool BCPData_HasPendingBatches(out int BatchCount)
        {
            int numItems;
            numItems = m_BCPDataQueue.Count;
            BatchCount = numItems / BCPBatchSize;
            return numItems > BCPBatchSize;
        }
        private SqlConnection m_SqlConnection;

        System.Threading.Thread m_RunThread;
        public bool Run()
        {
            lock(SyncRoot)
            {
                if (!m_IsRunning)
                {
                    m_ShutdownSignal = false;
                    System.Threading.ThreadStart TS;
                    TS = new System.Threading.ThreadStart(DoRun);
                    m_RunThread = new System.Threading.Thread(TS);
                    m_RunThread.Start();
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        private void DoRun()
        {
            lock (SyncRoot)
            {
                if (m_IsRunning) { return; }
                m_IsRunning = true;
            }
            try
            {
                int numEmptyLoops = 0;
                long RowCount;
                RowCount = 0;
                long NotificationCount = 0;
                List<DataRow> dataLIst = new List<DataRow>();
                using (m_SqlConnection = new SqlConnection(DestinationConnection))
                {
                    m_SqlConnection.Open();
                    SqlBulkCopy bc = new SqlBulkCopy(m_SqlConnection);
                    bc.DestinationTableName = DestinationTable;
                    bc.BatchSize = BCPBatchSize;

                    DataTable dtDestination;
                    dtDestination = new DataTable();
                    using (SqlCommand cmdDestination = new SqlCommand(string.Concat(@"Select * from ", DestinationTable, @" where 1 = 0"), m_SqlConnection))
                    {
                        cmdDestination.CommandTimeout = TimeoutSeconds;
                        using (SqlDataReader rdrDestination = cmdDestination.ExecuteReader())
                        {
                            dtDestination.Load(rdrDestination);
                        }
                    }
                    int CounterSrc = 0;
                    int CounterDst = 0;
                    foreach (DataColumn dataColumn in dtDestination.Columns)
                    {
                        if (dataColumn.ReadOnly)
                        {
                            CounterDst += 1;
                        }
                        else
                        {
                            bc.ColumnMappings.Add(new SqlBulkCopyColumnMapping(CounterDst, CounterDst));
                            CounterSrc += 1;
                            CounterDst += 1;
                        }
                    }

                    while (IsRunning)
                    {
                        while (!BCPData_Dequeue(dataLIst, false))
                        {
                            if (numEmptyLoops < 100) { numEmptyLoops += 1; }
                            System.Threading.Thread.Sleep(numEmptyLoops);
                            if (ShutdownSignal) { break; }
                        }
                        numEmptyLoops = 0;
                        if (dataLIst.Count > 0)
                        {
                            bc.WriteToServer(dataLIst.ToArray());
                            NotificationCount += dataLIst.Count;
                            RowCount = RowCounter.Increment(dataLIst.Count);
                            dataLIst.Clear();
                        }
                        if (ShutdownSignal)
                        {
                            break;
                        }
                        if (NotificationCount >= ProgressNotificationCount)
                        {
                            NotificationCount = 0;
                            MessageQueue_Enqueue(string.Concat(BCPPackageName, @" ", RowCount.ToString(), @" Complete"));
                        }
                    }
                    if (BCPData_Dequeue(dataLIst, true))
                    {
                        bc.WriteToServer(dataLIst.ToArray());
                        RowCounter.Increment(dataLIst.Count);
                        dataLIst.Clear();
                    }
                    MessageQueue_Enqueue(string.Concat(BCPPackageName, @" ", RowCount.ToString(), @" Complete"));
                    m_SqlConnection.Close();
                }
            }
            catch(Exception ex)
            {
                MessageQueue_Enqueue(ex.ToString());
                ErrorMessage = ex.ToString();
            }
            finally
            {
                lock (SyncRoot)
                {
                    m_IsRunning = false;
                }
            }
        }
    }
}
