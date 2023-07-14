using ChillX.Core.Structures;
using Google.Apis.Bigquery.v2.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CXSqlClrExtensions.BCP
{
    public class BCPRowsToTableReader
    {
        public BCPRowsToTableReader(ChannelisedQueueManager<DataRow> _BCPDataQueue, string _BCPPackageName, string _SourceConnection, string _DestinationConnection, string _DestinationTable, string _SQL, string _BCPSplitOnValueOfField, int _BCPBatchSize, int _TimeoutSeconds)
        {
            m_BCPDataQueue = _BCPDataQueue;
            BCPPackageName = _BCPPackageName;
            SourceConnection = _SourceConnection;
            DestinationConnection = _DestinationConnection;
            DestinationTable = _DestinationTable;
            SQL = _SQL;
            BCPSplitOnValueOfField = _BCPSplitOnValueOfField;
            BCPBatchSize = _BCPBatchSize;
            TimeoutSeconds = _TimeoutSeconds;
        }
        private object SyncRoot = new object();
        public string BCPPackageName { get; private set; } = string.Empty;
        public string SQL { get; set; }
        public string SourceConnection { get; private set; }
        public string DestinationConnection { get; private set; }
        public string DestinationTable { get; private set; }
        public string BCPSplitOnValueOfField { get; private set; }
        public int BCPBatchSize { get; private set; } = 10000;
        public string ErrorMessage { get; set; } = string.Empty;
        public int TimeoutSeconds { get; set; } = 3600;


        private ChannelisedQueueManager<DataRow> m_BCPDataQueue;
        private ThreadSafeQueue<DataRow> BCPData_GetQueue(object Channel)
        {
            return m_BCPDataQueue.GetQueue(Channel);
        }
        //public bool BCPData_Dequeue(List<KeyValuePair<object, DataRow>> targetList)
        //{
        //    int numItems;
        //    bool Success = false;
        //    numItems = m_BCPDataQueue.DeQueue(targetList);
        //    Success = numItems > 0;
        //    return Success;
        //}

        private bool m_IsRunning = false;
        public bool IsRunning
        {
            get
            {
                lock (SyncRoot)
                {
                    return m_IsRunning;
                }
            }
            private set
            {
                lock (SyncRoot)
                {
                    m_IsRunning = value;
                }
            }
        }


        private bool m_RunStarted = false;
        public bool RunStarted
        {
            get
            {
                lock (SyncRoot)
                {
                    return m_RunStarted;
                }
            }
            private set
            {
                lock (SyncRoot)
                {
                    m_RunStarted = value;
                }
            }
        }

        private bool m_ShutdownSignal = false;
        private bool ShutdownSignal
        {
            get
            {
                lock (SyncRoot)
                {
                    return m_ShutdownSignal;
                }
            }
            set
            {
                lock (SyncRoot)
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


        System.Threading.Thread m_RunThread;
        public bool Run()
        {
            lock (SyncRoot)
            {
                if (!m_IsRunning)
                {
                    m_ShutdownSignal = false;
                    m_RunStarted = false;
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
                m_RunStarted = true;
            }
            try
            {
                DataTable dtDestination;
                dtDestination = new DataTable();
                using (SqlConnection sqlConDestination = new SqlConnection(DestinationConnection))
                {
                    sqlConDestination.Open();
                    using (SqlCommand cmdDestination = new SqlCommand(string.Concat(@"Select * from ", DestinationTable, @" where 1 = 0"), sqlConDestination))
                    {
                        cmdDestination.CommandTimeout = TimeoutSeconds;
                        using (SqlDataReader rdrDestination = cmdDestination.ExecuteReader())
                        {
                            dtDestination.Load(rdrDestination);
                        }
                    }
                }
                int CounterSrc = 0;
                int CounterDst = 0;
                Dictionary<int,int> MappingDict = new Dictionary<int,int>();
                foreach (DataColumn dataColumn in dtDestination.Columns)
                {
                    if (dataColumn.ReadOnly)
                    {
                        CounterDst += 1;
                    }
                    else
                    {
                        MappingDict.Add(CounterSrc, CounterDst);
                        CounterSrc += 1;
                        CounterDst += 1;
                    }
                }
                ThreadSafeQueue<DataRow> QueueInstance;
                int columnCount;
                DataRow newRow;
                object splitValue;
                columnCount = CounterSrc;
                Dictionary<object, ThreadSafeQueue<DataRow>> QueueDict;
                QueueDict = new Dictionary<object, ThreadSafeQueue<DataRow>>();
                int NumFull;
                NumFull = 0;
                using (SqlConnection srcConnenction = new SqlConnection(SourceConnection))
                {
                    srcConnenction.Open();
                    using (SqlCommand cmd = new SqlCommand(SQL, srcConnenction))
                    {
                        cmd.CommandTimeout = TimeoutSeconds;
                        using (SqlDataReader rdr = cmd.ExecuteReader())
                        {
                            while (rdr.Read())
                            {
                                newRow = dtDestination.NewRow();
                                for (int I = 0; I < columnCount; I++)
                                {
                                    newRow[MappingDict[I]] = rdr[I];
                                }
                                splitValue = rdr[BCPSplitOnValueOfField];
                                if (!QueueDict.TryGetValue(splitValue,out QueueInstance))
                                {
                                    QueueInstance = BCPData_GetQueue(splitValue);
                                    QueueDict.Add(splitValue, QueueInstance);
                                }
                                while (!QueueInstance.EnqueueCapped(newRow, BCPBatchSize*5))
                                {
                                    if (NumFull < 100) { NumFull += 1; }
                                    System.Threading.Thread.Sleep(NumFull);
                                    if (ShutdownSignal) { break; }
                                }
                                NumFull = 0;
                                if (ShutdownSignal) { break; }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
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
