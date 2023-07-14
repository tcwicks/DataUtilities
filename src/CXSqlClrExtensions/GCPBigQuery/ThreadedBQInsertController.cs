using ChillX.Core.Structures;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CXSqlClrExtensions.GCPBigQuery
{
    internal class ThreadedBQInsertController
    {
        public ThreadedBQInsertController(ThreadSafeQueue<BQInsertJob> _PendingWorkQueue, ThreadSafeQueue<BQInsertJob> _FailedWorkQueue, string _BQCredentialJSon, string _BQProjectID, string _BQDataset, string _BQTable, string _UniqueKeyField)
        {
            m_BQCredentialJSon = _BQCredentialJSon;
            m_BQProjectID = _BQProjectID;
            m_BQDataset = _BQDataset;
            m_BQTable = _BQTable;
            m_UniqueKeyField = _UniqueKeyField;
            m_PendingWorkQueue = _PendingWorkQueue;
            m_FailedWorkQueue = _FailedWorkQueue;
        }
        private object SyncRoot { get; } = new object();
        private string m_BQCredentialJSon;
        public string BQCredentialJSon { get { return m_BQCredentialJSon; } }
        private string m_BQProjectID;
        public string BQProjectID { get { return m_BQProjectID; }  }
        private string m_BQDataset;
        public string BQDataset { get { return m_BQDataset; } }
        private string m_BQTable;
        public string BQTable { get { return m_BQTable; } }
        private string m_UniqueKeyField;
        public string UniqueKeyField { get { return m_UniqueKeyField; } }
        private string m_ErrorMessage = string.Empty;
        public string ErrorMessage
        {
            get
            {
                lock(SyncRoot) { return m_ErrorMessage; }
            }
            private set 
            {
                lock(SyncRoot) { m_ErrorMessage = value; }
            }
        }
        private bool m_IsFatalError = false;
        public bool IsFatalError
        {
            get { lock(SyncRoot) { return m_IsFatalError; } }
        }
        private bool m_IsStarted = false;
        public bool IsStarted
        {
            get
            {
                lock(SyncRoot)
                {
                    return m_IsStarted;
                }
            }
        }
        public bool m_IsRunning = false;
        public bool IsRunning
        {
            get
            {
                lock(SyncRoot)
                {
                    return m_IsRunning;
                }
            }
        }
        private bool m_ShutdownSignal = false;
        public bool ShutdownSignal
        {
            get
            {
                lock(SyncRoot)
                {
                    return m_ShutdownSignal;
                }
            }
        }
        public ThreadsafeCounterLong CompletedCounter { get; } = new ThreadsafeCounterLong();
        private ThreadSafeQueue<BQInsertJob> m_PendingWorkQueue = new ThreadSafeQueue<BQInsertJob>();
        private ThreadSafeQueue<BQInsertJob> PendingWorkQueue { get { return m_PendingWorkQueue; } } 
        private ThreadSafeQueue<BQInsertJob> m_FailedWorkQueue = new ThreadSafeQueue<BQInsertJob>();
        private ThreadSafeQueue<BQInsertJob> FailedWorkQueue { get { return m_FailedWorkQueue; } } 

        public void Shutdown()
        {
            lock (SyncRoot)
            {
                m_ShutdownSignal = true;
            }
        }

        public void Abort()
        {
            PendingWorkQueue.Clear();
            lock (SyncRoot)
            {
                m_ShutdownSignal = true;
            }
            PendingWorkQueue.Clear();
        }
        System.Threading.Thread m_RunThread;
        public bool Run()
        {
            lock(SyncRoot)
            {
                if (!m_IsRunning)
                {
                    m_ShutdownSignal = false;
                    m_IsStarted = false;
                    CompletedCounter.ValueInterlocked = 0;
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
            BQInsertJob BQJob;
            lock (SyncRoot)
            {
                if (m_IsRunning) { return; }
                m_IsRunning = true;
                m_IsStarted = true;
                CompletedCounter.ValueInterlocked = 0;
            }
            try
            {
                if (string.IsNullOrEmpty(BQCredentialJSon))
                {
                    throw new InvalidOperationException(@"GCP BQ Credentials cannot be blank: ");
                }
                GoogleCredential BQCredential = GoogleCredential.FromJson(BQCredentialJSon);

                BigQueryClient BQClient = BigQueryClient.Create(BQProjectID, BQCredential);

                BigQueryTable targetTable;

                targetTable = BQClient.GetTable(BQProjectID, BQDataset, BQTable);
                while (IsRunning)
                {
                    bool HasData;
                    bool Success;
                    int NumRetry;
                    BQJob = PendingWorkQueue.DeQueue(out HasData);
                    if (HasData)
                    {
                        NumRetry = 0;
                        Success = false;
                        while (!Success)
                        {
                            try
                            {
                                if (BQJob.RequireValidate)
                                {
                                    
                                    if (BQJob.NumFailures > 3)
                                    {
                                        FailedWorkQueue.Enqueue(BQJob);
                                        Success = true;
                                    }
                                    else
                                    {
                                        StringBuilder sbCheckKeys = new StringBuilder();
                                        sbCheckKeys.Append(@"SELECT ").Append(UniqueKeyField).Append(@" from ").Append(targetTable).Append(@" where ").Append(UniqueKeyField).Append(@" in (");
                                        bool IsFirst;
                                        IsFirst = true;
                                        foreach (BigQueryInsertRow row in BQJob.RowList)
                                        {
                                            if (IsFirst) { IsFirst = false; }
                                            else { sbCheckKeys.Append(@","); }
                                            sbCheckKeys.Append(@"'").Append(row[UniqueKeyField].ToString()).Append(@"'");
                                        }
                                        sbCheckKeys.Append(@")");
                                        BigQueryParameter[] parameters = null;
                                        BigQueryResults results = BQClient.ExecuteQuery(sbCheckKeys.ToString(), parameters);
                                        HashSet<string> InsertedKeys;
                                        InsertedKeys = new HashSet<string>();
                                        foreach (BigQueryRow row in results)
                                        {
                                            InsertedKeys.Add(row[UniqueKeyField].ToString());
                                        }
                                        List<BigQueryInsertRow> BQRowListMissing;
                                        BQRowListMissing = new List<BigQueryInsertRow>();
                                        foreach (BigQueryInsertRow row in BQJob.RowList)
                                        {
                                            if (!InsertedKeys.Contains(row[UniqueKeyField]))
                                            {
                                                BQRowListMissing.Add(row);
                                            }
                                        }
                                        if (BQRowListMissing.Count > 0) 
                                        {
                                            BQJob.Update(BQRowListMissing);
                                            targetTable.InsertRows(BQJob.RowList);
                                            CompletedCounter.Increment(BQJob.RowList.Count);
                                        }
                                        Success = true;
                                    }
                                }
                                else
                                {
                                    targetTable.InsertRows(BQJob.RowList);
                                    CompletedCounter.Increment(BQJob.RowList.Count);
                                    Success = true;
                                }
                            }
                            catch (Exception ex2)
                            {
                                BQJob.IsFailed();
                                PendingWorkQueue.Enqueue(BQJob);
                            }
                        }
                    }
                    else
                    {
                        System.Threading.Thread.Sleep(10);
                        if (ShutdownSignal) { break; }
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.ToString();
                lock (SyncRoot)
                {
                    m_IsRunning = false;
                    m_IsFatalError = true;
                }
            }
            finally
            {
                lock (SyncRoot)
                {
                    m_IsRunning = false;
                }
                if (ShutdownSignal)
                {
                    PendingWorkQueue.Clear();
                    FailedWorkQueue.Clear();
                }
            }
        }
    }
}
