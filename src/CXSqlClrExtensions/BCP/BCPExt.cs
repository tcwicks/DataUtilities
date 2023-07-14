using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using ChillX.Core.Structures;

namespace CXSqlClrExtensions.BCP
{
    internal static class BCPExt
    {
        public static void BCPCopyToTableThreaded(int threadOffsetSeconds, int perThreadProgressNotificationCount, int diagnosticLevel, int timeoutSeconds, int batchSizeBCP, string sourceConnection, string sqlStatements, string BCPSplitOnValueOfField, string destinationConnection, string destinationTable, out string ErrorMessage, int MaxConcurrency = 16)
        {
            //Warning Not stable on very large datasets.
            Dictionary<object, BCPRowsToTableWriter> BCPWriterDict;
            BCPRowsToTableWriter BCPWriter;
            BCPRowsToTableReader BCPReader;
            BCPWriterDict = new Dictionary<object, BCPRowsToTableWriter>();
            List<BCPRowsToTableWriter> ActiveBCPWriterList;
            ActiveBCPWriterList = new List<BCPRowsToTableWriter>();
            List<BCPRowsToTableReader> ActiveBCPReaderList;
            ActiveBCPReaderList = new List<BCPRowsToTableReader>();
            List<string> SQLJobListRaw;
            List<string> SQLJobList;
            ChannelisedQueueManager<DataRow> QueueManager;
            QueueManager = new ChannelisedQueueManager<DataRow>();
            Stopwatch SWNotification = new Stopwatch();
            Stopwatch SWProcessTimer = new Stopwatch();
            ErrorMessage = string.Empty;
            SWProcessTimer.Start();

            //using (SqlConnection sqlConContext = new SqlConnection(sourceConnection))
            using (SqlConnection sqlConContext = new SqlConnection("context connection=true"))
            {
                sqlConContext.Open();
                using (SqlConnection srcConnenction = new SqlConnection(sourceConnection))
                {
                    SQLJobListRaw = new List<string>(sqlStatements.Split(@";".ToCharArray(), StringSplitOptions.RemoveEmptyEntries));
                    SQLJobList = new List<string>();
                    foreach (string sqlString in SQLJobListRaw)
                    {
                        if (!string.IsNullOrEmpty(sqlString.Trim().Trim(@";".ToCharArray()).Trim()))
                        {
                            SQLJobList.Add(sqlString);
                        }
                    }
                    if (SQLJobList.Count == 0)
                    {
                        throw new InvalidOperationException(@"Source SQL must contain at least one value SQL statement");
                    }
                    srcConnenction.Open();
                    using (SqlCommand cmd = new SqlCommand(SQLJobList[0], srcConnenction))
                    {
                        cmd.CommandTimeout = timeoutSeconds;
                        //object SplitValue;
                        QueueManager = new ChannelisedQueueManager<DataRow>();

                        try
                        {
                            DataTable dtDestination;
                            dtDestination = new DataTable();
                            using (SqlConnection sqlConDestination = new SqlConnection(destinationConnection))
                            {
                                sqlConDestination.Open();
                                using (SqlCommand cmdDestination = new SqlCommand(string.Concat(@"Select * from ", destinationTable, @" where 1 = 0"), sqlConDestination))
                                {
                                    cmdDestination.CommandTimeout = timeoutSeconds;
                                    using (SqlDataReader rdrDestination = cmdDestination.ExecuteReader())
                                    {
                                        dtDestination.Load(rdrDestination);
                                    }
                                }
                            }
                            int RequiredColumns;
                            int DestinationColumnOrdinal;
                            StringBuilder sbRequiredColumns = new StringBuilder();
                            string RequiredColumnsText;
                            RequiredColumns = 0;
                            DestinationColumnOrdinal = 0;
                            foreach (DataColumn column in dtDestination.Columns)
                            {
                                if (!column.ReadOnly)
                                {
                                    sbRequiredColumns.Append(@"Source Column Ordinal: ").Append(RequiredColumns.ToString().PadLeft(3, '0')).Append(@" - Destination Column Ordinal: ").Append(DestinationColumnOrdinal.ToString().PadLeft(3, '0')).Append(@" - Desination Column Name: ").AppendLine(column.ColumnName);
                                    RequiredColumns += 1;
                                }
                                DestinationColumnOrdinal += 1;
                            }
                            RequiredColumnsText = sbRequiredColumns.ToString();
                            if (diagnosticLevel > 0)
                            {
                                string.Concat("Source data will be mapped by ordinal sequence to the following destination fields:\r\n").SQLPipePrintImmediate(sqlConContext);
                                @"---------------------------------------------".SQLPipePrintImmediate(sqlConContext);
                                RequiredColumnsText.SQLPipePrintImmediate(sqlConContext);
                                "---------------------------------------------\r\n".SQLPipePrintImmediate(sqlConContext);
                                "\r\nExtracting source schema from provided sql\r\n".SQLPipePrintImmediate(sqlConContext);
                            }
                            using (SqlDataReader rdr = cmd.ExecuteReader())
                            {
                                DataTable tableSchema = rdr.GetSchemaTable();
                                bool SplitFieldFound = false;
                                foreach (DataRow row in tableSchema.Rows)
                                {
                                    if (row[@"ColumnName"].ToString().ToLower().Trim() == BCPSplitOnValueOfField.ToLower().Trim())
                                    {
                                        SplitFieldFound = true;
                                        break;
                                    }
                                }
                                if (!SplitFieldFound)
                                {
                                    StringBuilder sbMissingFieldError = new StringBuilder();
                                    sbMissingFieldError.Append(@"Specified field ").Append(BCPSplitOnValueOfField).AppendLine(" to split BCP threads on was not found in the data source:");
                                    sbMissingFieldError.AppendLine(@"Available Fields Are:");
                                    foreach (DataRow row in tableSchema.Rows)
                                    {
                                        sbMissingFieldError.AppendLine(row[@"ColumnName"].ToString());
                                    }
                                    throw new ArgumentException(sbMissingFieldError.ToString());
                                }
                                if (rdr.FieldCount < RequiredColumns)
                                {
                                    throw new InvalidOperationException(string.Concat(@"Column Count missmatch. Source SQL has ", rdr.FieldCount.ToString(), @" columns while destination table has ", RequiredColumns.ToString(), " non read only columns. \r\nSince we are not using BCP table mappings please match the columns in the source to the destination.\r\n For columns such as Identity columns and caluclated fields skip these as they are read only. Column sequence must also match please structure the source sql as an exact match to the schema of the destination table.\r\nRequired Columns are:\r\n", sbRequiredColumns.ToString()));
                                }
                            }
                            SWNotification.Start();
                            foreach (string sqlString in SQLJobList)
                            {
                                if (diagnosticLevel > 2)
                                {
                                    string.Concat("Adding BCP Reader for SQL Statement: \r\n\r\n", sqlString, "\r\n").SQLPipePrintImmediate(sqlConContext);
                                }
                                else if (diagnosticLevel > 0)
                                {
                                    string.Concat(@"Adding BCP Reader:", ActiveBCPReaderList.Count.ToString()).SQLPipePrintImmediate(sqlConContext);
                                }
                                BCPReader = new BCPRowsToTableReader(QueueManager, string.Concat(@"BCP Reader ", ActiveBCPReaderList.Count.ToString()), sourceConnection, destinationConnection, destinationTable, sqlString, BCPSplitOnValueOfField, batchSizeBCP, timeoutSeconds);
                                BCPReader.Run();
                                while (!BCPReader.RunStarted)
                                {
                                    System.Threading.Thread.Sleep(1);
                                }
                                ActiveBCPReaderList.Add(BCPReader);
                            }
                            bool isReading;
                            isReading = true;
                            List<object> ChannelList;
                            ChannelList = new List<object>();
                            while (isReading)
                            {
                                isReading = false;
                                ChannelList.Clear();
                                QueueManager.GetChannels(ChannelList);
                                foreach (object Channel in ChannelList)
                                {
                                    if (!BCPWriterDict.TryGetValue(Channel, out BCPWriter))
                                    {
                                        BCPWriter = new BCPRowsToTableWriter(QueueManager.GetQueue(Channel), string.Concat(@"BCP ", destinationTable, " : ", BCPSplitOnValueOfField, @" = ", Channel.ToString()), destinationConnection, destinationTable, Channel.ToString(), timeoutSeconds, batchSizeBCP, perThreadProgressNotificationCount);
                                        BCPWriter.Run();
                                        BCPWriterDict.Add(Channel, BCPWriter);
                                        ActiveBCPWriterList.Add(BCPWriter);
                                    }
                                }
                                foreach (BCPRowsToTableReader readerInstance in ActiveBCPReaderList)
                                {
                                    if (readerInstance.IsRunning)
                                    {
                                        isReading = true;
                                    }
                                }
                                if (SWNotification.Elapsed.TotalSeconds > 1d)
                                {
                                    foreach (BCPRowsToTableWriter BCPWRapperInstance in ActiveBCPWriterList)
                                    {
                                        string Message;
                                        Message = BCPWRapperInstance.MessageQueue_Dequeue();
                                        while (Message != null)
                                        {
                                            if (diagnosticLevel > 1)
                                            {
                                                Message.SQLPipePrintImmediate(sqlConContext);
                                            }
                                            Message = BCPWRapperInstance.MessageQueue_Dequeue();
                                        }
                                        if (!BCPWRapperInstance.IsRunning)
                                        {
                                            BCPWRapperInstance.Run();
                                        }
                                    }
                                    SWNotification.Restart();
                                }
                                System.Threading.Thread.Sleep(100);
                                //foreach (KeyValuePair<object, DataRow> newRowPair in DataRowList)
                                //{
                                //    SplitValue = newRowPair.Key;
                                //    if (SplitValue == null)
                                //    {
                                //        throw new InvalidOperationException(string.Concat(@"Value in BCP thread split field ", BCPSplitOnValueOfField, @" cannot have null values: "));
                                //    }

                                //    //if (!BCPWriterDict.TryGetValue(SplitValue, out BCPWriter))
                                //    //{
                                //    //    if (BCPWriterDict.Count >= MaxConcurrency)
                                //    //    {
                                //    //        throw new InvalidOperationException(string.Concat(@"Concurrency count exceeded. Number of partitions in the data created by the number of distinct values in field ", BCPSplitOnValueOfField, @" is greater than the maximum concurrency specified which is: ", MaxConcurrency.ToString()));
                                //    //    }
                                //    //    BCPWriter = new BCPRowsToTableWriter(string.Concat(@"BCP ", destinationTable, " : ", BCPSplitOnValueOfField, @" = ", SplitValue.ToString()), destinationConnection, destinationTable, SplitValue.ToString(), timeoutSeconds, batchSizeBCP, perThreadProgressNotificationCount);
                                //    //    if (diagnosticLevel > 0)
                                //    //    {
                                //    //        string.Concat(@"Adding BCP Writer: ", BCPWriter.BCPPackageName).SQLPipePrintImmediate(sqlConContext);
                                //    //    }
                                //    //    BCPWriter.Run();
                                //    //    BCPWriterDict.Add(SplitValue, BCPWriter);
                                //    //    ActiveBCPWriterList.Add(BCPWriter);
                                //    //}

                                //    //while (!BCPWriter.BCPData_Enqueue(newRowPair.Value))
                                //    //{
                                //    //    System.Threading.Thread.Sleep(1);
                                //    //}
                                //    //if (SWNotification.Elapsed.TotalSeconds > 1d)
                                //    //{
                                //    //    foreach (BCPRowsToTableWriter BCPWRapperInstance in ActiveBCPWriterList)
                                //    //    {
                                //    //        string Message;
                                //    //        Message = BCPWRapperInstance.MessageQueue_Dequeue();
                                //    //        while (Message != null)
                                //    //        {
                                //    //            if (diagnosticLevel > 1)
                                //    //            {
                                //    //                Message.SQLPipePrintImmediate(sqlConContext);
                                //    //            }
                                //    //            Message = BCPWRapperInstance.MessageQueue_Dequeue();
                                //    //        }
                                //    //    }
                                //    //    SWNotification.Restart();
                                //    //}
                                //}
                            }
                            string.Concat(@"Reading Data complete waiting for Write Complete. Time Elapsed: ", SWProcessTimer.Elapsed.ToString()).SQLPipePrintImmediate(sqlConContext);
                            bool IsRunning;
                            IsRunning = true;
                            while (IsRunning)
                            {
                                IsRunning = false;
                                int NumPendingBatches;
                                foreach (BCPRowsToTableWriter BCPWRapperInstance in ActiveBCPWriterList)
                                {
                                    if (BCPWRapperInstance.IsRunning && BCPWRapperInstance.BCPData_HasPendingBatches(out NumPendingBatches))
                                    {
                                        IsRunning = true;
                                    }
                                }
                                if (SWNotification.Elapsed.TotalSeconds > 1d)
                                {
                                    foreach (BCPRowsToTableWriter BCPWRapperInstance in ActiveBCPWriterList)
                                    {
                                        string Message;
                                        Message = BCPWRapperInstance.MessageQueue_Dequeue();
                                        while (Message != null)
                                        {
                                            if (diagnosticLevel > 1)
                                            {
                                                Message.SQLPipePrintImmediate(sqlConContext);
                                            }
                                            Message = BCPWRapperInstance.MessageQueue_Dequeue();
                                        }
                                        if (!BCPWRapperInstance.IsRunning)
                                        {
                                            BCPWRapperInstance.Run();
                                        }
                                    }
                                    SWNotification.Restart();
                                }
                                System.Threading.Thread.Sleep(100);
                            }
                            foreach (BCPRowsToTableWriter BCPWRapperInstance in ActiveBCPWriterList)
                            {
                                BCPWRapperInstance.Shutdown();
                            }
                            IsRunning = true;
                            while (IsRunning)
                            {
                                IsRunning = false;
                                int NumPendingBatches;
                                foreach (BCPRowsToTableWriter BCPWRapperInstance in ActiveBCPWriterList)
                                {
                                    if (BCPWRapperInstance.IsRunning)
                                    {
                                        IsRunning = true;
                                    }
                                }
                                if (SWNotification.Elapsed.TotalSeconds > 1d)
                                {
                                    foreach (BCPRowsToTableWriter BCPWRapperInstance in ActiveBCPWriterList)
                                    {
                                        string Message;
                                        Message = BCPWRapperInstance.MessageQueue_Dequeue();
                                        while (Message != null)
                                        {
                                            if (diagnosticLevel > 1)
                                            {
                                                Message.SQLPipePrintImmediate(sqlConContext);
                                            }
                                            Message = BCPWRapperInstance.MessageQueue_Dequeue();
                                        }
                                    }
                                    SWNotification.Restart();
                                }
                                System.Threading.Thread.Sleep(100);
                            }
                            foreach (BCPRowsToTableWriter BCPWRapperInstance in ActiveBCPWriterList)
                            {
                                string Message;
                                Message = BCPWRapperInstance.MessageQueue_Dequeue();
                                while (Message != null)
                                {
                                    if (diagnosticLevel > 1)
                                    {
                                        Message.SQLPipePrintImmediate(sqlConContext);
                                    }
                                    Message = BCPWRapperInstance.MessageQueue_Dequeue();
                                }
                            }
                            SWProcessTimer.Stop();
                            long TotalRecordCount;
                            TotalRecordCount = 0;
                            long RecordsPerSecond;
                            foreach (BCPRowsToTableWriter CompletedWrapper in ActiveBCPWriterList)
                            {
                                TotalRecordCount += CompletedWrapper.RowsCopiedCount;
                            }
                            RecordsPerSecond = Convert.ToInt64(TotalRecordCount / SWProcessTimer.Elapsed.TotalSeconds);
                            if (diagnosticLevel > 0)
                            {
                                string.Concat(@"Procesing Complete. Time Elapsed: ", SWProcessTimer.Elapsed.ToString(), @" - Total Records: ", TotalRecordCount.ToString(), @" - Records Per Second: ", RecordsPerSecond.ToString()).SQLPipePrintImmediate(sqlConContext);
                            }
                            foreach (BCPRowsToTableReader readerInstance in ActiveBCPReaderList)
                            {
                                if (!string.IsNullOrEmpty(readerInstance.ErrorMessage))
                                {
                                    @"".SQLPipePrintImmediate(sqlConContext);
                                    @"------------------------------------------------------------------".SQLPipePrintImmediate(sqlConContext);
                                    @"-------Error Reading Data-----------------------------------------".SQLPipePrintImmediate(sqlConContext);
                                    @"------------------------------------------------------------------".SQLPipePrintImmediate(sqlConContext);
                                    readerInstance.ErrorMessage.SQLPipePrintImmediate(sqlConContext);
                                    readerInstance.SQL.SQLPipePrintImmediate(sqlConContext);
                                    @"".SQLPipePrintImmediate(sqlConContext);
                                }
                            }
                            foreach (BCPRowsToTableWriter writerInstance in ActiveBCPWriterList)
                            {
                                if (!string.IsNullOrEmpty(writerInstance.ErrorMessage))
                                {
                                    @"".SQLPipePrintImmediate(sqlConContext);
                                    @"------------------------------------------------------------------".SQLPipePrintImmediate(sqlConContext);
                                    @"-------Error Writing Data-----------------------------------------".SQLPipePrintImmediate(sqlConContext);
                                    @"------------------------------------------------------------------".SQLPipePrintImmediate(sqlConContext);
                                    writerInstance.ErrorMessage.SQLPipePrintImmediate(sqlConContext);
                                    @"".SQLPipePrintImmediate(sqlConContext);
                                }
                            }
                            if (ActiveBCPWriterList.Count > 0)
                            {
                                SqlDataRecord record = new SqlDataRecord(
                                    new SqlMetaData("BCPPackage", SqlDbType.VarChar, 100),
                                    new SqlMetaData("NumRecords", SqlDbType.BigInt),
                                    new SqlMetaData("HasError", SqlDbType.Bit),
                                    new SqlMetaData("BCPSplitOnValueOfField", SqlDbType.VarChar, 255),
                                    new SqlMetaData("BCPSplitOnValue", SqlDbType.VarChar, 255),
                                    new SqlMetaData("ProcessingError", SqlDbType.Text));
                                SqlContext.Pipe.SendResultsStart(record);
                                foreach (BCPRowsToTableWriter CompletedWrapper in ActiveBCPWriterList)
                                {
                                    record.SetString(0, CompletedWrapper.BCPPackageName);
                                    record.SetInt64(1, CompletedWrapper.RowsCopiedCount);
                                    record.SetSqlBoolean(2, (CompletedWrapper.ErrorMessage != String.Empty));
                                    record.SetString(3, BCPSplitOnValueOfField);
                                    record.SetString(4, CompletedWrapper.BCPSPlitValue);
                                    record.SetString(5, CompletedWrapper.ErrorMessage);
                                    SqlContext.Pipe.SendResultsRow(record);
                                }
                                SqlContext.Pipe.SendResultsEnd();
                            }
                        }
                        catch (Exception ex)
                        {
                            ErrorMessage = ex.ToString();
                            foreach (BCPRowsToTableWriter BCPWRapperInstance in ActiveBCPWriterList)
                            {
                                BCPWRapperInstance.Abort();
                            }
                            foreach (BCPRowsToTableReader readerInstance in ActiveBCPReaderList)
                            {
                                readerInstance.Abort();
                            }
                            @"".SQLPipePrintImmediate(sqlConContext);
                            @"------------------------------------------------------------------".SQLPipePrintImmediate(sqlConContext);
                            @"-------Fatal Error------------------------------------------------".SQLPipePrintImmediate(sqlConContext);
                            @"------------------------------------------------------------------".SQLPipePrintImmediate(sqlConContext);
                            ErrorMessage.SQLPipePrintImmediate(sqlConContext);
                            @"".SQLPipePrintImmediate(sqlConContext);
                        }
                        finally
                        {
                            QueueManager.Dispose();
                            ActiveBCPWriterList.Clear();
                            BCPWriterDict.Clear();
                            SWProcessTimer.Stop();
                            SWNotification.Stop();
                        }
                    }
                }
            }
        }

        public static void BCPSourceSQLToTableThreaded(int threadOffsetSeconds, int progressNotificationCount, int diagnosticLevel, int timeoutSeconds, int batchSizeBCP, string sourceConnection, string sql, string destinationConnection, string destinationTable, out string ErrorMessage)
        {
            //Very stable regardless of dataset size
            BCPSourceSQLToTableWrapper Wrapper;
            List<BCPSourceSQLToTableWrapper> WrapperList;
            System.Threading.ThreadStart TS;
            System.Threading.Thread RunThread;
            List<System.Threading.Thread> RunThreadList;
            List<String> SQLJobList;
            StringBuilder sbError;
            if (progressNotificationCount < 0) { progressNotificationCount = 0; }
            RunThreadList = new List<System.Threading.Thread>();
            WrapperList = new List<BCPSourceSQLToTableWrapper>();
            using (SqlConnection sqlConContext = new SqlConnection("context connection=true"))
            {
                sbError = new StringBuilder();
                ErrorMessage = String.Empty;
                sqlConContext.Open();
                try
                {
                    SQLJobList = new List<string>(sql.Split(@";".ToCharArray()));
                    if (diagnosticLevel > 0)
                    {
                        string.Concat("Setting Up BCP - SQL Statements Recieved: ", SQLJobList.Count.ToString()).SQLPipePrintImmediate(sqlConContext);
                    }
                    //SqlContext.Pipe.Send(string.Concat("Starting BCP - SQL Statements Recieved: ", SQLJobList.Count.ToString()));
                    bool IsFirst;
                    int ThreadCounter;
                    IsFirst = true;
                    ThreadCounter = 0;
                    if (threadOffsetSeconds > 360) { threadOffsetSeconds = 360; }
                    foreach (string SQLJob in SQLJobList)
                    {
                        string SQLJobTrimmed = SQLJob.Trim();
                        if (!string.IsNullOrEmpty(SQLJobTrimmed))
                        {
                            ThreadCounter += 1;
                            if (IsFirst)
                            {
                                IsFirst = false;
                            }
                            else
                            {
                                if (threadOffsetSeconds > 0)
                                {
                                    if (diagnosticLevel > 0)
                                    {
                                        string.Concat("Waiting ", threadOffsetSeconds.ToString(), "Seconds between packages.\r\n").SQLPipePrintImmediate(sqlConContext);
                                    }
                                }
                                for (int I = 0; I < threadOffsetSeconds * 10; I++)
                                {
                                    System.Threading.Thread.Sleep(100);
                                    if (progressNotificationCount > 0)
                                    {
                                        if (WrapperList.Count > 0)
                                        {
                                            foreach (BCPSourceSQLToTableWrapper WrapperInstance in WrapperList)
                                            {
                                                string Message;
                                                Message = WrapperInstance.MessageQueue_Dequeue();
                                                while (Message != null)
                                                {
                                                    if (diagnosticLevel > 1)
                                                    {
                                                        Message.SQLPipePrintImmediate(sqlConContext);
                                                    }
                                                    Message = WrapperInstance.MessageQueue_Dequeue();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if (diagnosticLevel > 2)
                            {
                                string.Concat("Setup BCP Thread for: \r\n", SQLJobTrimmed.Trim(), "\r\n\r\n").SQLPipePrintImmediate(sqlConContext);
                            }
                            //SqlContext.Pipe.Send(@"Starting BCP Thread.");
                            Wrapper = new BCPSourceSQLToTableWrapper();
                            Wrapper.ProgressNotificationCount = progressNotificationCount;
                            Wrapper.BCPPackageName = String.Concat(@"BCP_", ThreadCounter.ToString().PadLeft(2, '0'));
                            Wrapper.TimeoutSeconds = timeoutSeconds;
                            Wrapper.BCPBatchSize = batchSizeBCP;
                            Wrapper.SourceConnection = sourceConnection;
                            Wrapper.SQL = SQLJobTrimmed;
                            Wrapper.DestinationConnection = destinationConnection;
                            Wrapper.DestinationTable = destinationTable;

                            TS = new System.Threading.ThreadStart(Wrapper.BCPSourceSQLToTable);
                            RunThread = new System.Threading.Thread(TS);
                            RunThreadList.Add(RunThread);
                            WrapperList.Add(Wrapper);
                            RunThread.Start();
                        }
                    }
                    bool IsRunning;
                    IsRunning = true;
                    while (IsRunning)
                    {
                        IsRunning = false;
                        foreach (System.Threading.Thread RunningThread in RunThreadList)
                        {
                            if (RunningThread.IsAlive)
                            {
                                IsRunning = true;
                            }
                        }
                        System.Threading.Thread.Sleep(100);
                        if (progressNotificationCount > 0)
                        {
                            if (WrapperList.Count > 0)
                            {
                                foreach (BCPSourceSQLToTableWrapper WrapperInstance in WrapperList)
                                {
                                    string Message;
                                    Message = WrapperInstance.MessageQueue_Dequeue();
                                    if (diagnosticLevel > 0)
                                    {
                                        while (Message != null)
                                        {
                                            Message.SQLPipePrintImmediate(sqlConContext);
                                            Message = WrapperInstance.MessageQueue_Dequeue();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    foreach (System.Threading.Thread RunningThread in RunThreadList)
                    {
                        RunningThread.Join();
                        if (diagnosticLevel > 0)
                        {
                            SqlContext.Pipe.Send(@"BCP Thread Complete");
                        }
                    }
                    foreach (BCPSourceSQLToTableWrapper CompletedWrapper in WrapperList)
                    {
                        if (CompletedWrapper.ErrorMessage != String.Empty)
                        {
                            SqlContext.Pipe.Send(CompletedWrapper.ErrorMessage);
                            sbError.AppendLine(string.Empty).AppendLine(CompletedWrapper.ErrorMessage).AppendLine(string.Empty);
                        }
                        sbError.AppendLine(CompletedWrapper.Status());
                    }
                    ErrorMessage = sbError.ToString();
                    if (WrapperList.Count > 0)
                    {
                        if (diagnosticLevel > 0)
                        {
                            SqlDataRecord record = new SqlDataRecord(
                            new SqlMetaData("BCPPackage", SqlDbType.VarChar, 100),
                            new SqlMetaData("NumRecords", SqlDbType.BigInt),
                            new SqlMetaData("HasError", SqlDbType.Bit),
                            new SqlMetaData("SQLStatement", SqlDbType.Text),
                            new SqlMetaData("ProcessingError", SqlDbType.Text));
                            SqlContext.Pipe.SendResultsStart(record);
                            foreach (BCPSourceSQLToTableWrapper CompletedWrapper in WrapperList)
                            {
                                record.SetString(0, CompletedWrapper.BCPPackageName);
                                record.SetInt64(1, CompletedWrapper.RowsCopiedCount);
                                record.SetSqlBoolean(2, (CompletedWrapper.ErrorMessage != String.Empty));
                                record.SetString(3, CompletedWrapper.SQL);
                                record.SetString(4, CompletedWrapper.ErrorMessage);
                                SqlContext.Pipe.SendResultsRow(record);
                            }
                            SqlContext.Pipe.SendResultsEnd();
                        }
                    }
                }
                catch (Exception ex)
                {
                    SqlContext.Pipe.Send(ex.ToString());
                    ErrorMessage = String.Concat(ErrorMessage, "\r\n", ex.ToString());
                    foreach (BCPSourceSQLToTableWrapper WrapperToAbort in WrapperList)
                    {
                        try
                        {
                            WrapperToAbort.TryCancelRunningCommand();
                        }
                        catch
                        {

                        }
                    }
                    foreach (System.Threading.Thread ThreadInstance in RunThreadList)
                    {
                        try
                        {
                            ThreadInstance.Abort();
                        }
                        catch
                        {

                        }
                    }
                }
                finally
                {
                    foreach (System.Threading.Thread ThreadInstance in RunThreadList)
                    {
                        try
                        {
                            if (ThreadInstance.IsAlive)
                            {
                                ThreadInstance.Abort();
                            }
                        }
                        catch
                        {

                        }
                    }
                    sqlConContext.Close();
                }
            }
        }

        public static void BCPSourceSQLToTable(string sql, string destinationConnection, string destinationTable, out string ErrorMessage)
        {

            //WindowsIdentity currentIdentity = SqlContext.WindowsIdentity;
            //WindowsImpersonationContext impersonatedIdentity = currentIdentity.Impersonate();
            ErrorMessage = string.Empty;
            ErrorMessage = @"Starting";
            try
            {
                DataTable source = new DataTable();
                ErrorMessage = @"Filling";
                using (SqlConnection cn = new SqlConnection("context connection=true"))
                {
                    cn.Open();
                    ErrorMessage = string.Concat(cn.ConnectionString, "\r\n");
                    SqlCommand cmd = new SqlCommand(sql, cn);
                    SqlDataAdapter da = new SqlDataAdapter(sql, cn);
                    da.Fill(source);
                    cn.Close();
                }
                ErrorMessage = @"BCP Running";
                using (SqlConnection conn = new SqlConnection(destinationConnection))
                {
                    conn.Open();
                    SqlBulkCopy bc = new SqlBulkCopy(conn);
                    bc.DestinationTableName = destinationTable;
                    bc.BatchSize = source.Rows.Count;
                    bc.WriteToServer(source);
                    conn.Close();
                }
                ErrorMessage = @"Complete";
            }
            catch (Exception ex)
            {
                ErrorMessage = string.Concat(ErrorMessage, "\r\n", ex.ToString());
                return;
            }
            finally
            {
                //impersonatedIdentity.Undo();
            }
            ErrorMessage = string.Empty;
        }

    }
}
