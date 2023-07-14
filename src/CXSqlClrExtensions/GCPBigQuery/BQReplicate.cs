using ChillX.Core.Structures;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CXSqlClrExtensions.GCPBigQuery
{
    public static class BQReplicate
    {

        //public const string CreatedByVersionFieldName = @"CreatedByVersion";
        //public const string ModifiedByVersionFieldName = @"ModifiedByVersion";


        //private string Config_SourceDBConnectionString
        //{
        //    get
        //    {
        //        return System.Configuration.ConfigurationManager.AppSettings[@"SourceDBConnectionString"];
        //    }
        //}

        //private string Config_BQAuthFile
        //{
        //    get
        //    {
        //        return System.Configuration.ConfigurationManager.AppSettings[@"BQAUth"];
        //    }
        //}
        //private string Config_BQProjectID
        //{
        //    get
        //    {
        //        return System.Configuration.ConfigurationManager.AppSettings[@"BQProject"];
        //    }
        //}

        //private GoogleCredential m_BQCredential = null;
        //private GoogleCredential BQCredential
        //{
        //    get
        //    {
        //        if (m_BQCredential == null)
        //        {
        //            m_BQCredential = GoogleCredential.FromFile(Config_BQAuthFile);
        //        }
        //        return m_BQCredential;
        //    }
        //}


        //private BigQueryClient m_BQClient = null;
        //private BigQueryClient BQClient
        //{
        //    get
        //    {
        //        if (m_BQClient == null)
        //        {
        //            m_BQClient = BigQueryClient.Create(Config_BQProjectID, BQCredential);
        //        }
        //        return m_BQClient;
        //    }
        //}

        //private static object SyncRoot = new object();

        public static void SetupDB()
        {
            using (SqlConnection sqlConContext = new SqlConnection("context connection=true"))
            {
                sqlConContext.Open();
                StringBuilder sbSQL = new StringBuilder();
                string SQL;
                SQL = @"DROP PROCEDURE if exists [dbo].[syssp_ViewSchema]
GO";
                using (SqlCommand cmd = new SqlCommand(SQL, sqlConContext))
                {
                    cmd.ExecuteNonQuery();
                }

                SQL = @"
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [dbo].[syssp_ViewSchema]
	-- Add the parameters for the stored procedure here
	@DatabaseName varchar(100),
	@SchemaName varchar(50),
	@TableName varchar(100)
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	set @SchemaName = isnull(@SchemaName,'dbo')
	if trim(@SchemaName) = ''
	begin
		set @SchemaName = 'dbo'
	end
	declare @TableSysName Sysname
	set @TableSysName = @TableName
	declare @SQL nvarchar(max)

	set @SQL ='select v.TABLE_SCHEMA as [schema], v.TABLE_NAME as [table], v.TABLE_NAME as [table_desc]
	,c.COLUMN_NAME as [column], c.DATA_TYPE as [datatype], isnull(c.CHARACTER_MAXIMUM_LENGTH,0) as [datalength]
	,isnull(c.NUMERIC_PRECISION,0) as [dataprecision], isnull(c.NUMERIC_SCALE,0) as [datascale], 0 as [CalculatedColumn]
	,c.COLUMN_NAME as [column_desc]
	from '+@DatabaseName+'.INFORMATION_SCHEMA.VIEWS v
	join '+@DatabaseName+'.INFORMATION_SCHEMA.COLUMNS c on c.TABLE_SCHEMA = v.TABLE_SCHEMA
	and c.TABLE_NAME = v.TABLE_NAME
	where v.TABLE_SCHEMA = '''+@SchemaName+'''
	and v.TABLE_NAME = '''+@TableSysName+'''
	order by v.TABLE_SCHEMA, v.TABLE_NAME, c.ORDINAL_POSITION'

	EXECUTE sp_executesql @SQL

END
";
                using (SqlCommand cmd = new SqlCommand(SQL, sqlConContext))
                {
                    cmd.ExecuteNonQuery();
                }

            }
        }

        public static void GCPBQExecSQL(int diagnosticLevel, string CredentialKey, string BQProjectID, string SQL)
        {
            using (SqlConnection sqlConnectionContext = new SqlConnection("context connection=true"))
            {
                sqlConnectionContext.Open();
                try
                {
                    if (diagnosticLevel > 0)
                    {
                        string.Concat(@"Executing SQL").SQLPipePrintImmediate(sqlConnectionContext);
                    }


                    string BQCredentialJSon;
                    CXSQLExt.CredentialRetrieveInternal(sqlConnectionContext, CredentialKey, out BQCredentialJSon);
                    if (string.IsNullOrEmpty(BQCredentialJSon))
                    {
                        throw new InvalidOperationException(string.Concat(@"GCP BQ Credentials not found for: ", CredentialKey));
                    }
                    GoogleCredential BQCredential = GoogleCredential.FromJson(BQCredentialJSon);
                    BigQueryClient BQClient = BigQueryClient.Create(BQProjectID, BQCredential);
                    BigQueryParameter[] parameters = null;
                    BigQueryResults results = BQClient.ExecuteQuery(SQL, parameters);

                    if (diagnosticLevel > 0)
                    {
                        @"SQL Exec Complete:".SQLPipePrintImmediate(sqlConnectionContext);
                        @"------------------------------------------".SQLPipePrintImmediate(sqlConnectionContext);
                        SQL.SQLPipePrintImmediate(sqlConnectionContext);
                        @"------------------------------------------".SQLPipePrintImmediate(sqlConnectionContext);
                    }
                }
                catch (Exception ex)
                {
                    @"SQL Exec Error:".SQLPipePrintImmediate(sqlConnectionContext);
                    @"------------------------------------------".SQLPipePrintImmediate(sqlConnectionContext);
                    SQL.SQLPipePrintImmediate(sqlConnectionContext);
                    @"------------------------------------------".SQLPipePrintImmediate(sqlConnectionContext);
                    @"BigQuery Exception:".SQLPipePrintImmediate(sqlConnectionContext);
                    ex.ToString().SQLPipePrintImmediate(sqlConnectionContext);
                }
            }
        }
        public static void GCPBQDropSchema(int diagnosticLevel, string CredentialKey, string BQProjectID, string destinationDataSet, string destinationTable)
        {
            using (SqlConnection sqlConnectionContext = new SqlConnection("context connection=true"))
            {
                string SQL_DropCreate = string.Empty;
                sqlConnectionContext.Open();
                try
                {
                    if (diagnosticLevel > 0)
                    {
                        string.Concat(@"Dropping BigQuery Schema: `", BQProjectID, @".", destinationDataSet, @".", destinationTable, @"`").SQLPipePrintImmediate(sqlConnectionContext);
                    }

                    StringBuilder sbSQL = new StringBuilder();
                    sbSQL.Append(@"drop table if exists `").Append(BQProjectID).Append(@".").Append(destinationDataSet).Append(@".").Append(destinationTable).AppendLine(@"`;");
                    SQL_DropCreate = sbSQL.ToString();

                    string BQCredentialJSon;
                    CXSQLExt.CredentialRetrieveInternal(sqlConnectionContext, CredentialKey, out BQCredentialJSon);
                    if (string.IsNullOrEmpty(BQCredentialJSon))
                    {
                        throw new InvalidOperationException(string.Concat(@"GCP BQ Credentials not found for: ", CredentialKey));
                    }
                    GoogleCredential BQCredential = GoogleCredential.FromJson(BQCredentialJSon);
                    BigQueryClient BQClient = BigQueryClient.Create(BQProjectID, BQCredential);
                    BigQueryParameter[] parameters = null;
                    BigQueryResults results = BQClient.ExecuteQuery(SQL_DropCreate, parameters);

                    if (diagnosticLevel > 0)
                    {
                        @"Bigquery Schema Syncronized. DDL is below:".SQLPipePrintImmediate(sqlConnectionContext);
                        @"------------------------------------------".SQLPipePrintImmediate(sqlConnectionContext);
                        SQL_DropCreate.SQLPipePrintImmediate(sqlConnectionContext);
                        @"------------------------------------------".SQLPipePrintImmediate(sqlConnectionContext);
                    }
                }
                catch (Exception ex)
                {
                    @"Error Syncronizing Schema: DDL is below:".SQLPipePrintImmediate(sqlConnectionContext);
                    @"------------------------------------------".SQLPipePrintImmediate(sqlConnectionContext);
                    SQL_DropCreate.SQLPipePrintImmediate(sqlConnectionContext);
                    @"------------------------------------------".SQLPipePrintImmediate(sqlConnectionContext);
                    @"BigQuery Exception:".SQLPipePrintImmediate(sqlConnectionContext);
                    ex.ToString().SQLPipePrintImmediate(sqlConnectionContext);
                }
            }
        }

        public static void GCPBQSyncSchema(int diagnosticLevel, string CredentialKey, string BQProjectID, string SourceDB, string SourceSchema, string SourceTable, string destinationDataSet, string destinationTable, string PartitionBy, string CluseterBy)
        {
            //using (SqlConnection sqlConnectionContext = new SqlConnection(CXSQLExt.ControlDB.LocalDBNameToConnectionString(3600)))
            using (SqlConnection sqlConnectionContext = new SqlConnection("context connection=true"))
            {
                string SQL_DropCreate = string.Empty;
                sqlConnectionContext.Open();
                try
                {
                    if (diagnosticLevel > 0)
                    {
                        string.Concat(@"Syncronizing BigQuery Schema - Source Schema: [", SourceDB, @"].[", SourceSchema, @"],.[", SourceTable, @"] to Destination: `", BQProjectID, @".", destinationDataSet, @".", destinationTable, @"`").SQLPipePrintImmediate(sqlConnectionContext);
                    }
                    string BQCredentialJSon;
                    CXSQLExt.CredentialRetrieveInternal(sqlConnectionContext, CredentialKey, out BQCredentialJSon);
                    if (string.IsNullOrEmpty(BQCredentialJSon))
                    {
                        throw new InvalidOperationException(string.Concat(@"GCP BQ Credentials not found for: ", CredentialKey));
                    }

                    GoogleCredential BQCredential = GoogleCredential.FromJson(BQCredentialJSon);
                    BigQueryClient BQClient = BigQueryClient.Create(BQProjectID, BQCredential);

                    string connectionString;
                    DBSchema sourceTableSchema;
                    connectionString = CXSQLExt.ControlDB.LocalDBNameToConnectionString();
                    sourceTableSchema = new DBSchema(SourceDB, SourceSchema, SourceTable);
                    sourceTableSchema.LoadSchema();

                    SQL_DropCreate = sourceTableSchema.BQCreateTableSQL(BQProjectID, destinationDataSet, destinationTable, PartitionBy, CluseterBy);
                    BigQueryParameter[] parameters = null;
                    BigQueryResults results = BQClient.ExecuteQuery(SQL_DropCreate, parameters);

                    if (diagnosticLevel > 0)
                    {
                        @"Bigquery Schema Syncronized. DDL is below:".SQLPipePrintImmediate(sqlConnectionContext);
                        @"------------------------------------------".SQLPipePrintImmediate(sqlConnectionContext);
                        SQL_DropCreate.SQLPipePrintImmediate(sqlConnectionContext);
                        @"------------------------------------------".SQLPipePrintImmediate(sqlConnectionContext);
                    }

                }
                catch (Exception ex)
                {
                    @"Error Syncronizing Schema: DDL is below:".SQLPipePrintImmediate(sqlConnectionContext);
                    @"------------------------------------------".SQLPipePrintImmediate(sqlConnectionContext);
                    SQL_DropCreate.SQLPipePrintImmediate(sqlConnectionContext);
                    @"------------------------------------------".SQLPipePrintImmediate(sqlConnectionContext);
                    @"BigQuery Exception:".SQLPipePrintImmediate(sqlConnectionContext);
                    ex.ToString().SQLPipePrintImmediate(sqlConnectionContext);
                }
            }
        }

        public static void GCPBQSyncDataSet(int diagnosticLevel, string CredentialKey, string BQProjectID, string SourceDB, string SourceSchema, string SourceTable, string destinationDataSet, string destinationTable, string CreatedByVersionFieldName, string ModifiedByVersionFieldName, string UniqueKeyFieldName, string whereClause, int NumGCPThreads = 8)
        {
            Stopwatch SWProcess;
            Stopwatch SWNotification;
            long SubmittedCounter;
            long PendingCounter;
            long CompletedCounter;
            SWProcess = new Stopwatch();
            SWNotification = new Stopwatch();
            SWProcess.Start();
            SWNotification.Start();
            //using (SqlConnection sqlConContext = new SqlConnection(CXSQLExt.ControlDB.LocalDBNameToConnectionString(3600)))
            using (SqlConnection sqlConContext = new SqlConnection("context connection=true"))
            {
                sqlConContext.Open();
                if (diagnosticLevel > 0)
                {
                    string.Concat(@"Syncronizing BigQuery Data - Source Schema: [", SourceDB, @"].[", SourceSchema, @"],.[", SourceTable, @"] to Destination: `", BQProjectID, @".", destinationDataSet, @".", destinationTable, @"`").SQLPipePrintImmediate(sqlConContext);
                    string.Concat(@"Using GCP Credentials for Key: ", CredentialKey).SQLPipePrintImmediate(sqlConContext);
                }
                string BQCredentialJSon;
                CXSQLExt.CredentialRetrieveInternal(sqlConContext, CredentialKey, out BQCredentialJSon);
                if (string.IsNullOrEmpty(BQCredentialJSon))
                {
                    throw new InvalidOperationException(string.Concat(@"GCP BQ Credentials not found for: ", CredentialKey));
                }

                GoogleCredential BQCredential = GoogleCredential.FromJson(BQCredentialJSon);

                BigQueryClient BQClient = BigQueryClient.Create(BQProjectID, BQCredential);

                ThreadSafeQueue<BQInsertJob> PendingWorkQueue = new ThreadSafeQueue<BQInsertJob>();
                ThreadSafeQueue<BQInsertJob> FailedWorkQueue = new ThreadSafeQueue<BQInsertJob>();

                DBSchema sourceTableSchema;
                string SQL_GetLocalVersions;
                try
                {

                    using (SqlConnection DBConnSource = new SqlConnection(SourceDB.LocalDBNameToConnectionString(3600)))
                    {
                        DBConnSource.Open();
                        sourceTableSchema = new DBSchema(SourceDB, SourceSchema, SourceTable);
                        sourceTableSchema.LoadSchema();

                        if (diagnosticLevel > 0)
                        {
                            string.Concat(@"Source Schema Loaded. Field Count: ", sourceTableSchema.Schema.Count.ToString());
                            if (diagnosticLevel > 3)
                            {
                                foreach (KeyValuePair<string,DBColumnType> Pair in sourceTableSchema.Schema)
                                {
                                    string.Concat(Pair.Value.ColumnName.ToString()).SQLPipePrintImmediate(sqlConContext);
                                }
                            }
                            string.Concat(@"Checking batch level data versions of local vs remote: ").SQLPipePrintImmediate(sqlConContext);
                        }
                        SQL_GetLocalVersions = string.Concat(@"select max([", CreatedByVersionFieldName, "]) as [", CreatedByVersionFieldName, "], max([", ModifiedByVersionFieldName, "]) as [", ModifiedByVersionFieldName, "] from [", SourceSchema, "].[", SourceTable, @"] where ", whereClause);
                        using (SqlCommand DBCmdGetVersions = new SqlCommand(SQL_GetLocalVersions, DBConnSource))
                        {
                            DBCmdGetVersions.CommandTimeout = 3600;
                            using (SqlDataReader DataReaderGetVersions = DBCmdGetVersions.ExecuteReader())
                            {
                                if (DataReaderGetVersions.Read())
                                {
                                    int CreatedByVersion_Local;
                                    int ModifiedByVersion_Local;
                                    int CreatedByVersion_Remote = -1;
                                    int ModifiedByVersion_Remote = -1;
                                    CreatedByVersion_Local = DataReaderGetVersions[CreatedByVersionFieldName].ToInt(0);
                                    ModifiedByVersion_Local = (int)DataReaderGetVersions[ModifiedByVersionFieldName].ToInt(0);

                                    string FinalTableName;
                                    string BQSQL;

                                    ThreadedBQInsertController BQInsertController;
                                    List<ThreadedBQInsertController> BQInsertControllerList = new List<ThreadedBQInsertController>();

                                    BigQueryClient BQDestinationClient;

                                    BigQueryInsertRow BQNewRow;
                                    List<BigQueryInsertRow> BQRowList;
                                    BQInsertJob BQJob;
                                    bool HasJob;

                                    BQDestinationClient = BQClient;
                                    BigQueryDataset BQDataSet = BQDestinationClient.GetDataset(destinationDataSet);
                                    BigQueryTable table;
                                    table = BQDestinationClient.GetTable(BQProjectID, destinationDataSet, destinationTable);
                                    //
                                    string SQL_GetRemoteVersions = string.Concat("SELECT max(", CreatedByVersionFieldName, ") as ", CreatedByVersionFieldName, ", max(", ModifiedByVersionFieldName, ") as ", ModifiedByVersionFieldName, $" FROM {table} where ", whereClause);
                                    BigQueryParameter[] parameters = null;
                                    BigQueryResults results = BQDestinationClient.ExecuteQuery(SQL_GetRemoteVersions, parameters);

                                    foreach (BigQueryRow row in results)
                                    {
                                        CreatedByVersion_Remote = row[CreatedByVersionFieldName].ToInt(-1);
                                        ModifiedByVersion_Remote = row[ModifiedByVersionFieldName].ToInt(-1);
                                        break;
                                    }

                                    if (diagnosticLevel > 0)
                                    {
                                        string.Concat(@"Created By Version Local: ", CreatedByVersion_Local.ToString(), @" Remote: ", CreatedByVersion_Remote.ToString(), @" - Modified By Version Local: ", ModifiedByVersion_Local.ToString(), @" Remote: ", ModifiedByVersion_Remote.ToString()).SQLPipePrintImmediate(sqlConContext);
                                    }

                                    bool IsFirst;
                                    StringBuilder sb_SQL = new StringBuilder();

                                    if ((ModifiedByVersion_Remote >= 0) && (sourceTableSchema.Schema.ContainsKey(UniqueKeyFieldName.ToLower())) && (ModifiedByVersion_Local > ModifiedByVersion_Remote))
                                    {
                                        BigQueryTable table_UniqueKeys;

                                        BQSQL = sourceTableSchema.BQCreateKeyTableSQL(BQProjectID, destinationDataSet, destinationTable, UniqueKeyFieldName, out FinalTableName);
                                        if (diagnosticLevel > 0)
                                        {
                                            string.Concat(@"Creating Key temp table ", FinalTableName).SQLPipePrintImmediate(sqlConContext);
                                        }
                                        if (diagnosticLevel > 3)
                                        {
                                            @"DDL Follows:".SQLPipePrintImmediate(sqlConContext);
                                            BQSQL.SQLPipePrintImmediate(sqlConContext);
                                        }
                                        BQExecuteSQL(BQClient, BQSQL);

                                        table_UniqueKeys = BQDestinationClient.GetTable(BQProjectID, destinationDataSet, FinalTableName);

                                        DBColumnType ColumnDefinition;
                                        ColumnDefinition = sourceTableSchema.Schema[UniqueKeyFieldName.ToLower()];
                                        string ColumnName;
                                        ColumnName = ColumnDefinition.ColumnDescription;
                                        sb_SQL.Append(@"Select ").Append(ColumnDefinition.ColumnDescription).AppendLine(@" ");
                                        sb_SQL.Append(@" FROM [").Append(SourceSchema).Append(@"].").Append("[").Append(SourceTable).Append(@"] ");
                                        sb_SQL.AppendLine(@" ");
                                        sb_SQL.Append(@" where [").Append(ModifiedByVersionFieldName).Append(@"] > ").Append(ModifiedByVersion_Remote.ToString()).Append(@" ");
                                        sb_SQL.Append(@" and ").Append(whereClause);
                                        string SQL_GetUpdateKeys;
                                        SQL_GetUpdateKeys = sb_SQL.ToString();
                                        int RetryCount;
                                        bool IsSuccess;
                                        if (diagnosticLevel > 0)
                                        {
                                            string.Concat(@"Syncronizing updates using temp table ", table_UniqueKeys.ToString()).SQLPipePrintImmediate(sqlConContext);
                                        }
                                        SWNotification.Restart();
                                        SubmittedCounter = 0;
                                        PendingCounter = 0;
                                        CompletedCounter = 0;
                                        using (SqlCommand DBCmdSourceData = new SqlCommand(SQL_GetUpdateKeys, DBConnSource))
                                        {
                                            DBCmdSourceData.CommandTimeout = 3600;
                                            using (SqlDataReader DataReaderSourceData = DBCmdSourceData.ExecuteReader())
                                            {
                                                for (int I = 0; I < NumGCPThreads; I++)
                                                {
                                                    BQInsertController = new ThreadedBQInsertController(PendingWorkQueue, FailedWorkQueue, BQCredentialJSon, BQProjectID, destinationDataSet, FinalTableName, UniqueKeyFieldName);
                                                    BQInsertController.Run();
                                                    BQInsertControllerList.Add(BQInsertController);
                                                }

                                                try
                                                {
                                                    BQRowList = new List<BigQueryInsertRow>();
                                                    while (DataReaderSourceData.Read())
                                                    {
                                                        BQNewRow = new BigQueryInsertRow();
                                                        BQNewRow.Add(ColumnName, DataReaderSourceData[ColumnName].SQLToBQValue(ColumnDefinition.DataType, null));
                                                        BQRowList.Add(BQNewRow);

                                                        RetryCount = 0;
                                                        if (BQRowList.Count >= 500)
                                                        {
                                                            while (PendingWorkQueue.Count > (NumGCPThreads * 2))
                                                            {
                                                                System.Threading.Thread.Sleep(100);
                                                                if (SWNotification.Elapsed.TotalSeconds > 10)
                                                                {
                                                                    SWNotification.Restart();
                                                                    if (diagnosticLevel > 2)
                                                                    {
                                                                        CompletedCounter = 0;
                                                                        foreach (ThreadedBQInsertController BQController in BQInsertControllerList)
                                                                        {
                                                                            CompletedCounter += BQController.CompletedCounter.ValueInterlocked;
                                                                        }
                                                                        PendingCounter = SubmittedCounter - CompletedCounter;
                                                                        string.Concat(@"Streaming to Big Query: Submitted: ", SubmittedCounter.ToString(), @" - Completed: ", CompletedCounter.ToString(), @" - Streaming Buffer: ", PendingCounter.ToString()).SQLPipePrintImmediate(sqlConContext);
                                                                    }
                                                                }
                                                            }
                                                            SubmittedCounter += BQRowList.Count;
                                                            BQJob = new BQInsertJob(BQRowList);
                                                            PendingWorkQueue.Enqueue(BQJob);
                                                            //BQInsertRows(table_UniqueKeys, BQRowList);
                                                            BQRowList = new List<BigQueryInsertRow>();
                                                        }
                                                        if (SWNotification.Elapsed.TotalSeconds > 10)
                                                        {
                                                            SWNotification.Restart();
                                                            if (diagnosticLevel > 2)
                                                            {
                                                                CompletedCounter = 0;
                                                                foreach (ThreadedBQInsertController BQController in BQInsertControllerList)
                                                                {
                                                                    CompletedCounter += BQController.CompletedCounter.ValueInterlocked;
                                                                }
                                                                PendingCounter = SubmittedCounter - CompletedCounter;
                                                                string.Concat(@"Streaming to Big Query: Submitted: ", SubmittedCounter.ToString(), @" - Completed: ", CompletedCounter.ToString(), @" - Streaming Buffer: ", PendingCounter.ToString()).SQLPipePrintImmediate(sqlConContext);
                                                            }
                                                        }
                                                    }
                                                    RetryCount = 0;
                                                    if (BQRowList.Count > 0)
                                                    {
                                                        SubmittedCounter += BQRowList.Count;
                                                        BQJob = new BQInsertJob(BQRowList);
                                                        PendingWorkQueue.Enqueue(BQJob);
                                                        //BQInsertRows(table_UniqueKeys, BQRowList);
                                                        BQRowList = new List<BigQueryInsertRow>();
                                                    }
                                                    while (PendingWorkQueue.Count > 0)
                                                    {
                                                        System.Threading.Thread.Sleep(100);
                                                        if (SWNotification.Elapsed.TotalSeconds > 10)
                                                        {
                                                            SWNotification.Restart();
                                                            if (diagnosticLevel > 2)
                                                            {
                                                                CompletedCounter = 0;
                                                                foreach (ThreadedBQInsertController BQController in BQInsertControllerList)
                                                                {
                                                                    CompletedCounter += BQController.CompletedCounter.ValueInterlocked;
                                                                }
                                                                PendingCounter = SubmittedCounter - CompletedCounter;
                                                                string.Concat(@"Streaming to Big Query: Submitted: ", SubmittedCounter.ToString(), @" - Completed: ", CompletedCounter.ToString(), @" - Streaming Buffer: ", PendingCounter.ToString()).SQLPipePrintImmediate(sqlConContext);
                                                            }
                                                        }
                                                    }
                                                    BQJob = FailedWorkQueue.DeQueue(out HasJob);
                                                    while (HasJob)
                                                    {
                                                        PendingWorkQueue.Enqueue(new BQInsertJob(BQJob.RowList));
                                                        BQJob = FailedWorkQueue.DeQueue(out HasJob);
                                                    }
                                                    while (PendingWorkQueue.Count > 0)
                                                    {
                                                        System.Threading.Thread.Sleep(100);
                                                    }
                                                    foreach (ThreadedBQInsertController Controller in BQInsertControllerList)
                                                    {
                                                        Controller.Shutdown();
                                                    }
                                                    HasJob = true;
                                                    while (HasJob)
                                                    {
                                                        HasJob = false;
                                                        foreach (ThreadedBQInsertController Controller in BQInsertControllerList)
                                                        {
                                                            if (Controller.IsRunning)
                                                            {
                                                                HasJob = true;
                                                            }
                                                        }
                                                        if (HasJob) { System.Threading.Thread.Sleep(100); }
                                                    }
                                                    if (diagnosticLevel > 1)
                                                    {
                                                        CompletedCounter = 0;
                                                        foreach (ThreadedBQInsertController BQController in BQInsertControllerList)
                                                        {
                                                            CompletedCounter += BQController.CompletedCounter.ValueInterlocked;
                                                        }
                                                        PendingCounter = SubmittedCounter - CompletedCounter;
                                                        string.Concat(@"Streaming to Big Query: Submitted: ", SubmittedCounter.ToString(), @" - Completed: ", CompletedCounter.ToString(), @" - Streaming Buffer: ", PendingCounter.ToString()).SQLPipePrintImmediate(sqlConContext);
                                                    }
                                                }
                                                finally
                                                {
                                                    foreach (ThreadedBQInsertController Controller in BQInsertControllerList)
                                                    {
                                                        Controller.Abort();
                                                    }
                                                }
                                            }
                                        }
                                        sb_SQL = new StringBuilder();
                                        sb_SQL.Append(@"Delete FROM ").Append(BQProjectID).Append(@".").Append(destinationDataSet).Append(@".").AppendLine(destinationTable);
                                        sb_SQL.Append(@"Where ").Append(ColumnName).Append(@" in (Select ").Append(ColumnName).Append(@" FROM ").Append(BQProjectID).Append(@".").Append(destinationDataSet).Append(@".").Append(FinalTableName).AppendLine(@");");

                                        string SQL_BQ_DeleteUpdated;
                                        SQL_BQ_DeleteUpdated = sb_SQL.ToString();
                                        BQDestinationClient = BQClient;

                                        results = BQDestinationClient.ExecuteQuery(SQL_BQ_DeleteUpdated, null);

                                        if (diagnosticLevel > 0)
                                        {
                                            string.Concat(@"Updates syncronized dropping temp table ", table_UniqueKeys.ToString()).SQLPipePrintImmediate(sqlConContext);
                                        }

                                        BQSQL = sourceTableSchema.BQDropTableSQL(BQProjectID, destinationDataSet, FinalTableName);
                                        BQExecuteSQL(BQClient, BQSQL);

                                    }

                                    sb_SQL = new StringBuilder();
                                    List<DBColumnType> ColumnDefinitionList = new List<DBColumnType>();
                                    IsFirst = true;
                                    sb_SQL.Append(@"Select ");
                                    foreach (DBColumnType ColumnDefinition in sourceTableSchema.Schema.Values)
                                    {
                                        if (ColumnDefinition.DataType != DBColumnType.Enum_DataType.NotImplemented)
                                        {
                                            if (IsFirst)
                                            {
                                                IsFirst = false;
                                            }
                                            else
                                            {
                                                sb_SQL.Append(@", ");
                                            }
                                            sb_SQL.Append(@"[").Append(ColumnDefinition.ColumnDescription).Append(@"]");
                                            ColumnDefinitionList.Add(ColumnDefinition);
                                        }
                                    }
                                    sb_SQL.AppendLine(@" ");
                                    sb_SQL.Append(@" from [").Append(SourceSchema).Append(@"].").Append("[").Append(SourceTable).Append(@"] ");
                                    sb_SQL.AppendLine(@" ");
                                    sb_SQL.Append(@" where ([").Append(CreatedByVersionFieldName).Append(@"] > ").Append(CreatedByVersion_Remote.ToString()).Append(@" ");
                                    sb_SQL.Append(@" or [").Append(ModifiedByVersionFieldName).Append(@"] > ").Append(ModifiedByVersion_Remote.ToString()).Append(@") ");
                                    sb_SQL.Append(@" and ").Append(whereClause);
                                    string SQL_GetSourceData;
                                    SQL_GetSourceData = sb_SQL.ToString();


                                    BigQueryTable InsertTable;
                                    BQSQL = sourceTableSchema.BQCreateInsertTableSQL(BQProjectID, destinationDataSet, destinationTable, out FinalTableName);
                                    if (diagnosticLevel > 0)
                                    {
                                        string.Concat(@"Creating Data temp table ", FinalTableName).SQLPipePrintImmediate(sqlConContext);
                                    }
                                    if (diagnosticLevel > 3)
                                    {
                                        @"DDL Follows:".SQLPipePrintImmediate(sqlConContext);
                                        BQSQL.SQLPipePrintImmediate(sqlConContext);
                                    }
                                    BQExecuteSQL(BQClient, BQSQL);

                                    InsertTable = BQDestinationClient.GetTable(BQProjectID, destinationDataSet, FinalTableName);
                                    PendingWorkQueue.Clear();
                                    FailedWorkQueue.Clear();
                                    BQInsertControllerList.Clear();

                                    if (diagnosticLevel > 0)
                                    {
                                        string.Concat(@"Syncronizing new records using temp table ", InsertTable.ToString()).SQLPipePrintImmediate(sqlConContext);
                                    }
                                    SWNotification.Restart();
                                    SubmittedCounter = 0;
                                    PendingCounter = 0;
                                    CompletedCounter = 0;

                                    using (SqlCommand DBCmdSourceData = new SqlCommand(SQL_GetSourceData, DBConnSource))
                                    {
                                        DBCmdSourceData.CommandTimeout = 300;
                                        using (SqlDataReader DataReaderSourceData = DBCmdSourceData.ExecuteReader())
                                        {
                                            for (int I = 0; I < NumGCPThreads; I++)
                                            {
                                                BQInsertController = new ThreadedBQInsertController(PendingWorkQueue, FailedWorkQueue, BQCredentialJSon, BQProjectID, destinationDataSet, FinalTableName, UniqueKeyFieldName);
                                                BQInsertController.Run();
                                                BQInsertControllerList.Add(BQInsertController);
                                            }
                                            try
                                            {
                                                BQRowList = new List<BigQueryInsertRow>();
                                                while (DataReaderSourceData.Read())
                                                {
                                                    BQNewRow = new BigQueryInsertRow();
                                                    foreach (DBColumnType ColumnDefinition in ColumnDefinitionList)
                                                    {
                                                        BQNewRow.Add(ColumnDefinition.ColumnDescription, DataReaderSourceData[ColumnDefinition.ColumnDescription].SQLToBQValue(ColumnDefinition.DataType, null));
                                                    }
                                                    BQRowList.Add(BQNewRow);

                                                    if (BQRowList.Count > 500)
                                                    {
                                                        while (PendingWorkQueue.Count > (NumGCPThreads * 2))
                                                        {
                                                            System.Threading.Thread.Sleep(100);
                                                            if (SWNotification.Elapsed.TotalSeconds > 10)
                                                            {
                                                                SWNotification.Restart();
                                                                if (diagnosticLevel > 2)
                                                                {
                                                                    CompletedCounter = 0;
                                                                    foreach (ThreadedBQInsertController BQController in BQInsertControllerList)
                                                                    {
                                                                        CompletedCounter += BQController.CompletedCounter.ValueInterlocked;
                                                                    }
                                                                    PendingCounter = SubmittedCounter - CompletedCounter;
                                                                    string.Concat(@"Streaming to Big Query: Submitted: ", SubmittedCounter.ToString(), @" - Completed: ", CompletedCounter.ToString(), @" - Streaming Buffer: ", PendingCounter.ToString()).SQLPipePrintImmediate(sqlConContext);
                                                                }
                                                            }
                                                        }
                                                        SubmittedCounter += BQRowList.Count;
                                                        BQJob = new BQInsertJob(BQRowList);
                                                        PendingWorkQueue.Enqueue(BQJob);
                                                        //BQInsertRows(InsertTable, BQRowList);
                                                        BQRowList = new List<BigQueryInsertRow>();
                                                    }
                                                    if (SWNotification.Elapsed.TotalSeconds > 10)
                                                    {
                                                        SWNotification.Restart();
                                                        if (diagnosticLevel > 2)
                                                        {
                                                            CompletedCounter = 0;
                                                            foreach (ThreadedBQInsertController BQController in BQInsertControllerList)
                                                            {
                                                                CompletedCounter += BQController.CompletedCounter.ValueInterlocked;
                                                            }
                                                            PendingCounter = SubmittedCounter - CompletedCounter;
                                                            string.Concat(@"Streaming to Big Query: Submitted: ", SubmittedCounter.ToString(), @" - Completed: ", CompletedCounter.ToString(), @" - Streaming Buffer: ", PendingCounter.ToString()).SQLPipePrintImmediate(sqlConContext);
                                                        }
                                                    }
                                                }
                                                if (BQRowList.Count > 0)
                                                {
                                                    SubmittedCounter += BQRowList.Count;
                                                    BQJob = new BQInsertJob(BQRowList);
                                                    PendingWorkQueue.Enqueue(BQJob);
                                                    //BQInsertRows(InsertTable, BQRowList);
                                                    BQRowList = new List<BigQueryInsertRow>();
                                                }
                                                System.Threading.Thread.Sleep(100);
                                                while (PendingWorkQueue.Count > 0)
                                                {
                                                    System.Threading.Thread.Sleep(100);
                                                    if (SWNotification.Elapsed.TotalSeconds > 10)
                                                    {
                                                        SWNotification.Restart();
                                                        if (diagnosticLevel > 2)
                                                        {
                                                            CompletedCounter = 0;
                                                            foreach (ThreadedBQInsertController BQController in BQInsertControllerList)
                                                            {
                                                                CompletedCounter += BQController.CompletedCounter.ValueInterlocked;
                                                            }
                                                            PendingCounter = SubmittedCounter - CompletedCounter;
                                                            string.Concat(@"Streaming to Big Query: Submitted: ", SubmittedCounter.ToString(), @" - Completed: ", CompletedCounter.ToString(), @" - Streaming Buffer: ", PendingCounter.ToString()).SQLPipePrintImmediate(sqlConContext);
                                                        }
                                                    }
                                                }
                                                BQJob = FailedWorkQueue.DeQueue(out HasJob);
                                                while (HasJob)
                                                {
                                                    PendingWorkQueue.Enqueue(new BQInsertJob(BQJob.RowList));
                                                    BQJob = FailedWorkQueue.DeQueue(out HasJob);
                                                }
                                                while (PendingWorkQueue.Count > 0)
                                                {
                                                    System.Threading.Thread.Sleep(100);
                                                }
                                                foreach (ThreadedBQInsertController Controller in BQInsertControllerList)
                                                {
                                                    Controller.Shutdown();
                                                }
                                                HasJob = true;
                                                while (HasJob)
                                                {
                                                    HasJob = false;
                                                    foreach (ThreadedBQInsertController Controller in BQInsertControllerList)
                                                    {
                                                        if (Controller.IsRunning)
                                                        {
                                                            HasJob = true;
                                                        }
                                                    }
                                                    if (HasJob) { System.Threading.Thread.Sleep(100); }
                                                }
                                                SWNotification.Restart();
                                                if (diagnosticLevel > 1)
                                                {
                                                    CompletedCounter = 0;
                                                    foreach (ThreadedBQInsertController BQController in BQInsertControllerList)
                                                    {
                                                        CompletedCounter += BQController.CompletedCounter.ValueInterlocked;
                                                    }
                                                    PendingCounter = SubmittedCounter - CompletedCounter;
                                                    string.Concat(@"Streaming to Big Query: Submitted: ", SubmittedCounter.ToString(), @" - Completed: ", CompletedCounter.ToString(), @" - Streaming Buffer: ", PendingCounter.ToString()).SQLPipePrintImmediate(sqlConContext);
                                                }
                                                try
                                                {
                                                    BQSQL = sourceTableSchema.BQCopyInsertToMain(BQProjectID, destinationDataSet, destinationTable, FinalTableName);
                                                    BQExecuteSQL(BQClient, BQSQL);
                                                }
                                                finally
                                                {
                                                    BQSQL = sourceTableSchema.BQDropTableSQL(BQProjectID, destinationDataSet, FinalTableName);
                                                    BQExecuteSQL(BQClient, BQSQL);
                                                }
                                                SWProcess.Stop();
                                                if (diagnosticLevel > 1)
                                                {
                                                    string.Concat(@"Syncronization Complete: Time taken: ", SWProcess.Elapsed.ToString()).SQLPipePrintImmediate(sqlConContext);
                                                    string.Concat(@"Total Records:", CompletedCounter.ToString()).SQLPipePrintImmediate(sqlConContext);
                                                    string.Concat(@"Records Per Second: ", Convert.ToInt64(CompletedCounter / SWProcess.Elapsed.TotalSeconds).ToString()).SQLPipePrintImmediate(sqlConContext);
                                                }
                                            }
                                            finally
                                            {
                                                foreach (ThreadedBQInsertController Controller in BQInsertControllerList)
                                                {
                                                    Controller.Abort();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                finally
                {
                    PendingWorkQueue.Dispose();
                    FailedWorkQueue.Dispose();
                }
            }
        }

        private static void BQExecuteSQL(BigQueryClient BQClient, string SQL, BigQueryParameter[] parameters = null)
        {
            int RetryCount = 0;
            bool IsSuccess = false;
            BigQueryResults results;
            while (!IsSuccess)
            {
                try
                {
                    RetryCount += 1;
                    results = BQClient.ExecuteQuery(SQL, parameters);
                    IsSuccess = true;
                }
                catch (Exception ex)
                {
                    if (RetryCount > 40)
                    {
                        throw ex;
                    }
                    for (int I = 0; I < 15; I++)
                    {
                        System.Threading.Thread.Sleep(1000);
                    }
                }
            }
        }

        private static void BQInsertRowsOld(BigQueryTable table, List<BigQueryInsertRow> bQRowList)
        {
            int RetryCount = 0;
            bool IsSuccess = false;
            while (!IsSuccess)
            {
                try
                {
                    RetryCount += 1;
                    table.InsertRows(bQRowList);
                    IsSuccess = true;
                }
                catch (Exception ex)
                {
                    if (RetryCount > 40)
                    {
                        throw ex;
                    }
                    for (int I = 0; I < 15; I++)
                    {
                        System.Threading.Thread.Sleep(1000);
                    }
                }
            }
        }

    }
}
