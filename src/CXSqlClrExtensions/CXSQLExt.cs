using CXSqlClrExtensions.BCP;
using Microsoft.SqlServer.Server;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CXSqlClrExtensions;
using Google.Apis.Bigquery.v2.Data;
using System.Collections.Concurrent;
using System.Net;

public static class CXSQLExt
{
    /*
    Sign Assembly: https://www.sqlshack.com/impact-clr-strict-security-configuration-setting-sql-server-2017/
    "C:\Program Files (x86)\Windows Kits\10\bin\10.0.19041.0\x64\makecert.exe" -r -pe -n “CN=ChillX Root Authority” -a sha256 -sky signature -cy authority -sv C:\Temp\CXSqlClrExtensions.pvk -len 2048 -m 144 C:\Temp\CXSqlClrExtensions.cer

    "C:\Program Files (x86)\Windows Kits\10\bin\10.0.19041.0\x64\PVK2PFX.exe" -pvk CXSqlClrExtensions.pvk -spc C:\Temp\CXSqlClrExtensions.cer -pfx C:\Temp\CXSqlClrExtensions.pfx -pi <<Your Password>> -po <<Your Password>>

    "C:\Program Files (x86)\Windows Kits\10\bin\10.0.19041.0\x64\signtool.exe" sign /f CLRStringSplit.pfx /p P@ssw0rd1 CLRStringSplit.dll

    EXEC sp_configure 'show advanced options' , 1;
    RECONFIGURE;

    EXEC sp_configure 'clr enable' ,1;
    RECONFIGURE;

    usefull tools:
    For Assembly Hash: https://github.com/TheBlueSky/dotnet-hash
    Example: dotnet-hash -a sha512 -o hex C:\Windows\Microsoft.NET\Framework64\v4.0.30319\netstandard.dll

    */
    public const string ControlDB = @"CXSqlClrExtensions";

    [Microsoft.SqlServer.Server.SqlProcedure()]
    public static void BCPCopyToTableThreaded(int threadOffsetSeconds, int perThreadProgressNotificationCount, int diagnosticLevel, int timeoutSeconds, int batchSizeBCP, string sourceConnection, string sql, string BCPSplitOnValueOfField, string destinationConnection, string destinationTable, out string ErrorMessage, int MaxConcurrency = 16)
    {
        BCPExt.BCPCopyToTableThreaded(threadOffsetSeconds, perThreadProgressNotificationCount, diagnosticLevel, timeoutSeconds, batchSizeBCP, sourceConnection, sql, BCPSplitOnValueOfField, destinationConnection, destinationTable, out ErrorMessage, MaxConcurrency);
    }

    [Microsoft.SqlServer.Server.SqlProcedure()]
    public static void BCPSourceSQLToTableThreaded(int threadOffsetSeconds, int progressNotificationCount, int diagnosticLevel, int timeoutSeconds, int batchSizeBCP, string sourceConnection, string sql, string destinationConnection, string destinationTable, out string ErrorMessage)
    {
        BCPExt.BCPSourceSQLToTableThreaded(threadOffsetSeconds, progressNotificationCount, diagnosticLevel, timeoutSeconds, batchSizeBCP, sourceConnection, sql, destinationConnection, destinationTable, out ErrorMessage);
    }

    [Microsoft.SqlServer.Server.SqlProcedure()]
    public static void BCPSourceSQLToTable(string sql, string destinationConnection, string destinationTable, out string ErrorMessage)
    {
        BCPExt.BCPSourceSQLToTable(sql, destinationConnection, destinationTable, out ErrorMessage);
    }

    [Microsoft.SqlServer.Server.SqlProcedure()]
    public static void CredentialStoreSetup()
    {
        bool credentialTableExists;
        string SQL;
        credentialTableExists = false;
        using (SqlConnection sqlConContext = new SqlConnection("context connection=true"))
        {
            sqlConContext.Open();
            using (SqlCommand cmd = new SqlCommand(@"SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[CXCredential]') AND type in (N'U')", sqlConContext))
            {
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        credentialTableExists = reader[@"name"].ToString().ToLower().Trim() == @"cxcredential";
                    }
                }
            }
            if (!credentialTableExists)
            {
                SQL = @"
CREATE TABLE [dbo].[CXCredential](
	[SecretKey] [varchar](255) NOT NULL,
	[UserIdentity] [varchar](255) NOT NULL,
	[Salt] [varbinary](16) NOT NULL,
	[SecretValue] [varbinary](max) NOT NULL,
 CONSTRAINT [PK_CXCredential] PRIMARY KEY CLUSTERED 
(
	[SecretKey] ASC,
	[UserIdentity] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
";
                using (SqlCommand cmd = new SqlCommand(SQL, sqlConContext))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }
    }

    [Microsoft.SqlServer.Server.SqlProcedure()]
    public static void CredentialSave(string secretKey, string secretValue)
    {
        byte[] EncryptedContent;
        byte[] Salt;
        string SQL;
        string CurrentUser;
        SQL = @"if exists (select [SecretKey] from [dbo].[CXCredential] where [SecretKey] = @SecretKey and [UserIdentity] = SUSER_NAME())
Begin
	update [dbo].[CXCredential]
	set [Salt] = @Salt
	,[SecretValue] = @SecretValue
End
else
Begin
INSERT INTO [dbo].[CXCredential]
           ([SecretKey]
           ,[UserIdentity]
           ,[Salt]
           ,[SecretValue])
     VALUES
           (@SecretKey
           ,SUSER_NAME()
           ,@Salt
           ,@SecretValue)
End";
        //using (SqlConnection sqlConContext = new SqlConnection("CXSqlClrExtensions".LocalDBNameToConnectionString()))
        using (SqlConnection sqlConContext = new SqlConnection("context connection=true"))
        {
            sqlConContext.Open();
            EncryptedContent = CXSqlClrExtensions.Encryption.EncryptionUtil.Encrypt(secretKey, secretValue, out Salt);
            using (SqlCommand cmd = new SqlCommand(SQL, sqlConContext))
            {
                cmd.Parameters.Add(new SqlParameter(@"@SecretKey", secretKey));
                cmd.Parameters.Add(new SqlParameter(@"@Salt", Salt));
                cmd.Parameters.Add(new SqlParameter(@"@SecretValue", EncryptedContent));
                cmd.ExecuteNonQuery();
            }
            try
            {
                CurrentUser = @"Unknown";
                using (SqlCommand cmd = new SqlCommand(@"select SUSER_NAME() as CurrentUser", sqlConContext))
                {
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            CurrentUser = reader[@"CurrentUser"].ToString();
                        }
                    }
                }
                string.Concat(@"V1.1 Secret: ", secretKey, @" Saved under User Identity: ", CurrentUser).SQLPipePrintImmediate(sqlConContext);
            }
            catch (Exception ex)
            {
                ex.ToString().SQLPipePrintImmediate(sqlConContext);
            }
        }
    }

    public static void CredentialRetrieveInternal(SqlConnection sqlConContext, string secretKey, out string secretValue)
    {
        byte[] EncryptedContent;
        byte[] Salt;
        string DecryptedContent;
        bool Success;
        DecryptedContent = string.Empty;
        secretValue = string.Empty;
        //using (SqlConnection sqlConContext = new SqlConnection("CXSqlClrExtensions".LocalDBNameToConnectionString()))
        Success = false;
        using (SqlCommand cmd = new SqlCommand(@"select [Salt],[SecretValue] from [dbo].[CXCredential] where [SecretKey] = @SecretKey and [UserIdentity] = SUSER_NAME()", sqlConContext))
        {
            cmd.Parameters.Add(new SqlParameter(@"@SecretKey", secretKey));
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                if (reader.Read())
                {
                    Salt = (byte[])reader[@"Salt"];
                    EncryptedContent = (byte[])reader[@"SecretValue"];
                    DecryptedContent = CXSqlClrExtensions.Encryption.EncryptionUtil.Decrypt(secretKey, EncryptedContent, Salt);
                    Success = true;
                }
                else
                {
                    string.Concat(@"Nothing found under current user for Secret Key: ", secretKey).SQLPipePrintImmediate(sqlConContext);
                }
            }
        }
        if (Success)
        {
            string.Concat(@"V1.1 Found credentials: ", secretKey).SQLPipePrintImmediate(sqlConContext);
            secretValue = DecryptedContent;
        }
        else
        {
            string.Concat(@"Nothing found under current user for Secret Key: ", secretKey).SQLPipePrintImmediate(sqlConContext);
        }
    }

    [Microsoft.SqlServer.Server.SqlProcedure()]
    public static void GCPBQExecSQL(int DiagnosticLevel, string CredentialKey, string BQProjectID, string SQL)
    {
        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
        CXSqlClrExtensions.GCPBigQuery.BQReplicate.GCPBQExecSQL(DiagnosticLevel, CredentialKey, BQProjectID, SQL);
    }

    [Microsoft.SqlServer.Server.SqlProcedure()]
    public static void GCPBQSyncSchema(int DiagnosticLevel, string CredentialKey, string BQProjectID, string SourceDB, string SourceSchema, string SourceTable, string destinationDataSet, string destinationTable, string PartitionBy, string ClusterBy)
    {
        //ServicePointManager.SecurityProtocol = (SecurityProtocolType)3072;
        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
        CXSqlClrExtensions.GCPBigQuery.BQReplicate.GCPBQSyncSchema(DiagnosticLevel, CredentialKey, BQProjectID, SourceDB, SourceSchema, SourceTable, destinationDataSet, destinationTable, PartitionBy, ClusterBy);
    }

    [Microsoft.SqlServer.Server.SqlProcedure()]
    public static void GCPBQDropSchema(int DiagnosticLevel, string CredentialKey, string BQProjectID, string destinationDataSet, string destinationTable)
    {
        //ServicePointManager.SecurityProtocol = (SecurityProtocolType)3072;
        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
        CXSqlClrExtensions.GCPBigQuery.BQReplicate.GCPBQDropSchema(DiagnosticLevel, CredentialKey, BQProjectID, destinationDataSet, destinationTable);
    }

    [Microsoft.SqlServer.Server.SqlProcedure()]
    public static void GCPBQSyncDataSet(int DiagnosticLevel, string CredentialKey, string BQProjectID, string SourceDB, string SourceSchema, string SourceTable, string destinationDataSet, string destinationTable, string CreatedByVersionFieldName, string ModifiedByVersionFieldName, string UniqueKeyFieldName, string whereClause, int NumGCPThreads = 8)
    {
        //ServicePointManager.SecurityProtocol = (SecurityProtocolType)3072;
        ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
        CXSqlClrExtensions.GCPBigQuery.BQReplicate.GCPBQSyncDataSet(DiagnosticLevel, CredentialKey, BQProjectID, SourceDB, SourceSchema, SourceTable, destinationDataSet, destinationTable, CreatedByVersionFieldName, ModifiedByVersionFieldName, UniqueKeyFieldName, whereClause, NumGCPThreads);
    }
}
