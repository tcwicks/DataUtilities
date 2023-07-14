using Microsoft.SqlServer.Server;
using System;
using System.Data.SqlClient;
using System.Reflection;
using static CXSqlClrExtensions.GCPBigQuery.DBColumnType;

namespace CXSqlClrExtensions
{
    internal static class ExtensionMethods
    {
        const String _rowsCopiedFieldName = "_rowsCopied";
        static FieldInfo _rowsCopiedField = null;

        public static long RowsCopiedCount(this SqlBulkCopy bulkCopy)
        {
            if (_rowsCopiedField == null) _rowsCopiedField = typeof(SqlBulkCopy).GetField(_rowsCopiedFieldName, BindingFlags.NonPublic | BindingFlags.GetField | BindingFlags.Instance);
            return (int)_rowsCopiedField.GetValue(bulkCopy);
        }

        public static string LocalDBNameToConnectionString(this string Database, int TimeoutSeconds = 300)
        {
            return string.Concat(@"Data Source=(local);Initial Catalog=", Database, @";Integrated Security=True;MultipleActiveResultSets=true;Connection Timeout=", TimeoutSeconds.ToString());
        }

        public static void SQLPipePrintImmediate(this string Message, SqlConnection sqlCon)
        {
            try
            {
                Message = Message.Replace(@"%", @"%%");
                if (Message.Length > 2000)
                {
                    if (Message.Contains("\r") || Message.Contains("\n"))
                    {
                        foreach (string MessageLine in Message.Replace("\r\n", "\n").Replace("\r", "\n").Split("\n".ToCharArray()))
                        {
                            using (SqlCommand sqlCommand = new SqlCommand(string.Concat("raiserror('", MessageLine.Replace(@"'", @"`"), @"', 0, 0) with nowait"), sqlCon))
                            {
                                SqlContext.Pipe.ExecuteAndSend(sqlCommand);
                            }
                        }
                    }
                    else
                    {
                        SqlContext.Pipe.Send(Message);
                        using (SqlCommand sqlCommand = new SqlCommand("raiserror('', 0, 0) with nowait", sqlCon))
                        {
                            SqlContext.Pipe.ExecuteAndSend(sqlCommand);
                        }
                    }
                }
                else
                {
                    //using (SqlCommand sqlCommand = new SqlCommand(string.Format("raiserror('{0}', 0, 0) with nowait", Message.Replace(@"'", @"`")), sqlCon))
                    using (SqlCommand sqlCommand = new SqlCommand(string.Concat("raiserror('", Message.Replace(@"'", @"`"), @"', 0, 0) with nowait"), sqlCon))
                    {
                        SqlContext.Pipe.ExecuteAndSend(sqlCommand);
                    }
                }
            }
            catch
            {

            }
        }

        public static int ToInt(this Object Target, int Default)
        {
            int Result;
            if (Target == null)
            {
                return Default;
            }
            if (Target is int)
            {
                return (int)Target;
            }
            if (int.TryParse(Target.ToString(), out Result))
            {
                return Result;
            }
            return Default;
        }

        public static object SQLToBQValue(this object Target, Enum_DataType DataType, object Default)
        {
            Google.Cloud.BigQuery.V2.BigQueryNumeric ResultNumeric;
            if (Target == null)
            {
                return Default;
            }
            else if (Target is decimal)
            {
                ResultNumeric = (Google.Cloud.BigQuery.V2.BigQueryNumeric)(decimal)Target;
                return ResultNumeric;
            }
            else if (Target is DateTime)
            {
                if ((DataType == Enum_DataType.type_date) || (DataType == Enum_DataType.type_datetime))
                {
                    return Google.Cloud.BigQuery.V2.BigQueryDateTimeExtensions.AsBigQueryDate((DateTime)Target);
                }
            }
            else if (Target is DBNull)
            {
                return Default;
            }
            return Target;
        }

        public static string ToTimeStampText(this DateTime Target)
        {
            return Target.ToString(@"yyyyMMddHHmmss");
        }

        public static bool IsInt32CompatibleType(this System.Type t)
        {
            switch (Type.GetTypeCode(t))
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.Int16:
                case TypeCode.Int32:
                    return true;
                default:
                    return false;
            }
        }

        public static bool IsInt64CompatibleType(this System.Type t)
        {
            switch (Type.GetTypeCode(t))
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                    return true;
                default:
                    return false;
            }
        }
    }
}
