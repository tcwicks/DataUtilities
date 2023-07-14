using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CXSqlClrExtensions.GCPBigQuery
{
    internal class DBSchema
    {
        public enum Schema_SysTableSchema
        {
            Schema = 0,
            Table = 1,
            TableDesc = 2,
            Column = 3,
            DataType = 4,
            DataLength = 5,
            DataPrecision = 6,
            DataScale = 7,
            CalculatedColumn = 8,
            ColumnDesc = 9,
        }
        public const string Execute_GetTableSchema = @"EXECUTE [dbo].[syssp_ViewSchema] @DatabaseName, @SchemaName, @TableName";

        public DBSchema(string _targetDatabase, string _targetSchema, string _targetTable)
        {
            m_TargetDatabase = _targetDatabase;
            m_TargetSchema = _targetSchema;
            m_TargetTable = _targetTable;
        }

        private string m_TargetDatabase = string.Empty;
        private string TargetDatabase
        {
            get
            {
                return m_TargetDatabase;
            }
        }

        private string m_TargetSchema = string.Empty;
        private string TargetSchema
        {
            get
            {
                return m_TargetSchema;
            }
        }
        private string m_TargetTable = string.Empty;
        private string TargetTable
        {
            get
            {
                return m_TargetTable;
            }
        }

        public Dictionary<string, DBColumnType> Schema { get; } = new Dictionary<string, DBColumnType>();
        public void LoadSchema()
        {
            using (SqlConnection DBConn = new SqlConnection(CXSQLExt.ControlDB.LocalDBNameToConnectionString()))
            {
                DBConn.Open();
                DBColumnType ColumnTypeDefinition;
                Schema.Clear();
                using (SqlCommand DBCommandGetSchema = new SqlCommand(Execute_GetTableSchema, DBConn))
                {
                    DBCommandGetSchema.Parameters.Add(new SqlParameter(@"DatabaseName", TargetDatabase));
                    DBCommandGetSchema.Parameters.Add(new SqlParameter(@"SchemaName", TargetSchema));
                    DBCommandGetSchema.Parameters.Add(new SqlParameter(@"TableName", TargetTable));
                    using (SqlDataReader sqlDataReader = DBCommandGetSchema.ExecuteReader())
                    {
                        while (sqlDataReader.Read())
                        {
                            string ColumnName;
                            string DataType;
                            int DataLength;
                            int DataPrecision;
                            int DataScale;
                            bool IsCalculated;
                            string ColumnDescription;
                            ColumnName = (sqlDataReader[(int)Schema_SysTableSchema.Column].ToString());
                            DataType = (sqlDataReader[(int)Schema_SysTableSchema.DataType].ToString());
                            DataLength = (Convert.ToInt32(sqlDataReader[(int)Schema_SysTableSchema.DataLength]));
                            DataPrecision = (Convert.ToInt32(sqlDataReader[(int)Schema_SysTableSchema.DataPrecision]));
                            DataScale = (Convert.ToInt32(sqlDataReader[(int)Schema_SysTableSchema.DataScale]));
                            IsCalculated = (Convert.ToInt32(sqlDataReader[(int)Schema_SysTableSchema.CalculatedColumn]) != 0);
                            ColumnDescription = (sqlDataReader[(int)Schema_SysTableSchema.ColumnDesc].ToString());

                            ColumnTypeDefinition = new DBColumnType(ColumnName, DataType, DataLength, DataPrecision, DataScale, ColumnDescription);
                            if (ColumnTypeDefinition.ColumnName.ToLowerInvariant() != @"pkid")
                            {
                                Schema.Add(ColumnTypeDefinition.ColumnName, ColumnTypeDefinition);
                                //if (!IsCalculated)
                                //{
                                //    //if (ColumnTypeDefinition.DataType == DBColumnType.Enum_DataType.type_string)
                                //    //{
                                //    //    Schema.Add(ColumnTypeDefinition.ColumnName, ColumnTypeDefinition);
                                //    //}
                                //    //else if (ColumnTypeDefinition.ColumnName.ToLowerInvariant().Trim().StartsWith(@"etl_"))
                                //    //{
                                //    //    Schema.Add(ColumnTypeDefinition.ColumnName, ColumnTypeDefinition);
                                //    //}
                                //}
                            }
                        }
                    }
                }
            }
        }

        public string BQCreateTableSQL(string ProjectID, string DataSetName, string TableName, string PartitionBy, string CluseterBy)
        {
            StringBuilder sbSQL;
            sbSQL = new StringBuilder();
            sbSQL.Append(@"drop table if exists `").Append(ProjectID).Append(@".").Append(DataSetName).Append(@".").Append(TableName).AppendLine(@"`;");
            sbSQL.AppendLine(@"");
            sbSQL.Append(@"CREATE TABLE `").Append(ProjectID).Append(@".").Append(DataSetName).Append(@".").Append(TableName).AppendLine(@"`");
            sbSQL.AppendLine(@"(");
            bool IsFirst;
            IsFirst = true;
            foreach (DBColumnType ColumnTypeDefinition in Schema.Values)
            {
                if (ColumnTypeDefinition.DataType != DBColumnType.Enum_DataType.NotImplemented)
                {
                    if (IsFirst)
                    {
                        IsFirst = false;
                    }
                    else
                    {
                        sbSQL.AppendLine(@",");
                    }
                    sbSQL.Append(@"    ");
                    switch (ColumnTypeDefinition.DataType)
                    {
                        case DBColumnType.Enum_DataType.type_string:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" STRING(").Append(ColumnTypeDefinition.DataLength.ToString()).Append(@")");
                            break;
                        case DBColumnType.Enum_DataType.type_int:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" INT64");
                            break;
                        case DBColumnType.Enum_DataType.type_long:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" INT64");
                            break;
                        case DBColumnType.Enum_DataType.type_boolean:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" BOOL");
                            break;
                        case DBColumnType.Enum_DataType.type_decimal:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" NUMERIC(").Append(ColumnTypeDefinition.DataLength.ToString()).Append(@", ").Append(ColumnTypeDefinition.DataDecimalPlaces.ToString())
                                .Append(@") OPTIONS(rounding_mode='ROUND_HALF_AWAY_FROM_ZERO')");
                            break;
                        case DBColumnType.Enum_DataType.type_datetime:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" DATETIME");
                            break;
                        case DBColumnType.Enum_DataType.type_date:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" DATE");
                            break;
                        case DBColumnType.Enum_DataType.type_binary:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" BYTES(").Append(ColumnTypeDefinition.DataLength.ToString()).Append(@")");
                            break;
                    }
                }
            }
            sbSQL.AppendLine(@"").Append(@")");
            if (string.IsNullOrEmpty(PartitionBy)) { PartitionBy = string.Empty; }
            PartitionBy = PartitionBy.Trim();
            if (!string.IsNullOrEmpty(PartitionBy))
            {
                sbSQL.AppendLine(@"PARTITION BY");
                sbSQL.AppendLine(PartitionBy);
            }
            if (string.IsNullOrEmpty(CluseterBy)) { CluseterBy = string.Empty; }
            CluseterBy = CluseterBy.Trim();
            if (!String.IsNullOrEmpty(CluseterBy))
            {
                sbSQL.AppendLine(@"CLUSTER BY");
                sbSQL.AppendLine(CluseterBy);
            }
            sbSQL.AppendLine(@";");
            return sbSQL.ToString();
        }
        public string BQCreateInsertTableSQL(string ProjectID, string DataSetName, string TableName, out string FinalTableName)
        {
            StringBuilder sbSQL;
            sbSQL = new StringBuilder();
            FinalTableName = string.Concat(@"zzz_", TableName, @"_Insert_", DateTime.Now.ToTimeStampText());
            sbSQL.Append(@"drop table if exists `").Append(ProjectID).Append(@".").Append(DataSetName).Append(@".").Append(FinalTableName).AppendLine(@"`;");
            sbSQL.AppendLine(@"");
            sbSQL.Append(@"CREATE TABLE `").Append(ProjectID).Append(@".").Append(DataSetName).Append(@".").Append(FinalTableName).AppendLine(@"`");
            sbSQL.AppendLine(@"(");
            bool IsFirst;
            IsFirst = true;
            foreach (DBColumnType ColumnTypeDefinition in Schema.Values)
            {
                if (ColumnTypeDefinition.DataType != DBColumnType.Enum_DataType.NotImplemented)
                {
                    if (IsFirst)
                    {
                        IsFirst = false;
                    }
                    else
                    {
                        sbSQL.AppendLine(@",");
                    }
                    sbSQL.Append(@"    ");
                    switch (ColumnTypeDefinition.DataType)
                    {
                        case DBColumnType.Enum_DataType.type_string:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" STRING(").Append(ColumnTypeDefinition.DataLength.ToString()).Append(@")");
                            break;
                        case DBColumnType.Enum_DataType.type_int:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" INT64");
                            break;
                        case DBColumnType.Enum_DataType.type_long:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" INT64");
                            break;
                        case DBColumnType.Enum_DataType.type_boolean:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" BOOL");
                            break;
                        case DBColumnType.Enum_DataType.type_decimal:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" NUMERIC(").Append(ColumnTypeDefinition.DataLength.ToString()).Append(@", ").Append(ColumnTypeDefinition.DataDecimalPlaces.ToString())
                                .Append(@") OPTIONS(rounding_mode='ROUND_HALF_AWAY_FROM_ZERO')");
                            break;
                        case DBColumnType.Enum_DataType.type_datetime:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" DATETIME");
                            break;
                        case DBColumnType.Enum_DataType.type_date:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" DATE");
                            break;
                        case DBColumnType.Enum_DataType.type_binary:
                            sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" BYTES(").Append(ColumnTypeDefinition.DataLength.ToString()).Append(@")");
                            break;
                    }
                }
            }
            sbSQL.AppendLine(@"").AppendLine(@")");
            sbSQL.AppendLine(@"OPTIONS(");
            sbSQL.AppendLine(@"expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)");
            sbSQL.AppendLine(");");
            return sbSQL.ToString();
        }

        public string BQCreateKeyTableSQL(string ProjectID, string DataSetName, string TableName, string UniqueKeyFieldName, out string FinalTableName)
        {
            StringBuilder sbSQL;
            sbSQL = new StringBuilder();
            FinalTableName = string.Concat(@"zzz_", TableName, @"_", UniqueKeyFieldName, @"_", DateTime.Now.ToTimeStampText());
            if (Schema.ContainsKey(UniqueKeyFieldName.ToLower()))
            {
                sbSQL.AppendLine(@"");
                sbSQL.Append(@"drop table if exists `").Append(ProjectID).Append(@".").Append(DataSetName).Append(@".").Append(FinalTableName).AppendLine(@"`;");
                sbSQL.AppendLine(@"");
                sbSQL.Append(@"CREATE TABLE `").Append(ProjectID).Append(@".").Append(DataSetName).Append(@".").Append(FinalTableName).AppendLine(@"`");
                sbSQL.AppendLine(@"(");
                DBColumnType ColumnTypeDefinition;
                ColumnTypeDefinition = Schema[UniqueKeyFieldName.ToLower()];
                switch (ColumnTypeDefinition.DataType)
                {
                    case DBColumnType.Enum_DataType.type_string:
                        sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" STRING(").Append(ColumnTypeDefinition.DataLength.ToString()).Append(@")");
                        break;
                    case DBColumnType.Enum_DataType.type_int:
                        sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" INT64");
                        break;
                    case DBColumnType.Enum_DataType.type_long:
                        sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" INT64");
                        break;
                    case DBColumnType.Enum_DataType.type_boolean:
                        sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" BOOL");
                        break;
                    case DBColumnType.Enum_DataType.type_decimal:
                        sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" NUMERIC(").Append(ColumnTypeDefinition.DataLength.ToString()).Append(@", ").Append(ColumnTypeDefinition.DataDecimalPlaces.ToString())
                            .Append(@") OPTIONS(rounding_mode='ROUND_HALF_AWAY_FROM_ZERO')");
                        break;
                    case DBColumnType.Enum_DataType.type_datetime:
                        sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" DATETIME");
                        break;
                    case DBColumnType.Enum_DataType.type_date:
                        sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" DATE");
                        break;
                    case DBColumnType.Enum_DataType.type_binary:
                        sbSQL.Append(ColumnTypeDefinition.ColumnDescription).Append(@" BYTES(").Append(ColumnTypeDefinition.DataLength.ToString()).Append(@")");
                        break;
                }
                sbSQL.AppendLine(@"").AppendLine(@")");
                sbSQL.AppendLine(@"OPTIONS(");
                sbSQL.AppendLine(@"expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR)");
                sbSQL.AppendLine(");");
            }
            return sbSQL.ToString();
        }

        public string BQDropTableSQL(string ProjectID, string DataSetName, string TableName)
        {
            StringBuilder sbSQL;
            sbSQL = new StringBuilder();
            sbSQL.Append(@"drop table if exists `").Append(ProjectID).Append(@".").Append(DataSetName).Append(@".").Append(TableName).AppendLine(@"`;");
            return sbSQL.ToString();
        }

        public string BQCopyInsertToMain(string ProjectID, string DataSetName, string TableName, string InsertTableName)
        {
            StringBuilder sbSQL;
            sbSQL = new StringBuilder();
            sbSQL.Append(@"INSERT INTO `").Append(ProjectID).Append(@".").Append(DataSetName).Append(@".").Append(TableName).AppendLine(@"`");
            sbSQL.AppendLine(@"(");
            bool IsFirst;
            IsFirst = true;
            foreach (DBColumnType ColumnTypeDefinition in Schema.Values)
            {
                if (ColumnTypeDefinition.DataType != DBColumnType.Enum_DataType.NotImplemented)
                {
                    if (IsFirst)
                    {
                        IsFirst = false;
                    }
                    else
                    {
                        sbSQL.AppendLine(@",");
                    }
                    sbSQL.Append(ColumnTypeDefinition.ColumnDescription);
                }
            }
            sbSQL.AppendLine(@"");
            sbSQL.AppendLine(@")");

            sbSQL.AppendLine(@"SELECT");
            IsFirst = true;
            foreach (DBColumnType ColumnTypeDefinition in Schema.Values)
            {
                if (ColumnTypeDefinition.DataType != DBColumnType.Enum_DataType.NotImplemented)
                {
                    if (IsFirst)
                    {
                        IsFirst = false;
                    }
                    else
                    {
                        sbSQL.AppendLine(@",");
                    }
                    sbSQL.Append(ColumnTypeDefinition.ColumnDescription);
                }
            }
            sbSQL.AppendLine(@" ");
            sbSQL.Append(@" FROM `").Append(ProjectID).Append(@".").Append(DataSetName).Append(@".").Append(InsertTableName).AppendLine(@"`");
            return sbSQL.ToString();
        }
    }

}
