using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CXSqlClrExtensions.GCPBigQuery
{
    public class DBColumnType
    {
        public enum Enum_DataType
        {
            NotImplemented = 0,
            type_int = 1,
            type_long = 2,
            type_string = 3,
            type_boolean = 4,
            type_decimal = 5,
            type_datetime = 6,
            type_date = 7,
            type_binary = 8,
        }

        public DBColumnType(string _ColumnName, string _DataType, int _DataLength, int _DataPrecision, int _DataScale, string _ColumnDescription)
        {
            ColumnName = _ColumnName.ToLowerInvariant().Trim();
            _DataType = _DataType.ToLower().Trim();
            switch (_DataType)
            {
                case @"int":
                    DataType = Enum_DataType.type_int;
                    break;
                case @"bigint":
                    DataType = Enum_DataType.type_long;
                    break;
                case @"varchar":
                    DataType = Enum_DataType.type_string;
                    DataLength = _DataLength;
                    break;
                case @"nvarchar":
                    DataType = Enum_DataType.type_string;
                    DataLength = _DataLength;
                    break;
                case @"bit":
                    DataType = Enum_DataType.type_boolean;
                    break;
                case @"decimal":
                    DataType = Enum_DataType.type_decimal;
                    DataLength = _DataPrecision;
                    if (DataLength > 32)
                    {
                        DataLength = 32;
                    }
                    DataDecimalPlaces = _DataScale;
                    if (DataDecimalPlaces > 30)
                    {
                        DataDecimalPlaces = 30;
                    }
                    break;
                case @"datetime":
                    DataType = Enum_DataType.type_datetime;
                    break;
                case @"datetime2":
                    DataType = Enum_DataType.type_datetime;
                    break;
                case @"date":
                    DataType = Enum_DataType.type_date;
                    break;
                case @"varbinary":
                    DataType = Enum_DataType.type_binary;
                    DataLength = _DataLength;
                    break;
                default:
                    DataType = Enum_DataType.NotImplemented;
                    break;
            }
            ColumnDescription = _ColumnDescription;
        }

        public string ColumnName { get; private set; }
        public string ColumnParamName
        {
            get
            {
                return ColumnName.Replace(@"[", string.Empty).Replace(@"]", string.Empty);
            }
        }
        public Enum_DataType DataType { get; private set; }
        public int DataLength { get; private set; } = 0;
        public int DataDecimalPlaces { get; private set; } = 0;
        public string ColumnDescription { get; private set; }
        public int DataColumnToImport { get; set; }
    }

}
