using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace LinqToDB.DataProvider.DB2i
{
	using Data;
	using Mapping;
	using SchemaProvider;
	using SqlProvider;

	public class DB2iDataProvider : DynamicDataProviderBase
	{
		public DB2iDataProvider()
			: base(ProviderName.DB2i, new DB2iMappingSchema())
		{
			SqlProviderFlags.AcceptsTakeAsParameter       = false;
			SqlProviderFlags.AcceptsTakeAsParameterIfSkip = true;
            SqlProviderFlags.IsParameterOrderDependent = true;

			SetCharField("CHAR", (r,i) => r.GetString(i).TrimEnd());

			_sqlOptimizer = new DB2iSqlOptimizer(SqlProviderFlags);
		}

        Type _iDB2BigInt; Type _iDB2Integer; Type _iDB2SmallInt; Type _iDB2Decimal; Type _iDB2DecFloat16; Type _iDB2DecFloat34;
        Type _iDB2Real; Type _iDB2Double; Type _iDB2Char; Type _iDB2VarChar; Type _iDB2Clob;
        Type _iDB2Binary; Type _iDB2Blob; Type _iDB2Date; Type _iDB2Time; Type _iDB2TimeStamp;
        Type _iDB2Xml; Type _iDB2Rowid;

		protected override void OnConnectionTypeCreated(Type connectionType)
		{
            _iDB2BigInt = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2BigInt", true);
            _iDB2Integer = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2Integer", true);
            _iDB2SmallInt = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2SmallInt", true);
            _iDB2Decimal = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2Decimal", true);
            _iDB2DecFloat16 = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2DecFloat16", true);
            _iDB2DecFloat34 = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2DecFloat34", true);
            _iDB2Real = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2Real", true);
            _iDB2Double = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2Double", true);
            _iDB2Char = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2Char", true);
            _iDB2VarChar = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2VarChar", true);
            _iDB2Clob = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2Clob", true);
            _iDB2Binary = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2Binary", true);
            _iDB2Blob = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2Blob", true);
            _iDB2Date = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2Date", true);
            _iDB2Time = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2Time", true);
            _iDB2TimeStamp = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2TimeStamp", true);
            _iDB2Xml = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2Xml", true);
            _iDB2Rowid = connectionType.Assembly.GetType("IBM.Data.DB2.iSeries.iDB2Rowid", true);

            SetProviderField(_iDB2BigInt, typeof(Int64), "GetiDB2BigInt");
            SetProviderField(_iDB2Integer, typeof(Int32), "GetiDB2Integer");
            SetProviderField(_iDB2SmallInt, typeof(Int16), "GetiDB2SmallInt");
            SetProviderField(_iDB2Decimal, typeof(Decimal), "GetiDB2Decimal");
            SetProviderField(_iDB2DecFloat16, typeof(Decimal), "GetiDB2DecFloat16");
            SetProviderField(_iDB2DecFloat34, typeof(Decimal), "GetiDB2DecFloat34");
            SetProviderField(_iDB2Real, typeof(Single), "GetiDB2Real");
            SetProviderField(_iDB2Double, typeof(Double), "GetiDB2Double");
            SetProviderField(_iDB2Char, typeof(String), "GetiDB2Char");
            SetProviderField(_iDB2VarChar, typeof(String), "GetiDB2VarChar");
            SetProviderField(_iDB2Clob, typeof(String), "GetiDB2Clob");
            SetProviderField(_iDB2Binary, typeof(byte[]), "GetiDB2Binary");
            SetProviderField(_iDB2Blob, typeof(byte[]), "GetiDB2Blob");
            SetProviderField(_iDB2Date, typeof(DateTime), "GetiDB2Date");
            SetProviderField(_iDB2Time, typeof(TimeSpan), "GetiDB2Time");
            SetProviderField(_iDB2TimeStamp, typeof(DateTime), "GetiDB2TimeStamp");
            SetProviderField(_iDB2Xml, typeof(string), "GetiDB2Xml");
            SetProviderField(_iDB2Rowid, typeof(byte[]), "GetiDB2Rowid");

			MappingSchema.AddScalarType(_iDB2BigInt,        GetNullValue(_iDB2BigInt),        true, DataType.Int64);
			MappingSchema.AddScalarType(_iDB2Integer,        GetNullValue(_iDB2Integer),        true, DataType.Int32);
			MappingSchema.AddScalarType(_iDB2SmallInt,        GetNullValue(_iDB2SmallInt),        true, DataType.Int16);
			MappingSchema.AddScalarType(_iDB2Decimal,      GetNullValue(_iDB2Decimal),      true, DataType.Decimal);
            MappingSchema.AddScalarType(_iDB2DecFloat16, GetNullValue(_iDB2DecFloat16), true, DataType.Decimal);
            MappingSchema.AddScalarType(_iDB2DecFloat34, GetNullValue(_iDB2DecFloat34), true, DataType.Decimal);
            MappingSchema.AddScalarType(_iDB2Real, GetNullValue(_iDB2Real), true, DataType.Single);
            MappingSchema.AddScalarType(_iDB2Double, GetNullValue(_iDB2Double), true, DataType.Double);
			MappingSchema.AddScalarType(_iDB2Char,       GetNullValue(_iDB2Char),       true, DataType.Char);
            MappingSchema.AddScalarType(_iDB2VarChar, GetNullValue(_iDB2VarChar), true, DataType.NVarChar);
			MappingSchema.AddScalarType(_iDB2Clob,         GetNullValue(_iDB2Clob),         true, DataType.NText);
            MappingSchema.AddScalarType(_iDB2Binary, GetNullValue(_iDB2Binary), true, DataType.VarBinary);
			MappingSchema.AddScalarType(_iDB2Blob,         GetNullValue(_iDB2Blob),         true, DataType.Blob);
            MappingSchema.AddScalarType(_iDB2Date, GetNullValue(_iDB2Date), true, DataType.Date);
			MappingSchema.AddScalarType(_iDB2Time,         GetNullValue(_iDB2Time),         true, DataType.Time);
			MappingSchema.AddScalarType(_iDB2TimeStamp,    GetNullValue(_iDB2TimeStamp),    true, DataType.Timestamp);
            MappingSchema.AddScalarType(_iDB2Xml, GetNullValue(_iDB2Xml), true, DataType.Xml);
            MappingSchema.AddScalarType(_iDB2Rowid, GetNullValue(_iDB2Rowid), true, DataType.VarBinary);

            _setBlob = GetSetParameter(connectionType, "iDB2Parameter", "iDB2DbType", "iDB2DbType", "iDB2Blob");
		}

		static object GetNullValue(Type type)
		{
			var getValue = Expression.Lambda<Func<object>>(Expression.Convert(Expression.Field(null, type, "Null"), typeof(object)));
			return getValue.Compile()();
		}

        public override string ConnectionNamespace { get { return "IBM.Data.DB2.iSeries"; } }
        protected override string ConnectionTypeName { get { return "IBM.Data.DB2.iSeries.iDB2Connection, IBM.Data.DB2.iSeries"; } }
        protected override string DataReaderTypeName { get { return "IBM.Data.DB2.iSeries.iDB2DataReader, IBM.Data.DB2.iSeries"; } }

		public override ISchemaProvider GetSchemaProvider()
		{
			return new DB2iSchemaProvider();
		}

		public override ISqlBuilder CreateSqlBuilder()
		{
			return new DB2iSqlBuilder(GetSqlOptimizer(), SqlProviderFlags);
		}

		readonly DB2iSqlOptimizer _sqlOptimizer;

		public override ISqlOptimizer GetSqlOptimizer()
		{
			return _sqlOptimizer;
		}

		public override void InitCommand(DataConnection dataConnection)
		{
			dataConnection.DisposeCommand();
			base.InitCommand(dataConnection);
		}

		static Action<IDbDataParameter> _setBlob;

		public override void SetParameter(IDbDataParameter parameter, string name, DataType dataType, object value)
		{
			if (value is sbyte)
			{
				value    = (short)(sbyte)value;
				dataType = DataType.Int16;
			}
			else if (value is byte)
			{
				value    = (short)(byte)value;
				dataType = DataType.Int16;
			}

			switch (dataType)
			{
				case DataType.UInt16     : dataType = DataType.Int32;    break;
				case DataType.UInt32     : dataType = DataType.Int64;    break;
				case DataType.UInt64     : dataType = DataType.Decimal;  break;
				case DataType.VarNumeric : dataType = DataType.Decimal;  break;
				case DataType.DateTime2  : dataType = DataType.DateTime; break;
				case DataType.Char       :
				case DataType.VarChar    :
				case DataType.NChar      :
				case DataType.NVarChar   :
					     if (value is Guid) value = ((Guid)value).ToString();
					else if (value is bool)
						value = Common.ConvertTo<char>.From((bool)value);
					break;
				case DataType.Boolean    :
				case DataType.Int16      :
					if (value is bool)
					{
						value    = (bool)value ? 1 : 0;
						dataType = DataType.Int16;
					}
					break;
				case DataType.Guid       :
					if (value is Guid)
					{
						value    = ((Guid)value).ToByteArray();
						dataType = DataType.VarBinary;
					}
					break;
				case DataType.Binary     :
				case DataType.VarBinary  :
					if (value is Guid) value = ((Guid)value).ToByteArray();
					break;
				case DataType.Blob       :
					base.SetParameter(parameter, "@" + name, dataType, value);
					_setBlob(parameter);
					return;
			}

			base.SetParameter(parameter, "@" + name, dataType, value);
		}

		static Func<IDbConnection,IDisposable> _bulkCopyCreator;
		static Func<int,string,object>         _columnMappingCreator;
	}
}
