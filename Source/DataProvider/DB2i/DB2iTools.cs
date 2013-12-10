using System;
using System.Data;
using System.Reflection;

namespace LinqToDB.DataProvider.DB2i
{
	using System.Configuration;
	using System.Linq;
	using System.Linq.Expressions;

	using Data;

	public static class DB2iTools
	{
        static readonly DB2iDataProvider _db2iDataProvider = new DB2iDataProvider();

        static DB2iTools()
		{
			DataConnection.AddDataProvider(_db2iDataProvider);
		}

		public static IDataProvider GetDataProvider()
		{
			return _db2iDataProvider;
		}

		public static void ResolveDB2i(string path)
		{
            new AssemblyResolver(path, "IBM.Data.DB2.iSeries");
		}

        public static void ResolveDB2i(Assembly assembly)
		{
            new AssemblyResolver(assembly, "IBM.Data.DB2.iSeries");
		}

		#region CreateDataConnection

		public static DataConnection CreateDataConnection(string connectionString)
		{
			return new DataConnection(_db2iDataProvider, connectionString);
		}

		public static DataConnection CreateDataConnection(IDbConnection connection)
		{
			return new DataConnection(_db2iDataProvider, connection);
		}

		public static DataConnection CreateDataConnection(IDbTransaction transaction)
		{
			return new DataConnection(_db2iDataProvider, transaction);
		}

		#endregion
	}
}
