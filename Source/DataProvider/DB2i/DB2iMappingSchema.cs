using System;

namespace LinqToDB.DataProvider.DB2i
{
	using Mapping;

	public class DB2iMappingSchema : MappingSchema
	{
		public DB2iMappingSchema() : this(ProviderName.DB2i)
		{
		}

		protected DB2iMappingSchema(string configuration) : base(configuration)
		{
		}

		internal static readonly DB2iMappingSchema Instance = new DB2iMappingSchema();
	}
}
