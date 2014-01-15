using System;
using System.Linq;
using System.Text;

namespace LinqToDB.DataProvider.DB2i
{
    using SqlQuery;
    using SqlProvider;
    using System.Collections.Generic;

    class DB2iSqlBuilder : BasicSqlBuilder
    {
        public DB2iSqlBuilder(ISqlOptimizer sqlOptimizer, SqlProviderFlags sqlProviderFlags)
            : base(sqlOptimizer, sqlProviderFlags)
        {
        }

        protected override ISqlBuilder CreateSqlBuilder()
        {
            return new DB2iSqlBuilder(SqlOptimizer, SqlProviderFlags);
        }

        SqlField _identityField;

        public override int CommandCount(SelectQuery selectQuery)
        {
            if (selectQuery.IsInsert && selectQuery.Insert.WithIdentity)
            {
                _identityField = selectQuery.Insert.Into.GetIdentityField();

                if (_identityField == null)
                    return 2;
            }

            return 1;
        }

        protected override void BuildSql(int commandNumber, SelectQuery selectQuery, StringBuilder sb, int indent, bool skipAlias)
        {
            SelectQuery = selectQuery;
            StringBuilder = sb;
            Indent = indent;
            SkipAlias = skipAlias;

            if (_identityField != null)
            {
                indent += 2;

                AppendIndent().AppendLine("SELECT");
                AppendIndent().Append("\t");
                BuildExpression(_identityField, false, true);
                sb.AppendLine();
                AppendIndent().AppendLine("FROM");
                AppendIndent().AppendLine("\tNEW TABLE");
                AppendIndent().AppendLine("\t(");
            }

            if (SelectQuery.From.Tables.Count == 0 && SelectQuery.Select.Columns.Count == 1)
            {
                if (SelectQuery.Select.Columns[0].Expression is SqlFunction)
                {
                    var func = (SqlFunction)SelectQuery.Select.Columns[0].Expression;

                    if (func.Name == "Iif" && func.Parameters.Length == 3 && func.Parameters[0] is SelectQuery.SearchCondition)
                    {
                        var sc = (SelectQuery.SearchCondition)func.Parameters[0];

                        if (sc.Conditions.Count == 1 && sc.Conditions[0].Predicate is SelectQuery.Predicate.FuncLike)
                        {
                            var p = (SelectQuery.Predicate.FuncLike)sc.Conditions[0].Predicate;

                            if (p.Function.Name == "EXISTS")
                            {
                                BuildAnyAsCount();
                                return;
                            }
                        }
                    }
                }
                else if (SelectQuery.Select.Columns[0].Expression is SelectQuery.SearchCondition)
                {
                    var sc = (SelectQuery.SearchCondition)SelectQuery.Select.Columns[0].Expression;

                    if (sc.Conditions.Count == 1 && sc.Conditions[0].Predicate is SelectQuery.Predicate.FuncLike)
                    {
                        var p = (SelectQuery.Predicate.FuncLike)sc.Conditions[0].Predicate;

                        if (p.Function.Name == "EXISTS")
                        {
                            BuildAnyAsCount();
                            return;
                        }
                    }
                }
            }

            base.BuildSql(commandNumber, selectQuery, sb, indent, skipAlias);

            if (_identityField != null)
                sb.AppendLine("\t)");
        }

        SelectQuery.Column _selectColumn;

        void BuildAnyAsCount()
        {
            SelectQuery.SearchCondition cond;

            if (SelectQuery.Select.Columns[0].Expression is SqlFunction)
            {
                var func = (SqlFunction)SelectQuery.Select.Columns[0].Expression;
                cond = (SelectQuery.SearchCondition)func.Parameters[0];
            }
            else
            {
                cond = (SelectQuery.SearchCondition)SelectQuery.Select.Columns[0].Expression;
            }

            var exist = ((SelectQuery.Predicate.FuncLike)cond.Conditions[0].Predicate).Function;
            var query = (SelectQuery)exist.Parameters[0];

            _selectColumn = new SelectQuery.Column(SelectQuery, new SqlExpression(cond.Conditions[0].IsNot ? "CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END" : "CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END"), SelectQuery.Select.Columns[0].Alias);

            BuildSql(0, query, StringBuilder);

            _selectColumn = null;
        }

        protected override IEnumerable<SelectQuery.Column> GetSelectedColumns()
        {
            if (_selectColumn != null)
                return new[] { _selectColumn };

            //if (NeedSkip && !SelectQuery.OrderBy.IsEmpty)
            //    return AlternativeGetSelectedColumns(base.GetSelectedColumns);

            return base.GetSelectedColumns();
        }

        protected override void BuildGetIdentity()
        {
            StringBuilder
                    .AppendLine(";")
                    .AppendLine("SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1");
        }

        protected override void BuildCommand(int commandNumber)
        {
            StringBuilder.AppendLine("SELECT identity_val_local() FROM SYSIBM.SYSDUMMY1");
        }

        protected override void BuildSql()
        {
            AlternativeBuildSql(false, base.BuildSql);
        }

        protected override void BuildSelectClause()
        {
            if (SelectQuery.From.Tables.Count == 0)
            {
                AppendIndent().AppendLine("SELECT");
                BuildColumns();
                AppendIndent().AppendLine("FROM SYSIBM.SYSDUMMY1 FETCH FIRST 1 ROW ONLY");
            }
            else
                base.BuildSelectClause();
        }

        protected override string LimitFormat
        {
            get { return SelectQuery.Select.SkipValue == null ? "FETCH FIRST {0} ROWS ONLY" : null; }
        }

        protected override void BuildFunction(SqlFunction func)
        {
            func = ConvertFunctionParameters(func);
            base.BuildFunction(func);
        }

        protected override void BuildFromClause()
        {
            if (!SelectQuery.IsUpdate)
                base.BuildFromClause();
        }

        protected override void BuildValue(object value)
        {
            if (value is Guid)
            {
                var s = ((Guid)value).ToString("N");

                StringBuilder
                    .Append("Cast(x'")
                    .Append(s.Substring(6, 2))
                    .Append(s.Substring(4, 2))
                    .Append(s.Substring(2, 2))
                    .Append(s.Substring(0, 2))
                    .Append(s.Substring(10, 2))
                    .Append(s.Substring(8, 2))
                    .Append(s.Substring(14, 2))
                    .Append(s.Substring(12, 2))
                    .Append(s.Substring(16, 16))
                    .Append("' as char(16) for bit data)");
            }
            else
                base.BuildValue(value);
        }

        protected override void BuildColumnExpression(ISqlExpression expr, string alias, ref bool addAlias)
        {
            var wrap = false;

            if (expr.SystemType == typeof(bool))
            {
                if (expr is SelectQuery.SearchCondition)
                    wrap = true;
                else
                {
                    var ex = expr as SqlExpression;
                    wrap = ex != null && ex.Expr == "{0}" && ex.Parameters.Length == 1 && ex.Parameters[0] is SelectQuery.SearchCondition;
                }
            }

            if (wrap) StringBuilder.Append("CASE WHEN ");
            base.BuildColumnExpression(expr, alias, ref addAlias);
            if (wrap) StringBuilder.Append(" THEN 1 ELSE 0 END");
        }

        public static LinqToDB.DataProvider.DB2.DB2IdentifierQuoteMode IdentifierQuoteMode = LinqToDB.DataProvider.DB2.DB2IdentifierQuoteMode.Auto;

        public override object Convert(object value, ConvertType convertType)
        {
            switch (convertType)
            {
                case ConvertType.NameToQueryParameter:
                    return "@" + value;

                case ConvertType.NameToCommandParameter:
                case ConvertType.NameToSprocParameter:
                    return ":" + value;

                case ConvertType.SprocParameterToName:
                    if (value != null)
                    {
                        var str = value.ToString();
                        return str.Length > 0 && str[0] == ':' ? str.Substring(1) : str;
                    }

                    break;

                case ConvertType.NameToQueryField:
                case ConvertType.NameToQueryFieldAlias:
                case ConvertType.NameToQueryTable:
                case ConvertType.NameToQueryTableAlias:
                    if (value != null && IdentifierQuoteMode != LinqToDB.DataProvider.DB2.DB2IdentifierQuoteMode.None)
                    {
                        var name = value.ToString();

                        if (name.Length > 0 && name[0] == '"')
                            return name;

                        if (IdentifierQuoteMode == LinqToDB.DataProvider.DB2.DB2IdentifierQuoteMode.Quote ||
                            name.StartsWith("_") ||
                            name.Any(c => char.IsLower(c) || char.IsWhiteSpace(c)))
                            return '"' + name + '"';
                    }

                    break;
            }

            return value;
        }

        protected override void BuildInsertOrUpdateQuery()
        {
            BuildInsertOrUpdateQueryAsMerge("FROM SYSIBM.SYSDUMMY1 FETCH FIRST 1 ROW ONLY");
        }

        protected override void BuildEmptyInsert()
        {
            StringBuilder.Append("VALUES ");

            foreach (var col in SelectQuery.Insert.Into.Fields)
                StringBuilder.Append("(DEFAULT)");

            StringBuilder.AppendLine();
        }

        protected override void BuildCreateTableIdentityAttribute1(SqlField field)
        {
            StringBuilder.Append("GENERATED ALWAYS AS IDENTITY");
        }
    }
}
