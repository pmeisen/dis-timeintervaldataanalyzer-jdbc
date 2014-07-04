package net.meisen.dissertation.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

/**
 * The database's meta information about the database itself.
 * 
 * @author pmeisen
 * 
 */
public class TidaDatabaseMetaData extends BaseWrapper implements
		DatabaseMetaData {
	private TidaConnection connection;

	private boolean todoMarker;
	
	/**
	 * The default constructor retrieves the meta-data for the specified
	 * {@code connection}.
	 * 
	 * @param connection
	 *            the {@code TidaConnection} to retrieve the meta-data for
	 */
	public TidaDatabaseMetaData(final TidaConnection connection) {
		this.connection = connection;
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return true;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	@Override
	public String getURL() throws SQLException {
		return connection.getDriverProperties().getURL();
	}

	@Override
	public String getUserName() throws SQLException {
		return connection.getDriverProperties().getUser();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return !nullsAreSortedHigh();
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return nullsAreSortedLow();
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return nullsAreSortedHigh();
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		// TODO retrieve from database
		return Constants.getManifestInfo().getImplementationTitle();
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		// TODO retrieve from database
		return Constants.getVersion().toStringRaw();
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getDriverName() throws SQLException {
		return Constants.getManifestInfo().getImplementationTitle();
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return Constants.getVersion().toStringRaw();
	}

	@Override
	public int getDriverMajorVersion() {
		return Constants.getVersion().getMajorAsInt();
	}

	@Override
	public int getDriverMinorVersion() {
		return Constants.getVersion().getMinorAsInt();
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return storesMixedCaseIdentifiers();
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return "\"";
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		return "";
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getStringFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		return "";
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return "";
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		return "-";
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return true;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType)
			throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return false;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return "";
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		return "";
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		return "";
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return true;
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		return ".";
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return true;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return 0;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return false;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		return 1;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(final int level)
			throws SQLException {
		return level == Connection.TRANSACTION_NONE;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions()
			throws SQLException {
		return false;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly()
			throws SQLException {
		return true;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return true;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getProcedures(final String catalog,
			final String schemaPattern, final String procedureNamePattern)
			throws SQLException {
		return new EmptyResultSet(new String[] { "PROCEDURE_CAT",
				"PROCEDURE_SCHEM", "PROCEDURE_NAME", "", "", "", "REMARKS",
				"PROCEDURE_TYPE", "SPECIFIC_NAME" });
	}

	@Override
	public ResultSet getProcedureColumns(final String catalog,
			final String schemaPattern, final String procedureNamePattern,
			final String columnNamePattern) throws SQLException {
		return new EmptyResultSet(new String[] { "PROCEDURE_CAT",
				"PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME",
				"COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH",
				"SCALE", "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
				"SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
				"ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME" });
	}

	@Override
	public ResultSet getTables(final String catalog,
			final String schemaPattern, final String tableNamePattern,
			final String[] types) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		return getSchemas("", null);
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		return new EmptyResultSet(new String[] { "TABLE_CAT" });
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		// TODO
		return null;
	}

	@Override
	public ResultSet getColumns(final String catalog,
			final String schemaPattern, final String tableNamePattern,
			final String columnNamePattern) throws SQLException {
		final String[] cols = new String[] { "TABLE_CAT", "TABLE_SCHEM",
				"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
				"COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
				"NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF",
				"SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
				"ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATLOG",
				"SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE",
				"IS_AUTOINCREMENT" };

		if (catalog == null || "".equals(catalog)) {

			// TODO
			return null;
		} else {
			return new EmptyResultSet(cols);
		}
	}

	@Override
	public ResultSet getColumnPrivileges(final String catalog,
			final String schema, final String table,
			final String columnNamePattern) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getTablePrivileges(final String catalog,
			final String schemaPattern, final String tableNamePattern)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getBestRowIdentifier(final String catalog,
			final String schema, final String table, final int scope,
			final boolean nullable) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getVersionColumns(final String catalog,
			final String schema, final String table) throws SQLException {
		return new EmptyResultSet(new String[] { "SCOPE", "COLUMN_NAME",
				"DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
				"DECIMAL_DIGITS", "PSEUDO_COLUMN" });
	}

	@Override
	public ResultSet getPrimaryKeys(final String catalog, final String schema,
			final String table) throws SQLException {
		return new EmptyResultSet(new String[] { "TABLE_CAT", "TABLE_SCHEM",
				"TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME" });
	}

	@Override
	public ResultSet getImportedKeys(final String catalog, final String schema,
			final String table) throws SQLException {
		return new EmptyResultSet(new String[] { "PKTABLE_CAT",
				"PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
				"FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME",
				"FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE",
				"FK_NAME", "PK_NAME", "DEFERRABILITY" });
	}

	@Override
	public ResultSet getExportedKeys(final String catalog, final String schema,
			final String table) throws SQLException {
		return new EmptyResultSet(new String[] { "PKTABLE_CAT",
				"PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
				"FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME",
				"FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE",
				"FK_NAME", "PK_NAME", "DEFERRABILITY" });
	}

	@Override
	public ResultSet getCrossReference(final String parentCatalog,
			final String parentSchema, final String parentTable,
			final String foreignCatalog, final String foreignSchema,
			final String foreignTable) throws SQLException {
		return new EmptyResultSet(new String[] { "PKTABLE_CAT",
				"PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
				"FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME",
				"FKCOLUMN_NAME", "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE",
				"FK_NAME", "PK_NAME", "DEFERRABILITY" });
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getIndexInfo(final String catalog, final String schema,
			final String table, final boolean unique, final boolean approximate)
			throws SQLException {
		return new EmptyResultSet(new String[] { "TABLE_CAT", "TABLE_SCHEM",
				"TABLE_NAME", "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME",
				"TYPE", "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
				"CARDINALITY", "PAGES", "FILTER_CONDITION" });
	}

	@Override
	public boolean supportsResultSetType(final int type) throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY == type;
	}

	@Override
	public boolean supportsResultSetConcurrency(final int type,
			final int concurrency) throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY == type
				&& ResultSet.CONCUR_READ_ONLY == concurrency;
	}

	@Override
	public boolean ownUpdatesAreVisible(final int type) throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY == type;
	}

	@Override
	public boolean ownDeletesAreVisible(final int type) throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY == type;
	}

	@Override
	public boolean ownInsertsAreVisible(final int type) throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY == type;
	}

	@Override
	public boolean othersUpdatesAreVisible(final int type) throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY == type;
	}

	@Override
	public boolean othersDeletesAreVisible(final int type) throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY == type;
	}

	@Override
	public boolean othersInsertsAreVisible(final int type) throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY == type;
	}

	@Override
	public boolean updatesAreDetected(final int type) throws SQLException {
		return false;
	}

	@Override
	public boolean deletesAreDetected(final int type) throws SQLException {
		return false;
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		return true;
	}

	@Override
	public ResultSet getUDTs(final String catalog, final String schemaPattern,
			final String typeNamePattern, final int[] types)
			throws SQLException {
		return new EmptyResultSet(
				new String[] { "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
						"CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE" });
	}

	@Override
	public Connection getConnection() throws SQLException {
		return connection;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return true;
	}

	@Override
	public ResultSet getSuperTypes(final String catalog,
			final String schemaPattern, final String typeNamePattern)
			throws SQLException {
		return new EmptyResultSet(new String[] { "TYPE_CAT", "TYPE_SCHEM",
				"TYPE_NAME", "SUPERTYPE_CAT", "SUPERTYPE_SCHEM",
				"SUPERTYPE_NAME" });
	}

	@Override
	public ResultSet getSuperTables(final String catalog,
			final String schemaPattern, final String tableNamePattern)
			throws SQLException {
		return new EmptyResultSet(new String[] { "TABLE_CAT", "TABLE_SCHEM",
				"TABLE_NAME", "SUPERTABLE_NAME" });
	}

	@Override
	public ResultSet getAttributes(final String catalog,
			final String schemaPattern, final String typeNamePattern,
			final String attributeNamePattern) throws SQLException {
		return new EmptyResultSet(new String[] { "TYPE_CAT", "TYPE_SCHEM",
				"TYPE_NAME", "ATTR_NAME", "DATA_TYPE", "ATTR_TYPE_NAME",
				"ATTR_SIZE", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE",
				"REMARKS", "ATTR_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB",
				"CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
				"SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
				"SOURCE_DATA_TYPE" });
	}

	@Override
	public boolean supportsResultSetHoldability(final int holdability)
			throws SQLException {
		return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT
				|| holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return ResultSet.HOLD_CURSORS_OVER_COMMIT;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return Constants.getVersion().getMajorAsInt();
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return Constants.getVersion().getMinorAsInt();
	}

	@Override
	public int getSQLStateType() throws SQLException {
		return -1;
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		return false;
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		return RowIdLifetime.ROWID_VALID_FOREVER;
	}

	@Override
	public ResultSet getSchemas(final String catalog, final String schemaPattern)
			throws SQLException {
		if (catalog == null || "".equals(catalog)) {

			// return the one and only schema
			return null;
		} else {
			return new EmptyResultSet(new String[] { "TABLE_SCHEM",
					"TABLE_CATALOG" });
		}
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		return false;
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSet getFunctions(final String catalog,
			final String schemaPattern, final String functionNamePattern)
			throws SQLException {
		return new EmptyResultSet(new String[] { "FUNCTION_CAT",
				"FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE",
				"SPECIFIC_NAME" });
	}

	@Override
	public ResultSet getFunctionColumns(final String catalog,
			final String schemaPattern, final String functionNamePattern,
			final String columnNamePattern) throws SQLException {
		return new EmptyResultSet(new String[] { "FUNCTION_CAT",
				"FUNCTION_SCHEM", "FUNCTION_NAME", "COLUMN_NAME",
				"COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME", "PRECISION", "LENGTH",
				"SCALE", "RADIX", "NULLABLE", "REMARKS", "CHAR_OCTET_LENGTH",
				"ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME" });
	}

}
