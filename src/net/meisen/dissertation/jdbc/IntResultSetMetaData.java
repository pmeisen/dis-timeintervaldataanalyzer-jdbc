package net.meisen.dissertation.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import net.meisen.dissertation.jdbc.protocol.DataType;

/**
 * The {@code ResultSetMetaData} for {@code IntResultSet} instances.
 * 
 * @author pmeisen
 * 
 */
public class IntResultSetMetaData extends BaseWrapper implements
		ResultSetMetaData {

	@Override
	public int getColumnCount() throws SQLException {
		return 1;
	}

	@Override
	public boolean isAutoIncrement(final int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isCaseSensitive(final int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isSearchable(final int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isCurrency(final int column) throws SQLException {
		return false;
	}

	@Override
	public int isNullable(final int column) throws SQLException {
		return ResultSetMetaData.columnNoNulls;
	}

	@Override
	public boolean isSigned(final int column) throws SQLException {
		return true;
	}

	@Override
	public int getColumnDisplaySize(final int column) throws SQLException {
		return ("" + Integer.MAX_VALUE).length();
	}

	@Override
	public String getColumnLabel(final int column) throws SQLException {
		return IntResultSet.COL_LABEL;
	}

	@Override
	public String getColumnName(final int column) throws SQLException {
		return IntResultSet.COL_LABEL;
	}

	@Override
	public String getSchemaName(final int column) throws SQLException {
		return "";
	}

	@Override
	public int getPrecision(final int column) throws SQLException {
		return DataType.INT.getPrecision();
	}

	@Override
	public int getScale(final int column) throws SQLException {
		return DataType.INT.getScale();
	}

	@Override
	public String getTableName(final int column) throws SQLException {
		return "";
	}

	@Override
	public String getCatalogName(final int column) throws SQLException {
		return "";
	}

	@Override
	public int getColumnType(final int column) throws SQLException {
		return DataType.INT.getSqlType();
	}

	@Override
	public String getColumnTypeName(final int column) throws SQLException {
		return DataType.INT.getRepresentorClass().getSimpleName();
	}

	@Override
	public boolean isReadOnly(final int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isWritable(final int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(final int column) throws SQLException {
		return false;
	}

	@Override
	public String getColumnClassName(final int column) throws SQLException {
		return DataType.INT.getRepresentorClass().getName();
	}

}
