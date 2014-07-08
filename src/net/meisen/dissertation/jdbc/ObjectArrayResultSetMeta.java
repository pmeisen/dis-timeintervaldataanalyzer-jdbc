package net.meisen.dissertation.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * A {@code ObjectArrayResultSet} for an {@code ObjectArrayResultSet}.
 * 
 * @author pmeisen
 * 
 */
public class ObjectArrayResultSetMeta extends BaseWrapper implements
		ResultSetMetaData {

	private final String[] names;

	/**
	 * Default constructor specifying the {@code names} of the
	 * {@code ObjectArrayResultSet}.
	 * 
	 * @param names
	 *            the labels of the {@code ObjectArrayResultSet}
	 */
	public ObjectArrayResultSetMeta(final String[] names) {
		this.names = names;
	}

	/**
	 * Checks if the {@code ResultSet} has a column with the specified index.
	 * 
	 * @param columnIndex
	 *            the index to be checked
	 * 
	 * @throws SQLException
	 *             if the {@code ResultSet} does not have a column with the
	 *             specified {@code columnIndex}.
	 */
	protected void checkColumnIndex(final int columnIndex) throws SQLException {
		if (columnIndex < 1 || columnIndex > names.length) {
			throw TidaSqlExceptions.createException(8000, "" + names.length);
		}
	}

	@Override
	public int getColumnCount() throws SQLException {
		return names.length;
	}

	@Override
	public boolean isAutoIncrement(final int column) throws SQLException {
		checkColumnIndex(column);

		return false;
	}

	@Override
	public boolean isCaseSensitive(final int column) throws SQLException {
		checkColumnIndex(column);

		return true;
	}

	@Override
	public boolean isSearchable(final int column) throws SQLException {
		checkColumnIndex(column);

		return false;
	}

	@Override
	public boolean isCurrency(final int column) throws SQLException {
		checkColumnIndex(column);

		return false;
	}

	@Override
	public int isNullable(final int column) throws SQLException {
		checkColumnIndex(column);

		return ResultSetMetaData.columnNullableUnknown;
	}

	@Override
	public boolean isSigned(final int column) throws SQLException {
		checkColumnIndex(column);

		return false;
	}

	@Override
	public int getColumnDisplaySize(final int column) throws SQLException {
		checkColumnIndex(column);

		return -1;
	}

	@Override
	public String getColumnLabel(final int column) throws SQLException {
		checkColumnIndex(column);

		return names[column - 1];
	}

	@Override
	public String getColumnName(final int column) throws SQLException {
		return getColumnLabel(column);
	}

	@Override
	public String getSchemaName(final int column) throws SQLException {
		checkColumnIndex(column);

		return "";
	}

	@Override
	public int getPrecision(final int column) throws SQLException {
		checkColumnIndex(column);

		return -1;
	}

	@Override
	public int getScale(final int column) throws SQLException {
		checkColumnIndex(column);

		return -1;
	}

	@Override
	public String getTableName(final int column) throws SQLException {
		checkColumnIndex(column);

		return "";
	}

	@Override
	public String getCatalogName(final int column) throws SQLException {
		checkColumnIndex(column);

		return "";
	}

	@Override
	public int getColumnType(final int column) throws SQLException {
		checkColumnIndex(column);

		return Types.JAVA_OBJECT;
	}

	@Override
	public String getColumnTypeName(final int column) throws SQLException {
		checkColumnIndex(column);

		return Object.class.getName();
	}

	@Override
	public boolean isReadOnly(final int column) throws SQLException {
		checkColumnIndex(column);

		return true;
	}

	@Override
	public boolean isWritable(final int column) throws SQLException {
		checkColumnIndex(column);

		return false;
	}

	@Override
	public boolean isDefinitelyWritable(final int column) throws SQLException {
		checkColumnIndex(column);

		return false;
	}

	@Override
	public String getColumnClassName(final int column) throws SQLException {
		checkColumnIndex(column);

		return Object.class.getName();
	}
}
