package net.meisen.dissertation.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import net.meisen.dissertation.jdbc.protocol.DataType;

/**
 * Meta-data for {@code TidaResultSet} instances.
 * 
 * @author pmeisen
 * 
 */
public class TidaResultSetMetaData extends BaseWrapper implements
		ResultSetMetaData {

	private final TidaResultSet resultSet;

	/**
	 * The {@code ResultSet} the meta-data instance is created for.
	 * 
	 * @param resultSet
	 */
	public TidaResultSetMetaData(final TidaResultSet resultSet) {
		this.resultSet = resultSet;
	}

	@Override
	public int getColumnCount() throws SQLException {

		if (TidaResultSetType.MODIFY.equals(resultSet.getResultSetType())) {
			throw TidaSqlExceptions.createException(6001);
		}

		final DataType[] header = resultSet.getHeaderTypes();
		if (header == null) {
			throw TidaSqlExceptions.createException(6000);
		} else {
			return header.length;
		}
	}

	@Override
	public boolean isAutoIncrement(final int column) throws SQLException {
		return false;
	}

	@Override
	public boolean isCaseSensitive(final int column) throws SQLException {
		return true;
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
		return ResultSetMetaData.columnNullableUnknown;
	}

	@Override
	public boolean isSigned(final int column) throws SQLException {
		final DataType type = resultSet.getHeaderType(column);

		if (type == null) {
			throw TidaSqlExceptions.createException(6002, "" + column);
		} else {
			return type.isSigned();
		}
	}

	@Override
	public int getColumnDisplaySize(final int column) throws SQLException {
		final DataType type = resultSet.getHeaderType(column);

		if (type == null) {
			throw TidaSqlExceptions.createException(6002, "" + column);
		} else {
			if (type.getPrecision() != 0) {
				return type.getPrecision() + (type.getScale() == 0 ? 0 : 1);
			} else {
				return 100;
			}
		}
	}

	@Override
	public String getColumnLabel(final int column) throws SQLException {
		final String name = resultSet.getHeaderLabel(column);

		if (name == null) {
			throw TidaSqlExceptions.createException(6002, "" + column);
		} else {
			return name;
		}
	}

	@Override
	public String getColumnName(final int column) throws SQLException {
		return getColumnLabel(column);
	}

	@Override
	public String getSchemaName(final int column) throws SQLException {
		return "";
	}

	@Override
	public int getPrecision(final int column) throws SQLException {
		final DataType type = resultSet.getHeaderType(column);

		if (type == null) {
			throw TidaSqlExceptions.createException(6002, "" + column);
		} else {
			return type.getPrecision();
		}
	}

	@Override
	public int getScale(final int column) throws SQLException {
		final DataType type = resultSet.getHeaderType(column);

		if (type == null) {
			throw TidaSqlExceptions.createException(6002, "" + column);
		} else {
			return type.getScale();
		}
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
		final DataType type = resultSet.getHeaderType(column);

		if (type == null) {
			throw TidaSqlExceptions.createException(6002, "" + column);
		} else {
			return type.getSqlType();
		}
	}

	@Override
	public String getColumnTypeName(final int column) throws SQLException {
		final DataType type = resultSet.getHeaderType(column);

		if (type == null) {
			throw TidaSqlExceptions.createException(6002, "" + column);
		} else {
			return type.getRepresentorClass().getSimpleName();
		}
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
		final DataType type = resultSet.getHeaderType(column);

		if (type == null) {
			throw TidaSqlExceptions.createException(6002, "" + column);
		} else {
			return type.getRepresentorClass().getName();
		}
	}

}
