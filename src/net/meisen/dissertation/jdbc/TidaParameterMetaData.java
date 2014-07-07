package net.meisen.dissertation.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import net.meisen.dissertation.jdbc.TidaStatement.Placeholder;
import net.meisen.dissertation.jdbc.protocol.DataType;

/**
 * Implementation of a {@code ParameterMetaData}.
 * 
 * @author pmeisen
 * 
 */
public class TidaParameterMetaData extends BaseWrapper implements
		ParameterMetaData {

	private final List<Placeholder> placeholders;

	/**
	 * The default constructor specifying the {@code placeholders}.
	 * 
	 * @param placeholders
	 *            the {@code Placeholder} instances defined
	 */
	public TidaParameterMetaData(final List<Placeholder> placeholders) {
		this.placeholders = placeholders == null ? new ArrayList<Placeholder>(0)
				: placeholders;
	}

	/**
	 * Checks if the parameter-index is valid.
	 * 
	 * @param parameterIndex
	 *            the index to be checked (1-based)
	 * 
	 * @return the 0-based index
	 * 
	 * @throws SQLException
	 *             if the index is invalid
	 */
	protected int checkParameter(final int parameterIndex) throws SQLException {
		if (placeholders == null) {
			throw TidaSqlExceptions.createException(3007, "" + parameterIndex);
		}

		final int size = placeholders.size();
		if (parameterIndex < 1 || parameterIndex > size) {
			throw TidaSqlExceptions.createException(3007, "" + parameterIndex);
		}

		return parameterIndex - 1;
	}

	/**
	 * Gets the value for the specified {@code idx}.
	 * 
	 * @param idx
	 *            the index to get the value for
	 * 
	 * @return the value
	 * 
	 * @throws SQLException
	 *             if the index is invalid
	 */
	protected Object get(final int idx) throws SQLException {
		final int pos = checkParameter(idx);
		return this.placeholders.get(pos);
	}

	/**
	 * Gets the {@code DataType} of the specified {@code idx}.
	 * 
	 * @param idx
	 *            the index to get the value for
	 * 
	 * @return the {@code DataType}
	 * 
	 * @throws SQLException
	 *             if the index is invalid
	 */
	protected DataType getDataType(final int idx) throws SQLException {
		final Object o = get(idx);

		if (o == null) {
			return null;
		} else {
			return DataType.find(o.getClass());
		}
	}

	@Override
	public int getParameterCount() throws SQLException {
		return placeholders.size();
	}

	@Override
	public int isNullable(final int param) throws SQLException {
		return parameterNullable;
	}

	@Override
	public boolean isSigned(final int param) throws SQLException {
		final DataType dt = getDataType(param);
		return dt == null ? false : dt.isSigned();
	}

	@Override
	public int getPrecision(final int param) throws SQLException {
		final DataType dt = getDataType(param);
		return dt == null ? -1 : dt.getPrecision();
	}

	@Override
	public int getScale(final int param) throws SQLException {
		final DataType dt = getDataType(param);
		return dt == null ? -1 : dt.getScale();
	}

	@Override
	public int getParameterType(final int param) throws SQLException {
		final DataType dt = getDataType(param);
		return dt == null ? Types.NULL : dt.getSqlType();
	}

	@Override
	public String getParameterTypeName(final int param) throws SQLException {
		final DataType dt = getDataType(param);
		return dt == null ? null : dt.toString();
	}

	@Override
	public String getParameterClassName(final int param) throws SQLException {
		final DataType dt = getDataType(param);
		return dt == null ? null : dt.getRepresentorClass().getName();
	}

	@Override
	public int getParameterMode(final int param) throws SQLException {
		return parameterModeIn;
	}
}
