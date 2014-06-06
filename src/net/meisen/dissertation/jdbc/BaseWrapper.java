package net.meisen.dissertation.jdbc;

import java.sql.SQLException;
import java.sql.Wrapper;

/**
 * A base-implementation of a {@code Wrapper}.
 * 
 * @author pmeisen
 * 
 */
public class BaseWrapper implements Wrapper {

	@Override
	@SuppressWarnings("unchecked")
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (isWrapperFor(iface)) {
			return (T) this;
		} else {
			throw TidaSqlExceptions.createException(9000, iface.toString());
		}
	}

	@Override
	public boolean isWrapperFor(final Class<?> iface) throws SQLException {
		return (iface != null && iface.isAssignableFrom(getClass()));
	}
}
