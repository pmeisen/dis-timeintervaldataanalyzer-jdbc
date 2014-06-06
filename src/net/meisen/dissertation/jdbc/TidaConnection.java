package net.meisen.dissertation.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import net.meisen.dissertation.jdbc.version.Version;

public class TidaConnection extends BaseWrapper implements Connection {
	private final ServerProperties serverProperties;

	private boolean readOnly;
	private boolean closed;

	public TidaConnection(final ServerProperties serverProperties) {
		this.serverProperties = serverProperties;

		this.readOnly = false;
		this.closed = false;
	}

	@Override
	public Statement createStatement() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(final String sql)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall(final String sql) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String nativeSQL(final String sql) throws SQLException {
		return sql;
	}

	@Override
	public void setAutoCommit(final boolean autoCommit) throws SQLException {
		checkClosed();

		// check if the auto-commit should have been disabled
		if (!autoCommit) {
			throw TidaSqlExceptions.createException(1001);
		}
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return true;
	}

	@Override
	public void commit() throws SQLException {
		checkClosed();

		// data is always directly committed, nothing to do
	}

	@Override
	public void rollback() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(1001);
	}

	@Override
	public void close() throws SQLException {
		if (isClosed()) {
			return;
		}

		this.closed = true;
	}

	@Override
	public boolean isClosed() throws SQLException {
		return this.closed;
	}

	@Override
	public TidaDatabaseMetaData getMetaData() throws SQLException {
		checkClosed();

		return new TidaDatabaseMetaData(this);
	}

	@Override
	public void setReadOnly(final boolean readOnly) throws SQLException {
		this.readOnly = readOnly;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return readOnly;
	}

	@Override
	public void setCatalog(final String catalog) throws SQLException {
		// not supported
	}

	@Override
	public String getCatalog() throws SQLException {
		return null;
	}

	@Override
	public void setTransactionIsolation(final int level) throws SQLException {
		throw TidaSqlExceptions.createException(1000);
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return TRANSACTION_NONE;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub
	}

	@Override
	public Statement createStatement(final int resultSetType,
			final int resultSetConcurrency) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(final String sql,
			final int resultSetType, final int resultSetConcurrency)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall(final String sql,
			final int resultSetType, int resultSetConcurrency)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub
	}

	@Override
	public void setHoldability(final int holdability) throws SQLException {
		// TODO Auto-generated method stub
	}

	@Override
	public int getHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(1001);
	}

	@Override
	public Savepoint setSavepoint(final String name) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(1001);
	}

	@Override
	public void rollback(final Savepoint savepoint) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(1001);
	}

	@Override
	public void releaseSavepoint(final Savepoint savepoint) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(1001);
	}

	@Override
	public Statement createStatement(final int resultSetType,
			final int resultSetConcurrency, final int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(final String sql,
			final int resultSetType, final int resultSetConcurrency,
			final int resultSetHoldability) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public CallableStatement prepareCall(final String sql,
			final int resultSetType, final int resultSetConcurrency,
			final int resultSetHoldability) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(final String sql,
			final int autoGeneratedKeys) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(final String sql,
			final int[] columnIndexes) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PreparedStatement prepareStatement(final String sql,
			final String[] columnNames) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob createClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob createBlob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob createNClob() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isValid(final int timeout) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setClientInfo(final String name, final String value)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setClientInfo(final Properties properties)
			throws SQLClientInfoException {
		// TODO Auto-generated method stub
	}

	@Override
	public String getClientInfo(final String name) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Array createArrayOf(final String typeName, final Object[] elements)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Struct createStruct(final String typeName, final Object[] attributes)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public ServerProperties getServerProperties() {
		return serverProperties;
	}

	/**
	 * Checks if the connection is closed, if so an exception is thrown,
	 * otherwise nothing happens.
	 * 
	 * @throws SQLException
	 *             if the connection is closed
	 * 
	 * @see #isClosed()
	 */
	protected void checkClosed() throws SQLException {
		if (isClosed()) {
			throw TidaSqlExceptions.createException(1999);
		}
	}
}
