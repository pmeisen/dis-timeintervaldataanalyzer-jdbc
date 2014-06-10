package net.meisen.dissertation.jdbc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import net.meisen.dissertation.jdbc.protocol.Protocol;

public class TidaConnection extends BaseWrapper implements Connection {
	private final ServerProperties serverProperties;
	private final Protocol protocol;
	private final Socket socket;

	private SQLWarning rootWarning;

	private boolean readOnly;
	private boolean closed;
	private int holdability = -1;

	public TidaConnection(final ServerProperties serverProperties)
			throws SQLException {
		this.serverProperties = serverProperties;

		// create the Socket and the Protocol on it
		socket = new Socket();
		try {
			socket.connect(new InetSocketAddress("localhost", 7001), 1000);
			socket.setSoTimeout(serverProperties.getTimeout());
			this.protocol = new Protocol(socket);
		} catch (final IOException e) {
			throw TidaSqlExceptions.createException(2003, e,
					serverProperties.getRawJdbc());
		}

		this.readOnly = false;
		this.closed = false;
	}

	@Override
	public Statement createStatement() throws SQLException {
		return createStatement(ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public Statement createStatement(final int resultSetType,
			final int resultSetConcurrency) throws SQLException {
		return createStatement(resultSetType, resultSetConcurrency,
				getHoldability());
	}

	@Override
	public Statement createStatement(final int resultSetType,
			final int resultSetConcurrency, final int resultSetHoldability)
			throws SQLException {
		checkClosed();

		return new TidaStatement(null, resultSetType, resultSetConcurrency,
				resultSetHoldability, Statement.NO_GENERATED_KEYS);
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

		try {
			this.protocol.close();
			this.socket.close();
		} catch (final IOException e) {
			throw TidaSqlExceptions.createException(2004);
		} finally {
			this.closed = true;
		}
	}

	@Override
	public boolean isClosed() {
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
		checkClosed();

		return rootWarning;
	}

	/**
	 * Adds a warning to the chain.
	 * 
	 * @param warning
	 *            the warning to be added
	 */
	public void addWarning(final SQLWarning warning) {
		if (rootWarning == null) {
			rootWarning = warning;
		} else {
			rootWarning.setNextWarning(warning);
		}
	}

	@Override
	public void clearWarnings() throws SQLException {
		checkClosed();

		rootWarning = null;
	}

	@Override
	public PreparedStatement prepareStatement(final String sql)
			throws SQLException {
		return prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public PreparedStatement prepareStatement(final String sql,
			final int resultSetType, final int resultSetConcurrency)
			throws SQLException {
		return prepareStatement(sql, resultSetType, resultSetConcurrency,
				getHoldability());
	}

	@Override
	public PreparedStatement prepareStatement(final String sql,
			final int resultSetType, final int resultSetConcurrency,
			final int resultSetHoldability) throws SQLException {
		return new TidaStatement(sql, resultSetType, resultSetConcurrency,
				resultSetHoldability, Statement.NO_GENERATED_KEYS);
	}

	@Override
	public PreparedStatement prepareStatement(final String sql,
			final int autoGeneratedKeys) throws SQLException {
		return new TidaStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY, getHoldability(), autoGeneratedKeys);
	}

	@Override
	public PreparedStatement prepareStatement(final String sql,
			final int[] columnIndexes) throws SQLException {
		return new TidaStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY, getHoldability(), columnIndexes);
	}

	@Override
	public PreparedStatement prepareStatement(final String sql,
			final String[] columnNames) throws SQLException {
		return new TidaStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY, getHoldability(), columnNames);
	}

	@Override
	public CallableStatement prepareCall(final String sql) throws SQLException {
		return prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
	}

	@Override
	public CallableStatement prepareCall(final String sql,
			final int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return prepareCall(sql, resultSetType, resultSetConcurrency,
				getHoldability());
	}

	@Override
	public CallableStatement prepareCall(final String sql,
			final int resultSetType, final int resultSetConcurrency,
			final int resultSetHoldability) throws SQLException {
		throw TidaSqlExceptions.createException(1002);
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		checkClosed();

		return Collections.<String, Class<?>> emptyMap();
	}

	@Override
	public void setTypeMap(final Map<String, Class<?>> map) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(1004);
	}

	@Override
	public void setHoldability(final int holdability) throws SQLException {
		if (holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT
				|| holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT) {
			this.holdability = holdability;
		} else {
			this.holdability = -1;
		}
	}

	@Override
	public int getHoldability() throws SQLException {
		return holdability == -1 ? getMetaData().getResultSetHoldability()
				: holdability;
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
	public boolean isValid(final int timeout) throws SQLException {

		// check the timeout value
		if (timeout < 0) {
			throw TidaSqlExceptions.createException(1003);
		} else if (isClosed()) {
			return false;
		}

		// create a thread to fire a query
		final boolean[] flag = new boolean[] { true };
		final Thread t = new Thread() {

			public void run() {
				try {
					protocol.writeAndHandle("ALIVE", null);
				} catch (final Throwable e) {
					flag[0] = false;
				}
			}
		};

		// start the thread with a timeout in ms
		final int timeoutInMs = timeout * 1000;
		try {
			t.start();
			final long start = System.currentTimeMillis();
			t.join(timeoutInMs);

			try {
				t.setContextClassLoader(null);
			} catch (final Throwable th) {
				// nothing to do
			}

			if (timeout == 0) {
				return flag[0];
			} else {
				return flag[0]
						&& (System.currentTimeMillis() - start) < timeout;
			}
		} catch (final Throwable e) {
			return false;
		}
	}

	@Override
	public void setClientInfo(final String name, final String value)
			throws SQLClientInfoException {
		throw TidaSqlExceptions.createClientInfoException(1005, name, value);
	}

	@Override
	public void setClientInfo(final Properties properties)
			throws SQLClientInfoException {

		if (!isClosed() && (properties == null || properties.isEmpty())) {
			return;
		} else {
			throw TidaSqlExceptions.createClientInfoException(1006,
					properties.toString());
		}
	}

	@Override
	public String getClientInfo(final String name) throws SQLException {
		checkClosed();

		return null;
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		checkClosed();

		return new Properties();
	}

	@Override
	public Array createArrayOf(final String typeName, final Object[] elements)
			throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Struct createStruct(final String typeName, final Object[] attributes)
			throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob createClob() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob createBlob() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob createNClob() throws SQLException {
		checkClosed();

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(1007);
	}

	/**
	 * Gets the {@code ServerProperties} for the {@code TidaConnection}.
	 * 
	 * @return the {@code ServerProperties} for the {@code TidaConnection}
	 */
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
