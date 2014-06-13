package net.meisen.dissertation.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public class TidaResultSet extends BaseConnectionWrapper implements ResultSet {
	private final String sql;
	private final QueryResponseHandler handler;

	private final int resultSetHoldability;
	private final int resultSetType;
	private final int resultSetConcurrency;

	private final TidaResultSetType type;

	public TidaResultSet(final TidaStatement statement, final String sql,
			final TidaResultSetType expectedType, final int resultSetType,
			final int resultSetConcurrency, final int resultSetHoldability)
			throws SQLException {
		super(statement);

		// check the query
		if (sql == null) {
			throw TidaSqlExceptions.createException(4004);
		}

		// check the resultSet settings
		if (resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
			this.resultSetType = resultSetType;
		} else if (resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE
				|| resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
			throw TidaSqlExceptions.createNotSupportedException(4013, ""
					+ resultSetType);
		} else {
			throw TidaSqlExceptions.createException(4014, "" + resultSetType);
		}

		if (resultSetConcurrency == ResultSet.CONCUR_READ_ONLY) {
			this.resultSetConcurrency = resultSetConcurrency;
		} else if (resultSetConcurrency == ResultSet.CONCUR_UPDATABLE) {
			throw TidaSqlExceptions.createNotSupportedException(4015, ""
					+ resultSetType);
		} else {
			throw TidaSqlExceptions.createException(4016, "" + resultSetType);
		}

		if (resultSetHoldability == ResultSet.CLOSE_CURSORS_AT_COMMIT
				|| resultSetHoldability == ResultSet.HOLD_CURSORS_OVER_COMMIT) {
			this.resultSetHoldability = resultSetHoldability;
		} else {
			throw TidaSqlExceptions.createException(4017, "" + resultSetType);
		}

		// create the handler for the resultSet
		final String handlerClass = statement.getDriverProperties()
				.getHandlerClass();
		try {
			@SuppressWarnings("unchecked")
			final Class<? extends QueryResponseHandler> clazz = (Class<? extends QueryResponseHandler>) Class
					.forName(handlerClass);
			this.handler = clazz.newInstance();
		} catch (final Exception e) {
			throw TidaSqlExceptions.createException(4003, e, handlerClass);
		}

		// set the SQL statement
		this.sql = sql;

		// fire the query
		fireQuery(sql);
		handler.setSingleResultExpected(TidaResultSetType.INTVALUE
				.equals(expectedType));

		if (handler.hasHeader()) {

			// it's a ResultSet
			if (handler.isSingleResultExpected()) {
				close();
				throw TidaSqlExceptions.createException(4006);
			}
			this.type = TidaResultSetType.RESULTSET;
		} else if (TidaResultSetType.RESULTSET.equals(expectedType)) {
			throw TidaSqlExceptions.createException(4005);
		} else {

			// handle the response once for the result
			handleResponse(handler);

			/*
			 * If the handler expected the singleResult it is done because it
			 * already read the end or throw an exception, otherwise we have to
			 * read the end now.
			 */
			if (!handler.isSingleResultExpected()) {
				handleResponse(handler);
			}

			// validate that the end was really read
			if (!handler.reachedEOR()) {
				// close the ResultSet
				this.close();

				// throw the exception this ResultSet was not for selects
				throw TidaSqlExceptions.createException(4005);
			}
			this.type = TidaResultSetType.INTVALUE;
		}
	}

	public boolean isResultSetType(final TidaResultSetType expectedType) {
		return type.equals(expectedType);
	}

	@Override
	protected BaseConnectionWrapper getProtocolScope() {
		return getParent();
	}

	@Override
	protected boolean doCloseOnCommit() {
		return resultSetHoldability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	/**
	 * Checks if the {@code ResultSet} is closed, if so an exception is thrown,
	 * otherwise nothing happens.
	 * 
	 * @throws SQLException
	 *             if the {@code ResultSet} is closed
	 * 
	 * @see #isClosed()
	 */
	protected void checkClosed() throws SQLException {
		if (isClosed()) {
			throw TidaSqlExceptions.createException(4999);
		}
	}

	@Override
	public boolean next() throws SQLException {
		checkClosed();

		if (handler.reachedEOR()) {
			return false;
		} else {

			// read the next one
			handleResponse(handler);

			// if no eor there was a next
			return !handler.reachedEOR();
		}
	}

	@Override
	public boolean wasNull() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String getString(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getBoolean(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte getByte(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public short getShort(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInt(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDouble(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public BigDecimal getBigDecimal(final int columnIndex, final int scale)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBytes(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getAsciiStream(final int columnIndex)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getUnicodeStream(final int columnIndex)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getBinaryStream(final int columnIndex)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getString(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getBoolean(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte getByte(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public short getShort(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInt(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDouble(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public BigDecimal getBigDecimal(final String columnLabel, final int scale)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBytes(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getAsciiStream(final String columnLabel)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getUnicodeStream(final String columnLabel)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getBinaryStream(final String columnLabel)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
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
	public String getCursorName() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int findColumn(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Reader getCharacterStream(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getCharacterStream(final String columnLabel)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getBigDecimal(final String columnLabel)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		checkClosed();

		return handler.getLastResult() == null;
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		checkClosed();

		return handler.reachedEOR();
	}

	@Override
	public boolean isFirst() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4007);
	}

	@Override
	public boolean isLast() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4007);
	}

	@Override
	public void beforeFirst() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4007);
	}

	@Override
	public void afterLast() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4007);
	}

	@Override
	public boolean first() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4007);
	}

	@Override
	public boolean last() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4007);
	}

	@Override
	public int getRow() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4007);
	}

	@Override
	public boolean absolute(final int row) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4007);
	}

	@Override
	public boolean relative(final int rows) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4007);
	}

	@Override
	public boolean previous() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4007);
	}

	@Override
	public void setFetchDirection(final int direction) throws SQLException {
		checkClosed();

		if (direction != ResultSet.FETCH_FORWARD) {
			throw TidaSqlExceptions.createNotSupportedException(4000, ""
					+ direction);
		}
	}

	@Override
	public int getFetchDirection() throws SQLException {
		checkClosed();

		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(final int rows) throws SQLException {
		checkClosed();

		if (rows > 1) {
			throw TidaSqlExceptions
					.createNotSupportedException(4001, "" + rows);
		} else if (rows < 0) {
			throw TidaSqlExceptions.createException(4002, "" + rows);
		}
	}

	@Override
	public int getFetchSize() throws SQLException {
		checkClosed();

		return 1;
	}

	@Override
	public int getType() throws SQLException {
		checkClosed();

		return resultSetType;
	}

	@Override
	public int getConcurrency() throws SQLException {
		checkClosed();

		return resultSetConcurrency;
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4008);
	}

	@Override
	public boolean rowInserted() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4008);
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4008);
	}

	@Override
	public void updateNull(final int columnIndex) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBoolean(final int columnIndex, final boolean x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateByte(final int columnIndex, final byte x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateShort(final int columnIndex, final short x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateInt(final int columnIndex, final int x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateLong(final int columnIndex, final long x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateFloat(final int columnIndex, final float x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateDouble(final int columnIndex, final double x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBigDecimal(final int columnIndex, final BigDecimal x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateString(final int columnIndex, final String x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBytes(final int columnIndex, final byte[] x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateDate(final int columnIndex, final Date x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateTime(final int columnIndex, final Time x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateTimestamp(final int columnIndex, final Timestamp x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateAsciiStream(final int columnIndex, final InputStream x,
			final int length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBinaryStream(final int columnIndex, final InputStream x,
			final int length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateCharacterStream(final int columnIndex, final Reader x,
			final int length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateObject(final int columnIndex, final Object x,
			final int scaleOrLength) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateObject(final int columnIndex, final Object x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateNull(final String columnLabel) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBoolean(final String columnLabel, final boolean x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateByte(final String columnLabel, final byte x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateShort(final String columnLabel, final short x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateInt(final String columnLabel, final int x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateLong(final String columnLabel, final long x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateFloat(final String columnLabel, final float x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateDouble(final String columnLabel, final double x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBigDecimal(final String columnLabel, final BigDecimal x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateString(final String columnLabel, final String x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBytes(final String columnLabel, final byte[] x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateDate(final String columnLabel, final Date x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateTime(final String columnLabel, final Time x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateTimestamp(final String columnLabel, final Timestamp x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateAsciiStream(final String columnLabel,
			final InputStream x, final int length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBinaryStream(final String columnLabel,
			final InputStream x, final int length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateCharacterStream(final String columnLabel,
			final Reader reader, final int length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateObject(final String columnLabel, final Object x,
			final int scaleOrLength) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateObject(final String columnLabel, final Object x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void insertRow() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4010);
	}

	@Override
	public void updateRow() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void deleteRow() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4011);
	}

	@Override
	public void refreshRow() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4007);
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4012);
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4012);
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4012);
	}

	@Override
	public Statement getStatement() throws SQLException {
		return (TidaStatement) getParent();
	}

	@Override
	public Object getObject(final int columnIndex,
			final Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Ref getRef(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob getBlob(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob getClob(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Array getArray(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject(final String columnLabel,
			final Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Ref getRef(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Blob getBlob(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Clob getClob(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Array getArray(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(final int columnIndex, final Calendar cal)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(final String columnLabel, final Calendar cal)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(final int columnIndex, final Calendar cal)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(final String columnLabel, final Calendar cal)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(final int columnIndex, final Calendar cal)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(final String columnLabel, final Calendar cal)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URL getURL(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public URL getURL(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateRef(final int columnIndex, final Ref x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateRef(final String columnLabel, final Ref x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBlob(final int columnIndex, final Blob x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBlob(final String columnLabel, final Blob x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateClob(final int columnIndex, final Clob x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateClob(final String columnLabel, final Clob x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateArray(final int columnIndex, final Array x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateArray(final String columnLabel, final Array x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public RowId getRowId(final int columnIndex) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createNotSupportedException(4019);
	}

	@Override
	public RowId getRowId(final String columnLabel) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createNotSupportedException(4019);
	}

	@Override
	public void updateRowId(final int columnIndex, final RowId x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateRowId(final String columnLabel, final RowId x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public int getHoldability() throws SQLException {
		checkClosed();

		return resultSetHoldability;
	}

	@Override
	public void updateNString(int columnIndex, String nString)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateNString(String columnLabel, String nString)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NClob getNClob(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SQLXML getSQLXML(final int columnIndex) throws SQLException {
		throw TidaSqlExceptions.createException(4018);
	}

	@Override
	public SQLXML getSQLXML(final String columnLabel) throws SQLException {
		throw TidaSqlExceptions.createException(4018);
	}

	@Override
	public void updateSQLXML(final int columnIndex, final SQLXML xmlObject)
			throws SQLException {
		throw TidaSqlExceptions.createException(4018);
	}

	@Override
	public void updateSQLXML(final String columnLabel, final SQLXML xmlObject)
			throws SQLException {
		throw TidaSqlExceptions.createException(4018);
	}

	@Override
	public String getNString(final int columnIndex) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getNString(final String columnLabel) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream(final int columnIndex)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reader getNCharacterStream(final String columnLabel)
			throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateNCharacterStream(final int columnIndex, final Reader x,
			final long length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);

	}

	@Override
	public void updateNCharacterStream(final String columnLabel,
			final Reader reader, long length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateAsciiStream(final int columnIndex, final InputStream x,
			final long length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBinaryStream(final int columnIndex, final InputStream x,
			final long length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateCharacterStream(final int columnIndex, final Reader x,
			final long length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateAsciiStream(final String columnLabel,
			final InputStream x, final long length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBinaryStream(final String columnLabel,
			final InputStream x, long length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateCharacterStream(final String columnLabel,
			final Reader reader, final long length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBlob(final int columnIndex,
			final InputStream inputStream, final long length)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBlob(final String columnLabel,
			final InputStream inputStream, long length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateClob(final int columnIndex, final Reader reader,
			final long length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateClob(final String columnLabel, final Reader reader,
			final long length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateNClob(final int columnIndex, final Reader reader,
			final long length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateNClob(final String columnLabel, final Reader reader,
			final long length) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateNCharacterStream(final int columnIndex, final Reader x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateNCharacterStream(final String columnLabel,
			final Reader reader) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateAsciiStream(final int columnIndex, final InputStream x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBinaryStream(final int columnIndex, final InputStream x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateCharacterStream(final int columnIndex, final Reader x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateAsciiStream(final String columnLabel, final InputStream x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBinaryStream(final String columnLabel, final InputStream x)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateCharacterStream(final String columnLabel,
			final Reader reader) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBlob(final int columnIndex, final InputStream inputStream)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateBlob(final String columnLabel,
			final InputStream inputStream) throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateClob(final int columnIndex, final Reader reader)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateClob(final String columnLabel, final Reader reader)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateNClob(final int columnIndex, final Reader reader)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public void updateNClob(final String columnLabel, final Reader reader)
			throws SQLException {
		checkClosed();

		throw TidaSqlExceptions.createException(4009);
	}

	@Override
	public String toString() {
		return "[" + getClass().getSimpleName() + "] " + sql;
	}
}
