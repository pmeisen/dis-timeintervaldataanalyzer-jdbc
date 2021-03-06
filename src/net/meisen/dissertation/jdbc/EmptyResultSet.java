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
import java.util.Arrays;
import java.util.Calendar;
import java.util.Map;

/**
 * A {@code ResultSet} which does not return any values at all.
 *
 * @author pmeisen
 */
public class EmptyResultSet extends BaseWrapper implements ResultSet {
    private final String[] columns;

    private boolean closed;

    /**
     * Constructor creating an empty {@code ResultSet} without any columns.
     */
    public EmptyResultSet() {
        this(new String[0]);
    }

    /**
     * Constructor creating an empty {@code ResultSet} with the specified
     * columns.
     *
     * @param columns the names of the columns of the {@code ResultSet}
     */
    public EmptyResultSet(final String[] columns) {
        this.columns = columns;
        this.closed = false;
    }

    /**
     * Checks if the {@code ResultSet} is closed and throws an exception if so.
     *
     * @throws SQLException if the {@code ResultSet} is closed
     */
    protected void checkClosed() throws SQLException {
        if (closed) {
            throw TidaSqlExceptions.createException(7998);
        }
    }

    /**
     * Checks if the {@code ResultSet} has a column with the specified label.
     *
     * @param columnLabel the label to be checked
     * @throws SQLException if the {@code ResultSet} does not have a column with the
     *                      specified {@code columnLabel}.
     */
    protected void checkColumnLabel(final String columnLabel)
            throws SQLException {

        for (final String colLabel : columns) {
            if (columnLabel.equalsIgnoreCase(colLabel)) {
                return;
            }
        }

        throw TidaSqlExceptions.createException(7002, Arrays.asList(columns)
                .toString());
    }

    /**
     * Gets the index of the column with the specified {@code label}.
     *
     * @param columnLabel the label to get the index for
     * @return the index of the column with the specified label
     */
    protected int getColumnIndex(final String columnLabel) {
        for (int i = 0; i < columns.length; i++) {
            final String colLabel = columns[i];
            if (columnLabel.equalsIgnoreCase(colLabel)) {
                return i;
            }
        }

        return -1;
    }

    /**
     * Checks if the {@code ResultSet} has a column with the specified index.
     *
     * @param columnIndex the index to be checked
     * @throws SQLException if the {@code ResultSet} does not have a column with the
     *                      specified {@code columnIndex}.
     */
    protected void checkColumnIndex(final int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columns.length) {
            throw TidaSqlExceptions.createException(7000, "" + columns.length);
        }
    }

    @Override
    public boolean next() throws SQLException {
        return false;
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();

        return false;
    }

    @Override
    public String getString(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public byte getByte(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public short getShort(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public int getInt(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public long getLong(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public float getFloat(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public double getDouble(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(final int columnIndex, final int scale)
            throws SQLException {

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public Date getDate(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public Time getTime(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public InputStream getAsciiStream(final int columnIndex)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(final int columnIndex)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public InputStream getBinaryStream(final int columnIndex)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public String getString(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getString(getColumnIndex(columnLabel));
    }

    @Override
    public boolean getBoolean(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getBoolean(getColumnIndex(columnLabel));
    }

    @Override
    public byte getByte(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getByte(getColumnIndex(columnLabel));
    }

    @Override
    public short getShort(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getShort(getColumnIndex(columnLabel));
    }

    @Override
    public int getInt(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getInt(getColumnIndex(columnLabel));
    }

    @Override
    public long getLong(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getLong(getColumnIndex(columnLabel));
    }

    @Override
    public float getFloat(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getFloat(getColumnIndex(columnLabel));
    }

    @Override
    public double getDouble(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getDouble(getColumnIndex(columnLabel));
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal(final String columnLabel, final int scale)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getBigDecimal(getColumnIndex(columnLabel));
    }

    @Override
    public byte[] getBytes(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getBytes(getColumnIndex(columnLabel));
    }

    @Override
    public Date getDate(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getDate(getColumnIndex(columnLabel));
    }

    @Override
    public Time getTime(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getTime(getColumnIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getTimestamp(getColumnIndex(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(final String columnLabel)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getAsciiStream(getColumnIndex(columnLabel));
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream(final String columnLabel)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        //noinspection deprecation
        return getUnicodeStream(getColumnIndex(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(final String columnLabel)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getBinaryStream(getColumnIndex(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();

        return new SQLWarning();
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    @Override
    public String getCursorName() throws SQLException {
        checkClosed();

        return "";
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new IntResultSetMetaData();
    }

    @Override
    public Object getObject(final int columnIndex) throws SQLException {
        return getObject(columnIndex, Object.class);
    }

    @Override
    public Object getObject(final String columnLabel) throws SQLException {
        return getObject(columnLabel, Object.class);
    }


    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        final int index = getColumnIndex(columnLabel);
        if (index == -1) {
            throw TidaSqlExceptions.createException(7002, Arrays
                    .asList(columns).toString());
        }

        return index;
    }

    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public Reader getCharacterStream(final String columnLabel)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getCharacterStream(getColumnIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return BigDecimal.valueOf(getColumnIndex(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();

        return false;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();

        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();

        return false;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();

        return false;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();

        return false;
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();

        return false;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();

        return 0;
    }

    @Override
    public boolean absolute(final int row) throws SQLException {
        checkClosed();

        return false;
    }

    @Override
    public boolean relative(final int rows) throws SQLException {
        checkClosed();

        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        checkClosed();

        return false;
    }

    @Override
    public void setFetchDirection(final int direction) throws SQLException {
        // ignore
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(final int rows) throws SQLException {
        checkClosed();
        // ignore
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();

        return 0;
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();

        return ResultSet.TYPE_SCROLL_INSENSITIVE;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();

        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        checkClosed();

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public boolean rowInserted() throws SQLException {
        checkClosed();

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        checkClosed();

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateNull(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBoolean(final int columnIndex, final boolean x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateByte(final int columnIndex, final byte x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateShort(final int columnIndex, final short x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateInt(final int columnIndex, final int x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateLong(final int columnIndex, final long x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateFloat(final int columnIndex, final float x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateDouble(final int columnIndex, final double x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBigDecimal(final int columnIndex, final BigDecimal x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateString(final int columnIndex, final String x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBytes(final int columnIndex, final byte[] x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateDate(final int columnIndex, final Date x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateTime(final int columnIndex, final Time x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateTimestamp(final int columnIndex, Timestamp x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x,
                                  int length) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x,
                                   int length) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateCharacterStream(final int columnIndex, Reader x,
                                      int length) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateObject(final int columnIndex, final Object x,
                             final int scaleOrLength) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateObject(final int columnIndex, final Object x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateNull(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBoolean(final String columnLabel, final boolean x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateByte(final String columnLabel, final byte x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateShort(final String columnLabel, final short x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateInt(final String columnLabel, final int x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateLong(final String columnLabel, final long x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateFloat(final String columnLabel, final float x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateDouble(final String columnLabel, final double x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBigDecimal(final String columnLabel, final BigDecimal x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateString(final String columnLabel, final String x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBytes(final String columnLabel, final byte[] x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateDate(final String columnLabel, final Date x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateTime(final String columnLabel, final Time x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateTimestamp(final String columnLabel, final Timestamp x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateAsciiStream(final String columnLabel,
                                  final InputStream x, final int length) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBinaryStream(final String columnLabel,
                                   final InputStream x, final int length) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateCharacterStream(final String columnLabel,
                                      final Reader reader, final int length) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateObject(final String columnLabel, final Object x,
                             final int scaleOrLength) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateObject(final String columnLabel, final Object x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void insertRow() throws SQLException {
        checkClosed();

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateRow() throws SQLException {
        checkClosed();

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void deleteRow() throws SQLException {
        checkClosed();

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void refreshRow() throws SQLException {
        checkClosed();

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        checkClosed();

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        checkClosed();

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        checkClosed();

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(final int columnIndex,
                            final Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        return getObject(columnIndex);
    }

    @Override
    public Ref getRef(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public Blob getBlob(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public Clob getClob(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public Array getArray(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public Object getObject(final String columnLabel,
                            final Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getObject(getColumnIndex(columnLabel));
    }

    @Override
    public Ref getRef(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getRef(getColumnIndex(columnLabel));
    }

    @Override
    public Blob getBlob(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getBlob(getColumnIndex(columnLabel));
    }

    @Override
    public Clob getClob(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getClob(getColumnIndex(columnLabel));
    }

    @Override
    public Array getArray(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getArray(getColumnIndex(columnLabel));
    }

    @Override
    public Date getDate(final int columnIndex, final Calendar cal)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public Date getDate(final String columnLabel, final Calendar cal)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getDate(getColumnIndex(columnLabel), cal);
    }

    @Override
    public Time getTime(final int columnIndex, final Calendar cal)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public Time getTime(final String columnLabel, final Calendar cal)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getTime(getColumnIndex(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar cal)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel, final Calendar cal)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getTimestamp(getColumnIndex(columnLabel), cal);
    }

    @Override
    public URL getURL(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public URL getURL(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getURL(getColumnIndex(columnLabel));
    }

    @Override
    public void updateRef(final int columnIndex, final Ref x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateRef(final String columnLabel, final Ref x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBlob(final int columnIndex, final Blob x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBlob(final String columnLabel, final Blob x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateClob(final int columnIndex, final Clob x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateClob(final String columnLabel, final Clob x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateArray(final int columnIndex, final Array x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateArray(final String columnLabel, final Array x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public RowId getRowId(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7005);
    }

    @Override
    public RowId getRowId(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7005);
    }

    @Override
    public void updateRowId(final int columnIndex, final RowId x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateRowId(final String columnLabel, final RowId x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(final int columnIndex, final String nString)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateNString(final String columnLabel, String nString)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateNClob(final int columnIndex, final NClob nClob)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateNClob(final String columnLabel, NClob nClob)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public NClob getNClob(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public NClob getNClob(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getNClob(getColumnIndex(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public SQLXML getSQLXML(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getSQLXML(getColumnIndex(columnLabel));
    }

    @Override
    public void updateSQLXML(final int columnIndex, SQLXML xmlObject)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateSQLXML(final String columnLabel, SQLXML xmlObject)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public String getNString(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public String getNString(final String columnLabel) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getNString(getColumnIndex(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(final int columnIndex)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7999);
    }

    @Override
    public Reader getNCharacterStream(final String columnLabel)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        return getNCharacterStream(getColumnIndex(columnLabel));
    }

    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x,
                                       final long length) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateNCharacterStream(final String columnLabel,
                                       final Reader reader, final long length) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x,
                                  long length) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x,
                                   final long length) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x,
                                      final long length) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateAsciiStream(final String columnLabel,
                                  final InputStream x, final long length) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBinaryStream(final String columnLabel,
                                   final InputStream x, final long length) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateCharacterStream(final String columnLabel,
                                      final Reader reader, final long length) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBlob(final int columnIndex,
                           final InputStream inputStream, final long length)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBlob(final String columnLabel,
                           final InputStream inputStream, final long length)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateClob(final int columnIndex, final Reader reader,
                           final long length) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateClob(final String columnLabel, final Reader reader,
                           final long length) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateNClob(final int columnIndex, final Reader reader,
                            final long length) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateNClob(final String columnLabel, final Reader reader,
                            long length) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateNCharacterStream(final String columnLabel,
                                       final Reader reader) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateAsciiStream(final String columnLabel, final InputStream x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBinaryStream(final String columnLabel, final InputStream x)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateCharacterStream(final String columnLabel,
                                      final Reader reader) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBlob(final int columnIndex, final InputStream inputStream)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateBlob(final String columnLabel,
                           final InputStream inputStream) throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateClob(final int columnIndex, final Reader reader)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateClob(final String columnLabel, final Reader reader)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateNClob(final int columnIndex, final Reader reader)
            throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }

    @Override
    public void updateNClob(final String columnLabel, final Reader reader)
            throws SQLException {
        checkClosed();
        checkColumnLabel(columnLabel);

        throw TidaSqlExceptions.createNotSupportedException(7004);
    }
}
