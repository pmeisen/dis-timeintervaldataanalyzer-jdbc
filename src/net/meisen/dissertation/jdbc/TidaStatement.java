package net.meisen.dissertation.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
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
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The implementation of a {@code Statement} as well as an
 * {@code PreparedStatement}.
 * 
 * @author pmeisen
 * 
 */
public class TidaStatement extends BaseConnectionWrapper implements Statement,
		PreparedStatement {
	private final static Pattern placeholderPattern = Pattern
			.compile("'(?:(?:\\\\')|[^'])*'|(\\?)");

	/**
	 * The enumeration is used to determine the type of the current
	 * {@code ResultSet}. The {@code currentResultSet} could have been returned
	 * by the statement, therefore responsibility of closing is given to the
	 * using instance, or it could be used within the statement. In the later
	 * case the {@code Statement} has the responsibility to handle the
	 * {@code ResultSet}.
	 * 
	 * @author pmeisen
	 * 
	 */
	protected enum CurrentResultSetType {
		/**
		 * The {@code currentResultSet} of the {@code Statement} was passed to a
		 * calling instance. Responsibility is therefore with the calling
		 * instance.
		 */
		RETURNED_AS_RESULT,
		/**
		 * The {@code currentResultSet} is used internally by the
		 * {@code Statement}. Responsibility of closing is thereby with the
		 * {@code Statement}.
		 */
		USED_BY_STATEMENT;
	}

	/**
	 * Helper class which defines a place-holder within a {@code Statement}.
	 * 
	 * @author pmeisen
	 * 
	 */
	public final static class Placeholder {
		private final static DecimalFormat decFormatter;
		private final static SimpleDateFormat dateFormatter;

		static {

			// set the dateFormatter
			dateFormatter = new SimpleDateFormat();
			dateFormatter.applyPattern("dd.MM.yyyy HH:mm:ss");
			dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

			// set the decimalFormatter
			final DecimalFormatSymbols otherSymbols = new DecimalFormatSymbols(
					Locale.US);
			otherSymbols.setDecimalSeparator('.');
			otherSymbols.setGroupingSeparator(',');

			decFormatter = new DecimalFormat("##############0.###############");
			decFormatter.setGroupingUsed(false);
		}

		private final int pos;
		private Object value;

		/**
		 * Default constructor which just specifies the position of the
		 * place-holder, the value is set to {@code null}.
		 * 
		 * @param pos
		 *            the position of the place-holder
		 */
		public Placeholder(final int pos) {
			this(pos, null);
		}

		/**
		 * Constructor to specify the position and the value of the
		 * place-holder.
		 * 
		 * @param pos
		 *            the position of the place-holder
		 * @param value
		 *            the value of the place-holder
		 */
		public Placeholder(final int pos, final Object value) {
			this.pos = pos;
			this.set(value);
		}

		/**
		 * Gets the current value of the place-holder.
		 * 
		 * @return the current value of the place-holder
		 */
		public Object get() {
			return value;
		}

		/**
		 * Sets the value of the place-holder.
		 * 
		 * @param value
		 *            the value of the place-holder
		 */
		public void set(final Object value) {
			this.value = value;
		}

		/**
		 * Gets the position of place-holder within the statement.
		 * 
		 * @return the position of place-holder within the statement
		 */
		public int getPos() {
			return pos;
		}

		/**
		 * Gets the value of {@code this} as formatted value.
		 * 
		 * @return the formatted value
		 */
		public Object getFormattedValue() {

			if (value == null) {
				return "NULL";
			} else if (value instanceof Byte) {
				return "'" + Byte.toString((Byte) value) + "'";
			} else if (value instanceof Short) {
				return "'" + Short.toString((Short) value) + "'";
			} else if (value instanceof Integer) {
				return "'" + Integer.toString((Integer) value) + "'";
			} else if (value instanceof Long) {
				return "'" + Long.toString((Long) value) + "'";
			} else if (value instanceof Float) {
				return "'" + Float.toString((Float) value) + "'";
			} else if (value instanceof Double) {
				return "'" + Double.toString((Double) value) + "'";
			} else if (value instanceof BigDecimal) {
				return "'" + value.toString() + "'";
			} else if (value instanceof Number) {
				return "'" + decFormatter.format((Number) value) + "'";
			} else if (value instanceof java.util.Date) {
				return "'" + dateFormatter.format((java.util.Date) value) + "'";
			} else {
				return "'" + value.toString() + "'";
			}
		}

		@Override
		public String toString() {
			return "pos: " + pos + " with value " + getFormattedValue();
		}
	}

	private final String sql;
	private final List<Placeholder> placeholders;
	private final ExecutorService executor;

	private final int resultSetType;
	private final int resultSetConcurrency;
	private final int resultSetHoldability;
	private final int autoGeneratedKeys;
	private final int[] columnIndexes;
	private final String[] columnNames;

	private List<String> batch;
	private int queryTimeoutInMs;
	private TidaResultSet currentResultSet;
	private CurrentResultSetType currentResultSetType;

	/**
	 * Creating a {@code Statement} for the specified {@code connection} and the
	 * specified {@code sql}. The {@code Statement} uses the default settings,
	 * i.e. {@link ResultSet#TYPE_FORWARD_ONLY},
	 * {@link ResultSet#CONCUR_READ_ONLY},
	 * {@link ResultSet#CLOSE_CURSORS_AT_COMMIT}, and
	 * {@link Statement#NO_GENERATED_KEYS}.
	 * 
	 * @param connection
	 *            the connection used to fire the statement
	 * @param sql
	 *            the statement to be fired
	 * 
	 * @throws SQLException
	 *             if the statement cannot be created
	 */
	public TidaStatement(final TidaConnection connection, final String sql)
			throws SQLException {
		this(connection, sql, ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT,
				Statement.NO_GENERATED_KEYS);
	}

	/**
	 * Creating a {@code Statement} for the specified {@code connection} and the
	 * specified {@code sql}. The {@code Statement} will use the specified
	 * {@code resultSetType}, {@code resultSetConcurrency},
	 * {@code resultSetHoldability} and the {@code autoGeneratedKeys}.
	 * 
	 * @param connection
	 *            the connection used to fire the statement
	 * @param sql
	 *            the statement to be fired
	 * @param resultSetType
	 *            the type of the {@code ResultSet}, supports only
	 *            {@link ResultSet#TYPE_FORWARD_ONLY}
	 * @param resultSetConcurrency
	 *            the concurrency handling, supports only
	 *            {@link ResultSet#CONCUR_READ_ONLY}
	 * @param resultSetHoldability
	 *            the holdability, supports only
	 *            {@link ResultSet#CLOSE_CURSORS_AT_COMMIT} and
	 *            {@link ResultSet#HOLD_CURSORS_OVER_COMMIT}
	 * @param columnIndexes
	 *            the indexes of the columns to retrieve the key from, should be
	 *            empty or {@code null}
	 * 
	 * @throws SQLException
	 *             if the statement cannot be created
	 */
	public TidaStatement(final TidaConnection connection, final String sql,
			final int resultSetType, final int resultSetConcurrency,
			final int resultSetHoldability, final int[] columnIndexes)
			throws SQLException {
		this(connection, sql, resultSetType, resultSetConcurrency,
				resultSetHoldability, Statement.RETURN_GENERATED_KEYS);
	}

	/**
	 * Creating a {@code Statement} for the specified {@code connection} and the
	 * specified {@code sql}. The {@code Statement} will use the specified
	 * {@code resultSetType}, {@code resultSetConcurrency},
	 * {@code resultSetHoldability} and the {@code autoGeneratedKeys}.
	 * 
	 * @param connection
	 *            the connection used to fire the statement
	 * @param sql
	 *            the statement to be fired
	 * @param resultSetType
	 *            the type of the {@code ResultSet}, supports only
	 *            {@link ResultSet#TYPE_FORWARD_ONLY}
	 * @param resultSetConcurrency
	 *            the concurrency handling, supports only
	 *            {@link ResultSet#CONCUR_READ_ONLY}
	 * @param resultSetHoldability
	 *            the holdability, supports only
	 *            {@link ResultSet#CLOSE_CURSORS_AT_COMMIT} and
	 *            {@link ResultSet#HOLD_CURSORS_OVER_COMMIT}
	 * @param columnNames
	 *            the names of the columns to retrieve the key from, should be
	 *            empty or {@code null}
	 * 
	 * @throws SQLException
	 *             if the statement cannot be created
	 */
	public TidaStatement(final TidaConnection connection, final String sql,
			final int resultSetType, final int resultSetConcurrency,
			final int resultSetHoldability, final String[] columnNames)
			throws SQLException {
		this(connection, sql, resultSetType, resultSetConcurrency,
				resultSetHoldability, Statement.RETURN_GENERATED_KEYS);
	}

	/**
	 * Creating a {@code Statement} for the specified {@code connection} and the
	 * specified {@code sql}. The {@code Statement} will use the specified
	 * {@code resultSetType}, {@code resultSetConcurrency},
	 * {@code resultSetHoldability} and the {@code autoGeneratedKeys}.
	 * 
	 * @param connection
	 *            the connection used to fire the statement
	 * @param sql
	 *            the statement to be fired
	 * @param resultSetType
	 *            the type of the {@code ResultSet}, supports only
	 *            {@link ResultSet#TYPE_FORWARD_ONLY}
	 * @param resultSetConcurrency
	 *            the concurrency handling, supports only
	 *            {@link ResultSet#CONCUR_READ_ONLY}
	 * @param resultSetHoldability
	 *            the holdability, supports only
	 *            {@link ResultSet#CLOSE_CURSORS_AT_COMMIT} and
	 *            {@link ResultSet#HOLD_CURSORS_OVER_COMMIT}
	 * @param autoGeneratedKeys
	 *            the handling of generated keys, supports only
	 *            {@link Statement#NO_GENERATED_KEYS} and
	 *            {@link Statement#RETURN_GENERATED_KEYS}
	 * 
	 * @throws SQLException
	 *             if the statement cannot be created
	 */
	public TidaStatement(final TidaConnection connection, final String sql,
			final int resultSetType, final int resultSetConcurrency,
			final int resultSetHoldability, final int autoGeneratedKeys)
			throws SQLException {
		this(connection, sql, resultSetType, resultSetConcurrency,
				resultSetHoldability, autoGeneratedKeys, null, null);
	}

	/**
	 * Creating a {@code Statement} for the specified {@code connection} and the
	 * specified {@code sql}. The {@code Statement} will use the specified
	 * {@code resultSetType}, {@code resultSetConcurrency},
	 * {@code resultSetHoldability} and the {@code autoGeneratedKeys}.
	 * 
	 * @param connection
	 *            the connection used to fire the statement
	 * @param sql
	 *            the statement to be fired
	 * @param resultSetType
	 *            the type of the {@code ResultSet}, supports only
	 *            {@link ResultSet#TYPE_FORWARD_ONLY}
	 * @param resultSetConcurrency
	 *            the concurrency handling, supports only
	 *            {@link ResultSet#CONCUR_READ_ONLY}
	 * @param resultSetHoldability
	 *            the holdability, supports only
	 *            {@link ResultSet#CLOSE_CURSORS_AT_COMMIT} and
	 *            {@link ResultSet#HOLD_CURSORS_OVER_COMMIT}
	 * @param autoGeneratedKeys
	 *            the handling of generated keys, supports only
	 *            {@link Statement#NO_GENERATED_KEYS} and
	 *            {@link Statement#RETURN_GENERATED_KEYS}
	 * @param columnIndexes
	 *            the indexes of the columns to retrieve the key from, should be
	 *            empty or {@code null}
	 * @param columnNames
	 *            the names of the columns to retrieve the key from, should be
	 *            empty or {@code null}
	 * 
	 * @throws SQLException
	 *             if the statement cannot be created
	 */
	public TidaStatement(final TidaConnection connection, final String sql,
			final int resultSetType, final int resultSetConcurrency,
			final int resultSetHoldability, final int autoGeneratedKeys,
			final int[] columnIndexes, final String[] columnNames)
			throws SQLException {
		super(connection);

		// set defaults
		this.batch = new ArrayList<String>();
		this.queryTimeoutInMs = 0;
		this.currentResultSet = null;
		this.currentResultSetType = null;

		// set specified stuff
		this.resultSetType = resultSetType;
		this.resultSetConcurrency = resultSetConcurrency;
		this.resultSetHoldability = resultSetHoldability;
		this.autoGeneratedKeys = autoGeneratedKeys;
		this.columnIndexes = columnIndexes;
		this.columnNames = columnNames;

		this.sql = sql;
		this.executor = Executors.newFixedThreadPool(1);

		// check for place-holders
		this.placeholders = retrievePlaceholders();
	}

	/**
	 * Gets all the defined place-holders.
	 * 
	 * @return all the defined place-holders
	 */
	public List<Placeholder> getPlaceholders() {
		return Collections.unmodifiableList(placeholders);
	}

	/**
	 * Determines the place-holders defined within the {@code sql} of
	 * {@code this}.
	 * 
	 * @return the found place-holders
	 */
	protected List<Placeholder> retrievePlaceholders() {

		// check if we have any empty SQL, if so there are no placeholders
		if (sql == null || "".equals(sql.trim())) {
			return new ArrayList<Placeholder>(0);
		}

		// get all the placeholders
		final Matcher m = placeholderPattern.matcher(sql);
		final List<Placeholder> placeholders = new ArrayList<Placeholder>();
		while (m.find()) {
			if (m.group(1) != null) {
				final Placeholder ph = new Placeholder(m.start(1));
				placeholders.add(ph);
			}
		}

		return placeholders;
	}

	/**
	 * Replaces the place-holders within the {@code sql} according to the found
	 * place-holders. The sql-statement must be equal to the one, used to
	 * determine the place-holders, i.e. {@link #retrievePlaceholders()}.
	 * 
	 * @return the statement with replace place-holders
	 */
	protected String replacePlaceholder() {
		if (placeholders == null || placeholders.size() == 0) {
			return sql;
		} else {
			final StringBuilder sb = new StringBuilder();

			final ListIterator<Placeholder> li = placeholders
					.listIterator(placeholders.size());

			int curPos = sql.length();
			while (li.hasPrevious()) {
				final Placeholder ph = li.previous();

				sb.insert(0, sql.substring(ph.getPos() + 1, curPos));
				sb.insert(0, ph.getFormattedValue());

				curPos = ph.getPos();
			}

			sb.insert(0, sql.substring(0, curPos));

			return sb.toString();
		}
	}

	@Override
	protected BaseConnectionWrapper getProtocolScope() {
		return this;
	}

	@Override
	protected boolean doCloseOnCommit() {
		return false;
	}

	@Override
	public void close() throws SQLException {
		if (isClosed()) {
			return;
		}

		// close any currentResultSet
		if (currentResultSet != null
				&& CurrentResultSetType.USED_BY_STATEMENT
						.equals(currentResultSetType)) {
			currentResultSet.close();
		}

		// close the rest
		super.close();
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
			throw TidaSqlExceptions.createException(3999);
		}
	}

	/**
	 * Executes the statement specified by the {@code sql}. Any
	 * {@code currentResultSet} will be closed if the
	 * {@code currentResultSetType} of {@code this} is different from
	 * {@link CurrentResultSetType#RETURNED_AS_RESULT}.<br/>
	 * <br/>
	 * The method ensures that the {@code TidaResultSetType} of the returning
	 * {@code ResultSet} is of the type specified by {@code type}. If the types
	 * do not match an exception is thrown. If {@code type} is equal to
	 * {@link TidaResultSetType#UNKNOWN}, every type is allowed to be returned.<br/>
	 * <br/>
	 * Furthermore, the method allows to specify the {@code columnIndexes} or
	 * {@code columnNames} to retrieve keys from. Nevertheless, the key is
	 * typically not available in any column, therefore if the generated keys
	 * should be returned it should just be done using
	 * {@link Statement#RETURN_GENERATED_KEYS} and passing {@code null} or an
	 * empty array for {@code columnIndexes} or/and {@code columnNames}.
	 * 
	 * @param sql
	 *            the statement to be fired
	 * @param type
	 *            the expected type of the {@code TidaResultSet}, can be
	 *            {@link TidaResultSetType#UNKNOWN} if nothing specific is
	 *            expected
	 * @param currentResultSetType
	 *            the type of the usage of the created {@code TidaResultSet}
	 * @param autoGeneratedKeys
	 *            one of {@link Statement#RETURN_GENERATED_KEYS} or
	 *            {@link Statement#NO_GENERATED_KEYS}
	 * @param columnIndexes
	 *            the indexes of the columns to retrieve the key from, should be
	 *            empty or {@code null}
	 * @param columnNames
	 *            the names of the columns to retrieve the key from, should be
	 *            empty or {@code null}
	 * 
	 * @return the created instance
	 * 
	 * @throws SQLException
	 *             if the {@code Statement} is closed, if the resulting type of
	 *             the {@code ResultSet} does not match the expectation, or if
	 *             any unsupported setting is used
	 */
	protected TidaResultSet execute(final String sql,
			final TidaResultSetType type,
			final CurrentResultSetType currentResultSetType,
			final int autoGeneratedKeys, final int[] columnIndexes,
			final String[] columnNames) throws SQLException {
		checkClosed();

		if (currentResultSet != null
				&& CurrentResultSetType.USED_BY_STATEMENT
						.equals(this.currentResultSetType)) {

			/*
			 * Close any currentResultSet, because it cannot be used anymore.
			 * The currentResultSet is only kept if the information was needed
			 * afterwards.
			 */
			currentResultSet.close();
			currentResultSet = null;
		}

		// get the sql to be used
		final String query = sql == null ? replacePlaceholder() : sql;

		// create a thread to fire a query
		final Callable<TidaResultSet> executeTask = new Callable<TidaResultSet>() {

			@Override
			public TidaResultSet call() throws SQLException {
				return new TidaResultSet(TidaStatement.this, query, type,
						getResultSetType(), getResultSetConcurrency(),
						getResultSetHoldability(), autoGeneratedKeys,
						columnIndexes, columnNames);
			}
		};

		// run the query
		final Future<TidaResultSet> future = executor.submit(executeTask);
		final TidaResultSet resultSet;
		try {
			if (queryTimeoutInMs == 0) {
				resultSet = future.get();
			} else {
				resultSet = future.get(queryTimeoutInMs, TimeUnit.MILLISECONDS);
			}
		} catch (final InterruptedException e) {
			throw TidaSqlExceptions
					.createException(3006, query, e.getMessage());
		} catch (final ExecutionException e) {
			if (e.getCause() instanceof SQLException) {
				throw (SQLException) e.getCause();
			} else {
				throw (RuntimeException) e.getCause();
			}
		} catch (final TimeoutException e) {
			future.cancel(true);
			throw TidaSqlExceptions.createException(3005, query, ""
					+ getQueryTimeout());
		}

		// if the generated keys are needed than keep those
		this.currentResultSet = resultSet;
		this.currentResultSetType = currentResultSetType;

		return resultSet;
	}

	@Override
	public TidaResultSet executeQuery() throws SQLException {
		return executeQuery(null);
	}

	@Override
	public TidaResultSet executeQuery(final String sql) throws SQLException {
		return executeQuery(sql, autoGeneratedKeys, columnIndexes, columnNames);
	}

	/**
	 * Executes the specified {@code sql} statement, which has to be of the type
	 * {@link TidaResultSetType#QUERY}.
	 * 
	 * @param sql
	 *            the statement to be executed
	 * @param autoGeneratedKeys
	 *            definition of auto-generated keys should be returned, see
	 *            {@link #TidaStatement(TidaConnection, String, int, int, int, int)}
	 *            for supported settings
	 * @param columnIndexes
	 *            the column indexes used to retrieve the key, should be empty
	 *            or {@code null}
	 * @param columnNames
	 *            the column names used to retrieve the key, should be empty or
	 *            {@code null}
	 * 
	 * @return the {@code ResultSet}
	 * 
	 * @throws SQLException
	 *             if the result cannot be retrieved
	 */
	protected TidaResultSet executeQuery(final String sql,
			final int autoGeneratedKeys, final int[] columnIndexes,
			final String[] columnNames) throws SQLException {
		final TidaResultSet resultSet = execute(sql, TidaResultSetType.QUERY,
				CurrentResultSetType.RETURNED_AS_RESULT, autoGeneratedKeys,
				columnIndexes, columnNames);
		return resultSet;
	}

	@Override
	public boolean execute() throws SQLException {
		return execute(null);
	}

	@Override
	public boolean execute(final String sql) throws SQLException {
		return execute(sql, autoGeneratedKeys, null, null);
	}

	@Override
	public boolean execute(final String sql, final int autoGeneratedKeys)
			throws SQLException {
		return execute(sql, autoGeneratedKeys, null, null);
	}

	@Override
	public boolean execute(final String sql, final int[] columnIndexes)
			throws SQLException {
		return execute(sql, Statement.RETURN_GENERATED_KEYS, columnIndexes,
				null);
	}

	@Override
	public boolean execute(final String sql, final String[] columnNames)
			throws SQLException {
		return execute(sql, Statement.RETURN_GENERATED_KEYS, null, columnNames);
	}

	/**
	 * Executes the specified {@code sql} statement, which has to be of the type
	 * {@link TidaResultSetType#QUERY} or {@link TidaResultSetType#MODIFY}.
	 * 
	 * @param sql
	 *            the statement to be executed
	 * @param autoGeneratedKeys
	 *            definition of auto-generated keys should be returned, see
	 *            {@link #TidaStatement(TidaConnection, String, int, int, int, int)}
	 *            for supported settings
	 * @param columnIndexes
	 *            the column indexes used to retrieve the key, should be empty
	 *            or {@code null}
	 * @param columnNames
	 *            the column names used to retrieve the key, should be empty or
	 *            {@code null}
	 * 
	 * @return {@code true} if it's a query statement, otherwise {@code false}
	 *         (i.e. it's a modify statement)
	 * 
	 * @throws SQLException
	 *             if the result cannot be retrieved
	 */
	protected boolean execute(final String sql, final int autoGeneratedKeys,
			final int[] columnIndexes, final String[] columnNames)
			throws SQLException {
		final TidaResultSet resultSet = execute(sql, TidaResultSetType.UNKNOWN,
				CurrentResultSetType.USED_BY_STATEMENT, autoGeneratedKeys,
				columnIndexes, columnNames);

		// keep the currentResultSet
		return TidaResultSetType.QUERY.equals(resultSet.getResultSetType());
	}

	@Override
	public int executeUpdate() throws SQLException {
		return executeUpdate(null);
	}

	@Override
	public int executeUpdate(final String sql) throws SQLException {
		return executeUpdate(sql, autoGeneratedKeys, columnIndexes, columnNames);
	}

	@Override
	public int executeUpdate(final String sql, final int autoGeneratedKeys)
			throws SQLException {
		return executeUpdate(sql, autoGeneratedKeys, null, null);
	}

	@Override
	public int executeUpdate(final String sql, final int[] columnIndexes)
			throws SQLException {
		return executeUpdate(sql, Statement.RETURN_GENERATED_KEYS,
				columnIndexes, null);
	}

	@Override
	public int executeUpdate(final String sql, final String[] columnNames)
			throws SQLException {
		return executeUpdate(sql, Statement.RETURN_GENERATED_KEYS, null,
				columnNames);
	}

	/**
	 * Executes the specified {@code sql} statement, which has to be of the type
	 * {@link TidaResultSetType#MODIFY}.
	 * 
	 * @param sql
	 *            the statement to be executed
	 * @param autoGeneratedKeys
	 *            definition of auto-generated keys should be returned, see
	 *            {@link #TidaStatement(TidaConnection, String, int, int, int, int)}
	 *            for supported settings
	 * @param columnIndexes
	 *            the column indexes used to retrieve the key, should be empty
	 *            or {@code null}
	 * @param columnNames
	 *            the column names used to retrieve the key, should be empty or
	 *            {@code null}
	 * 
	 * @return typically the amount of records modified, but might have a
	 *         different meaning for the specified {@code sql} statement
	 * 
	 * @throws SQLException
	 *             if the result cannot be retrieved
	 */
	protected int executeUpdate(final String sql, final int autoGeneratedKeys,
			final int[] columnIndexes, final String[] columnNames)
			throws SQLException {
		final TidaResultSet resultSet = execute(sql, TidaResultSetType.MODIFY,
				CurrentResultSetType.USED_BY_STATEMENT, autoGeneratedKeys,
				columnIndexes, columnNames);

		return resultSet.getCountValue();
	}

	@Override
	public int[] executeBatch() throws SQLException {
		checkClosed();

		final int[] res = new int[batch.size()];
		for (int i = 0; i < batch.size(); i++) {
			res[i] = executeUpdate(sql);
		}

		return res;
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		return 0;
	}

	@Override
	public void setMaxFieldSize(final int max) throws SQLException {
		// ignored
	}

	@Override
	public int getMaxRows() throws SQLException {
		return Integer.MAX_VALUE;
	}

	@Override
	public void setMaxRows(final int max) throws SQLException {
		// ignored
	}

	@Override
	public void setEscapeProcessing(final boolean enable) throws SQLException {
		// ignored
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		checkClosed();

		return queryTimeoutInMs / 1000;
	}

	/**
	 * Sets the time-out in milliseconds.
	 * 
	 * @param milliseconds
	 *            the time-out value to be used
	 * 
	 * @throws SQLException
	 *             if the statement is closed
	 */
	public void setQueryTimeoutInMs(final int milliseconds) throws SQLException {
		if (milliseconds < 0) {
			throw TidaSqlExceptions.createException(3004, "" + milliseconds);
		}

		this.queryTimeoutInMs = milliseconds;
	}

	@Override
	public void setQueryTimeout(final int seconds) throws SQLException {
		setQueryTimeoutInMs(seconds * 1000);
	}

	@Override
	public void cancel() throws SQLException {
		executor.shutdownNow();
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
	public void setCursorName(final String name) throws SQLException {
		// no-op method
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		checkClosed();

		if (this.currentResultSet == null) {
			return null;
		} else if (!TidaResultSetType.QUERY.equals(currentResultSet
				.getResultSetType())) {
			return null;
		} else if (CurrentResultSetType.RETURNED_AS_RESULT
				.equals(currentResultSetType)) {
			return null;
		} else {
			this.currentResultSetType = CurrentResultSetType.RETURNED_AS_RESULT;

			// make sure it cannot be retrieved a second time
			return currentResultSet;
		}
	}

	@Override
	public int getUpdateCount() throws SQLException {
		checkClosed();

		if (currentResultSet == null) {
			return -1;
		} else if (!TidaResultSetType.MODIFY.equals(currentResultSet
				.getResultSetType())) {
			return -1;
		} else {
			final Integer count = currentResultSet.getCountValue();

			// check if we have a count
			if (count == null) {
				return -1;
			} else {
				return count;
			}
		}
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return false;
	}

	@Override
	public void setFetchDirection(final int direction) throws SQLException {
		checkClosed();

		if (direction != ResultSet.FETCH_FORWARD) {
			throw TidaSqlExceptions.createNotSupportedException(3000, ""
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
					.createNotSupportedException(3001, "" + rows);
		} else if (rows < 0) {
			throw TidaSqlExceptions.createException(3002, "" + rows);
		}
	}

	@Override
	public int getFetchSize() throws SQLException {
		checkClosed();

		return 1;
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return resultSetConcurrency;
	}

	@Override
	public int getResultSetType() throws SQLException {
		return resultSetType;
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return resultSetHoldability;
	}

	@Override
	public void addBatch(final String sql) throws SQLException {
		batch.add(sql);
	}

	@Override
	public void clearBatch() throws SQLException {
		batch.clear();
	}

	@Override
	public Connection getConnection() throws SQLException {
		return (Connection) getParent();
	}

	@Override
	public boolean getMoreResults(final int current) throws SQLException {
		return false;
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		if (currentResultSet == null) {
			return null;
		} else {
			return new IntResultSet(currentResultSet.getGeneratedKeys(), this);
		}
	}

	@Override
	public void setPoolable(final boolean poolable) throws SQLException {
		if (poolable) {
			throw TidaSqlExceptions.createNotSupportedException(3003);
		}
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return false;
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

	@Override
	public void setNull(final int parameterIndex, final int sqlType)
			throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(null);
	}

	@Override
	public void setBoolean(final int parameterIndex, final boolean x)
			throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(x);
	}

	@Override
	public void setByte(final int parameterIndex, final byte x)
			throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(x);
	}

	@Override
	public void setShort(final int parameterIndex, final short x)
			throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(x);
	}

	@Override
	public void setInt(final int parameterIndex, final int x)
			throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(x);
	}

	@Override
	public void setLong(final int parameterIndex, final long x)
			throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(x);
	}

	@Override
	public void setFloat(final int parameterIndex, final float x)
			throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(x);
	}

	@Override
	public void setDouble(final int parameterIndex, final double x)
			throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(x);
	}

	@Override
	public void setBigDecimal(final int parameterIndex, final BigDecimal x)
			throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(x);
	}

	@Override
	public void setString(final int parameterIndex, final String x)
			throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(x);
	}

	@Override
	public void setBytes(final int parameterIndex, final byte[] x)
			throws SQLException {
		throw TidaSqlExceptions
				.createNotSupportedException(3008, "byte-arrays");
	}

	/**
	 * Sets the date {@code x} at the specified {@code parameterIndex} with the
	 * specified {@code cal}. The {@code cal} can be {@code null} if the default
	 * calendar should be used.
	 * 
	 * @param parameterIndex
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            the date value
	 * @param cal
	 *            the calendar of the date
	 * 
	 * @throws SQLException
	 *             if parameterIndex does not correspond to a parameter marker
	 *             in the SQL statement; if a database access error occurs or
	 *             this method is called on a closed PreparedStatement
	 */
	protected void _setDate(final int parameterIndex, final java.util.Date x,
			final Calendar cal) throws SQLException {
		final int pos = checkParameter(parameterIndex);

		// get the date's timeZone
		final TimeZone srcTz = cal == null ? TimeZone.getDefault() : cal
				.getTimeZone();

		final TimeZone trgtTz = TimeZone.getTimeZone("UTC");

		// create a formatter for the date
		final DateFormat formatter = new SimpleDateFormat(
				"dd.MM.yyyy HH:mm:ss,SSS");

		// set the source timeZone and format the date
		formatter.setTimeZone(srcTz);
		final String strX = formatter.format(x);

		// set the target timeZone and parse the date
		formatter.setTimeZone(trgtTz);

		final TimeZone defTz = TimeZone.getDefault();
		try {
			TimeZone.setDefault(trgtTz);
			final java.util.Date parsedDate = formatter.parse(strX);
			placeholders.get(pos).set(parsedDate);
		} catch (final ParseException e) {
			placeholders.get(pos).set(null);
			throw TidaSqlExceptions.createException(3009, strX);
		} finally {
			TimeZone.setDefault(defTz);
		}
	}

	@Override
	public void setDate(final int parameterIndex, final Date x)
			throws SQLException {
		_setDate(parameterIndex, (java.util.Date) x, null);
	}

	@Override
	public void setTime(final int parameterIndex, final Time x)
			throws SQLException {
		_setDate(parameterIndex, (java.util.Date) x, null);
	}

	@Override
	public void setTimestamp(final int parameterIndex, final Timestamp x)
			throws SQLException {
		_setDate(parameterIndex, (java.util.Date) x, null);
	}

	@Override
	public void setAsciiStream(final int parameterIndex, final InputStream x,
			final int length) throws SQLException {
		throw TidaSqlExceptions
				.createNotSupportedException(3008, "AsciiStream");
	}

	@Override
	@Deprecated
	public void setUnicodeStream(final int parameterIndex, final InputStream x,
			final int length) throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008,
				"UnicodeStream");
	}

	@Override
	public void setBinaryStream(final int parameterIndex, final InputStream x,
			final int length) throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008,
				"BinaryStream");
	}

	@Override
	public void clearParameters() throws SQLException {
		if (placeholders == null) {
			return;
		}

		// set all the place-holders
		for (final Placeholder ph : placeholders) {
			ph.set(null);
		}
	}

	@Override
	public void setObject(final int parameterIndex, final Object x,
			final int targetSqlType) throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(x);
	}

	@Override
	public void setObject(final int parameterIndex, final Object x)
			throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(x);
	}

	@Override
	public void addBatch() throws SQLException {
		// ignore
	}

	@Override
	public void setCharacterStream(final int parameterIndex,
			final Reader reader, final int length) throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008,
				"CharacterStream");
	}

	@Override
	public void setRef(final int parameterIndex, final Ref x)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008,
				Ref.class.getName());
	}

	@Override
	public void setBlob(final int parameterIndex, final Blob x)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008,
				Blob.class.getName());
	}

	@Override
	public void setClob(final int parameterIndex, final Clob x)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008,
				Clob.class.getName());
	}

	@Override
	public void setArray(final int parameterIndex, final Array x)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008,
				Array.class.getName());
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		if (currentResultSet == null) {
			if (sql == null) {
				return null;
			} else {
				execute(sql, Statement.NO_GENERATED_KEYS);
				return currentResultSet.getMetaData();
			}
		} else {
			return currentResultSet.getMetaData();
		}
	}

	@Override
	public void setDate(final int parameterIndex, final Date x,
			final Calendar cal) throws SQLException {
		_setDate(parameterIndex, (java.util.Date) x, cal);
	}

	@Override
	public void setTime(final int parameterIndex, final Time x,
			final Calendar cal) throws SQLException {
		_setDate(parameterIndex, (java.util.Date) x, cal);
	}

	@Override
	public void setTimestamp(final int parameterIndex, final Timestamp x,
			final Calendar cal) throws SQLException {
		_setDate(parameterIndex, (java.util.Date) x, cal);
	}

	@Override
	public void setNull(final int parameterIndex, final int sqlType,
			final String typeName) throws SQLException {
		setNull(parameterIndex, sqlType);
	}

	@Override
	public void setURL(final int parameterIndex, final URL x)
			throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(x);
	}

	@Override
	public ParameterMetaData getParameterMetaData() throws SQLException {
		return new TidaParameterMetaData(placeholders);
	}

	@Override
	public void setRowId(final int parameterIndex, final RowId x)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008, "RowId");
	}

	@Override
	public void setNString(final int parameterIndex, final String value)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008, "NString");
	}

	@Override
	public void setNCharacterStream(final int parameterIndex,
			final Reader value, final long length) throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008,
				"NCharacterStream");
	}

	@Override
	public void setNClob(final int parameterIndex, final NClob value)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008, "NClob");
	}

	@Override
	public void setClob(final int parameterIndex, final Reader reader,
			final long length) throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008, "Clob");
	}

	@Override
	public void setBlob(final int parameterIndex,
			final InputStream inputStream, final long length)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008, "Blob");
	}

	@Override
	public void setNClob(final int parameterIndex, final Reader reader,
			final long length) throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008, "NClob");
	}

	@Override
	public void setSQLXML(final int parameterIndex, final SQLXML xmlObject)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008, "SQLXML");
	}

	@Override
	public void setObject(final int parameterIndex, final Object x,
			final int targetSqlType, final int scaleOrLength)
			throws SQLException {
		final int pos = checkParameter(parameterIndex);
		placeholders.get(pos).set(x);
	}

	@Override
	public void setAsciiStream(final int parameterIndex, final InputStream x,
			final long length) throws SQLException {
		throw TidaSqlExceptions
				.createNotSupportedException(3008, "AsciiStream");
	}

	@Override
	public void setBinaryStream(final int parameterIndex, final InputStream x,
			final long length) throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008,
				"BinaryStream");
	}

	@Override
	public void setCharacterStream(final int parameterIndex,
			final Reader reader, final long length) throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008,
				"CharacterStream");
	}

	@Override
	public void setAsciiStream(final int parameterIndex, final InputStream x)
			throws SQLException {
		throw TidaSqlExceptions
				.createNotSupportedException(3008, "AsciiStream");
	}

	@Override
	public void setBinaryStream(final int parameterIndex, final InputStream x)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008,
				"BinaryStream");
	}

	@Override
	public void setCharacterStream(final int parameterIndex, final Reader reader)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008,
				"CharacterStream");
	}

	@Override
	public void setNCharacterStream(final int parameterIndex, final Reader value)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008,
				"NCharacterStream");
	}

	@Override
	public void setClob(final int parameterIndex, final Reader reader)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008, "Clob");
	}

	@Override
	public void setBlob(final int parameterIndex, final InputStream inputStream)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008, "Blob");
	}

	@Override
	public void setNClob(final int parameterIndex, final Reader reader)
			throws SQLException {
		throw TidaSqlExceptions.createNotSupportedException(3008, "NClob");
	}

	@Override
	public String toString() {
		return "[" + getClass().getSimpleName() + "] " + sql;
	}
}
