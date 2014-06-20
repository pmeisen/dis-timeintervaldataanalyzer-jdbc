package net.meisen.dissertation.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import net.meisen.dissertation.jdbc.protocol.DataType;
import net.meisen.dissertation.jdbc.protocol.IResponseHandler;
import net.meisen.dissertation.jdbc.protocol.QueryStatus;
import net.meisen.dissertation.jdbc.protocol.QueryType;
import net.meisen.dissertation.jdbc.protocol.ResponseType;

/**
 * A {@code ResponseHandler} used by default by the driver to handle any
 * responses from the server. The implementation, i.e. handler to be used, can
 * be configured by the driver's property (see
 * {@link DriverProperties#PROPERTY_HANDLERCLASS}).
 * 
 * @author pmeisen
 * 
 */
public class QueryResponseHandler implements IResponseHandler {
	/**
	 * The prefix used to identify a resource which should be read from the
	 * class-path.
	 */
	public final static String PREFIX_CLASSPATH = "classpath";
	/**
	 * The prefix used to identify a resource which should be read from the
	 * file-system.
	 */
	public final static String PREFIX_FILE = "file";

	private boolean eor = false;

	private TidaResultSetType expectedResultSetType;
	private TidaResultSetType resultSetType;
	private QueryStatus queryStatus;

	private DataType[] header;
	private String[] headerNames;

	private Integer[] generatedIds;
	private Integer countValue;

	private Object[] lastResult;

	/**
	 * Default constructor, which resets the handler.
	 */
	public QueryResponseHandler() {
		resetHandler();
	}

	@Override
	public QueryStatus doHandleQueryType(final QueryType queryType) {

		// determine the type of the resultSet
		if (QueryType.MANIPULATION.equals(queryType)) {
			resultSetType = TidaResultSetType.MODIFY;
		} else if (QueryType.QUERY.equals(queryType)) {
			resultSetType = TidaResultSetType.QUERY;
		} else {
			throw new IllegalArgumentException("The queryType '" + queryType
					+ "' is not supported.");
		}

		// determine if the type should be handled
		final QueryStatus status;
		if (TidaResultSetType.UNKNOWN.equals(expectedResultSetType)) {
			if (QueryType.QUERY.equals(queryType)
					&& QueryStatus.PROCESSANDGETIDS.equals(queryStatus)) {
				status = QueryStatus.CANCEL;
			} else {
				status = queryStatus;
			}
		} else if (TidaResultSetType.MODIFY.equals(expectedResultSetType)) {
			if (QueryType.MANIPULATION.equals(queryType)) {
				status = queryStatus;
			} else {
				status = QueryStatus.CANCEL;
			}
		} else if (TidaResultSetType.QUERY.equals(expectedResultSetType)) {
			if (QueryType.QUERY.equals(queryType)) {

				// impossible to retrieve processed identifiers when we query
				// data
				if (QueryStatus.PROCESSANDGETIDS.equals(queryStatus)) {
					status = QueryStatus.CANCEL;
				} else {
					status = queryStatus;
				}
			} else {
				status = QueryStatus.CANCEL;
			}
		} else {
			throw new IllegalStateException("The queryType '" + queryType
					+ "' is not supported by the handler.");
		}

		return status;
	}

	/**
	 * Sets the expected {@code ResultSetType} of {@code this}. If the expected
	 * type is set to {@code TidaResultSetType#UNKNOWN} the handler will handle
	 * any kind of type. Otherwise the handling is canceled if the determined
	 * {@code QueryType} is unequal to the expected one.
	 * 
	 * @param expectedResultSetType
	 *            the expected {@code TidaResultSetType}
	 */
	public void setExpectedResultSetType(
			final TidaResultSetType expectedResultSetType) {
		this.expectedResultSetType = expectedResultSetType;
	}

	/**
	 * Gets the expected {@code TidaResultSetType}.
	 * 
	 * @return the expected {@code TidaResultSetType}
	 */
	public TidaResultSetType getExpectedResultSetType() {
		return expectedResultSetType;
	}

	/**
	 * Gets the determined {@code TidaResultSetType}.
	 * 
	 * @return the determined {@code TidaResultSetType}
	 */
	public TidaResultSetType getResultSetType() {
		return resultSetType;
	}

	@Override
	public InputStream getResourceStream(final String resource) {

		// get the path
		final String path;
		final boolean lookOnClasspath;
		if (resource.startsWith(PREFIX_CLASSPATH + ":")) {
			path = getPath(resource, PREFIX_CLASSPATH);

			lookOnClasspath = true;
		} else if (resource.startsWith(PREFIX_FILE + ":")) {
			path = getPath(resource, PREFIX_FILE);
			lookOnClasspath = false;
		} else {
			path = resource;
			lookOnClasspath = false;
		}

		// get the Stream
		InputStream is = getStream(path, lookOnClasspath);

		// if nothing could be found try to add a leading slash
		if (is == null) {
			if (path.startsWith("/")) {
				is = getStream(path.substring(1), lookOnClasspath);
			} else {
				is = getStream("/" + path, lookOnClasspath);
			}
		}

		return is;
	}

	/**
	 * Method used to determine the stream for the specified path.
	 * 
	 * @param path
	 *            the path to retrieve the stream for
	 * @param lookOnClasspath
	 *            {@code true} if the resource should be searched on class-path,
	 *            otherwise - i.e. {@code false} is set - the resource will be
	 *            searched on the file-system
	 * 
	 * @return the {@code InputStream} for the resource, {@code null} if the
	 *         resource could not be found
	 */
	protected InputStream getStream(final String path,
			final boolean lookOnClasspath) {
		if (lookOnClasspath) {
			return getClass().getResourceAsStream(path);
		} else {
			try {
				return new FileInputStream(new File(path));
			} catch (final FileNotFoundException e) {
				return null;
			}
		}
	}

	/**
	 * Determines the path of the resource by removing the specified
	 * {@code prefix}. The method does not check if the prefix of the resource
	 * is equal to the one specified, i.e. the method just calls:
	 * 
	 * <pre>
	 * resource.substring(prefix.length() + 1);
	 * </pre>
	 * 
	 * @param resource
	 *            the resource to remove the prefix from
	 * @param prefix
	 *            the prefix to be removed
	 * 
	 * @return the resource with the removed prefix
	 */
	protected String getPath(final String resource, final String prefix) {
		return resource.substring(prefix.length() + 1);
	}

	@Override
	public DataType[] getHeader() {
		return header;
	}

	/**
	 * Determines the zero-based position of the header with the specified
	 * {@code name}.
	 * 
	 * @param name
	 *            the name to retrieve the position for
	 * 
	 * @return the determined position or {@code -1} if no header with the
	 *         specified {@code name} could be found
	 */
	public int getHeaderPosition(final String name) {
		if (headerNames == null) {
			if (name == null) {
				return -1;
			} else if (name.matches("%d+")) {
				return Integer.parseInt(name);
			} else {
				return -1;
			}
		} else {

			for (int i = 0; i < headerNames.length; i++) {
				final String headerName = headerNames[i];
				if (headerName.equals(name)) {
					return i;
				}
			}

			return -1;
		}
	}

	/**
	 * Gets the {@code DataType} of the header at the specified zero-based
	 * {@code pos}.
	 * 
	 * @param pos
	 *            the zero-based position to get the {@code DataType} for
	 * 
	 * @return the retrieved {@code DataType} or {@code null} if no header at
	 *         the specified {@code pos} could be found
	 */
	public DataType getHeaderType(final int pos) {

		// no header, nothing is valid
		if (header == null) {
			return null;
		}
		// check the position
		else if (pos < 0 || pos >= header.length) {
			return null;
		} else {
			return header[pos];
		}
	}

	/**
	 * Gets the name of the header at the specified zero-based {@code pos}.
	 * 
	 * @param pos
	 *            the zero-based position to get the name for
	 * 
	 * @return the retrieved name or {@code null} if no header at the specified
	 *         {@code pos} could be found
	 */
	public String getHeaderName(final int pos) {
		// no header, nothing is valid
		if (headerNames == null) {
			return "" + pos;
		}
		// check the position
		else if (pos < 0 || pos >= header.length) {
			return null;
		} else {
			return headerNames[pos];
		}
	}

	/**
	 * Checks if the {@code clazz} is a valid type for the header specified at
	 * the zero-based {@code pos}.
	 * 
	 * @param pos
	 *            the zero-based position to check the type for
	 * @param clazz
	 *            the type to be checked
	 * 
	 * @return {@code true} if the specified {@code clazz} is a valid type for
	 *         the header at position {@code pos}, otherwise {@code false}
	 */
	public boolean isValidHeaderType(final int pos, final Class<?> clazz) {

		// no header, nothing is valid
		if (header == null) {
			return false;
		}
		// check the position
		else if (pos < 0 || pos >= header.length) {
			return false;
		}
		// check the Class<?>
		else if (header[pos].isClass(clazz)) {
			return true;
		}
		// invalid result
		else {
			return false;
		}
	}

	/**
	 * Casts the value at the specified zero-based position to the specified
	 * type.
	 * 
	 * @param pos
	 *            the zero-based position to get the value of the last result
	 *            for
	 * @param clazz
	 *            the type to cast the value to, the validity is not checked but
	 *            can be checked using {@link #isValidHeaderType(int, Class)}
	 * 
	 * @return the retrieved value
	 * 
	 * @see #isValidHeaderType(int, Class)
	 */
	@SuppressWarnings("unchecked")
	public <T> T cast(final int pos, final Class<T> clazz) {

		// check the position
		if (pos < 0 || pos >= header.length) {
			throw new IllegalArgumentException("Invalid position used.");
		}

		return (T) lastResult[pos];
	}

	@Override
	public boolean handleResult(final ResponseType type, final Object[] value) {
		if (value == null) {
			throw new NullPointerException(
					"The retrieved value cannot be null.");
		} else if (ResponseType.INT_ARRAY.equals(type)) {
			this.generatedIds = (Integer[]) value;
		} else if (ResponseType.INT.equals(type)) {
			this.countValue = (Integer) value[0];
		} else if (ResponseType.RESULT.equals(type)) {
			this.lastResult = value;
		} else if (ResponseType.HEADER.equals(type)) {
			this.header = (DataType[]) value;
		} else if (ResponseType.HEADERNAMES.equals(type)) {
			this.headerNames = (String[]) value;
		} else if (ResponseType.EOM.equals(type)) {
			/*
			 * Nothing to do, important it that it is reached and therefore stop
			 * reading anything further.
			 */
		} else {
			throw new IllegalArgumentException(
					"Unexpected values retrieved from type '" + type + "'.");
		}

		/*
		 * Auto-read everything as long as we didn't read the end of meta-data
		 * or any results.
		 */
		return !(ResponseType.EOM.equals(type) || ResponseType.RESULT
				.equals(type));
	}

	/**
	 * Method checks if the end-of-response was reached.
	 * 
	 * @return {@code true} if the end was reached, otherwise {@code false}
	 */
	public boolean reachedEOR() {
		return eor;
	}

	/**
	 * Method checks if the end-of-response was reached.
	 * 
	 * @return {@code true} if the end was reached, otherwise {@code false}
	 */
	public boolean isEOR() {
		return eor;
	}

	@Override
	public void signalEORReached() {
		eor = true;
	}

	@Override
	public void resetHandler() {
		eor = false;
		expectedResultSetType = TidaResultSetType.UNKNOWN;
		resultSetType = null;
		queryStatus = QueryStatus.PROCESS;
		header = null;
		headerNames = null;
		generatedIds = null;
		countValue = -1;
		lastResult = null;
	}

	/**
	 * Gets the record retrieved last. The method returns {@code null} if no
	 * such last-record exists.
	 * 
	 * @return the record retrieved last
	 */
	public Object[] getLastResult() {
		return lastResult;
	}

	/**
	 * Gets the generated identifiers received while handling.
	 * 
	 * @return the generated identifiers received
	 */
	public Integer[] getGeneratedIds() {
		return generatedIds;
	}

	/**
	 * Gets the current {@code QueryStatus} of the handler.
	 * 
	 * @return the current {@code QueryStatus} of the handler
	 */
	public QueryStatus getQueryStatus() {
		return queryStatus;
	}

	/**
	 * Sets the current {@code QueryStatus} of the handler.
	 * 
	 * @param queryStatus
	 *            the current {@code QueryStatus} of the handler to be set
	 */
	public void setQueryStatus(final QueryStatus queryStatus) {
		this.queryStatus = queryStatus;
	}

	/**
	 * Gets the retrieved count value of the handler.
	 * 
	 * @return the retrieved count value, can be {@ode null} if no such
	 *         value was handled so far
	 */
	public Integer getCountValue() {
		return countValue;
	}

	/**
	 * Gets the retrieved header-names handled by {@code this}.
	 * 
	 * @return the retrieved header-names
	 */
	public String[] getHeaderNames() {
		return headerNames;
	}
}
