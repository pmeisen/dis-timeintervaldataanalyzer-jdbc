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

public class QueryResponseHandler implements IResponseHandler {
	public final static String PREFIX_CLASSPATH = "classpath";
	public final static String PREFIX_FILE = "file";

	private boolean eor = false;
	private TidaResultSetType resultSetType = null;
	private TidaResultSetType expectedResultSetType = TidaResultSetType.UNKNOWN;
	private QueryStatus queryStatus = QueryStatus.PROCESS;
	private Object[] lastResult = null;
	private ResponseType lastType = null;

	private String[] headerNames = null;
	private DataType[] header = null;

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

	public void setExpectedResultSetType(
			final TidaResultSetType expectedResultSetType) {
		this.expectedResultSetType = expectedResultSetType;
	}

	public TidaResultSetType getExpectedResultSetType() {
		return expectedResultSetType;
	}

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

	protected String getPath(final String resource, final String prefix) {
		return resource.substring(prefix.length() + 1);
	}

	@Override
	public void setHeader(final DataType[] header) {
		this.header = header;
	}

	@Override
	public DataType[] getHeader() {
		return header;
	}

	@Override
	public void setHeaderNames(final String[] header) {
		this.headerNames = header;
	}

	@Override
	public boolean handleResult(final ResponseType type, final Object[] value) {
		if (value == null) {
			throw new NullPointerException(
					"The retrieved value cannot be null.");
		} else {
			lastResult = value;
			lastType = type;
		}

		// disable auto-read
		return false;
	}

	public boolean reachedEOR() {
		return eor;
	}

	@Override
	public void signalEORReached() {
		eor = true;
	}

	public Object[] getLastResult() {
		return lastResult;
	}

	public ResponseType getLastType() {
		return lastType;
	}

	public QueryStatus getQueryStatus() {
		return queryStatus;
	}

	public void setQueryStatus(final QueryStatus queryStatus) {
		this.queryStatus = queryStatus;
	}
}
