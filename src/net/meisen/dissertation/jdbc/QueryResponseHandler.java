package net.meisen.dissertation.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import net.meisen.dissertation.jdbc.protocol.IResponseHandler;
import net.meisen.dissertation.jdbc.protocol.RetrievedValue;

public class QueryResponseHandler implements IResponseHandler {
	public final static String PREFIX_CLASSPATH = "classpath";
	public final static String PREFIX_FILE = "file";

	private boolean singleResultExpected = false;
	private boolean eor = false;

	private RetrievedValue lastResult = null;

	private String[] headerNames = null;
	private Class<?>[] header = null;

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
	public void setHeader(final Class<?>[] header) {
		this.header = header;
	}

	@Override
	public void setHeaderNames(final String[] header) {
		this.headerNames = header;
	}

	public boolean hasHeader() {
		return this.header != null || this.headerNames != null;
	}

	@Override
	public boolean handleResult(final RetrievedValue value) {
		if (value == null) {
			throw new NullPointerException(
					"The retrieved value cannot be null.");
		} else if (isSingleResultExpected() && lastResult != null) {
			lastResult = null;
		} else {
			lastResult = value;
		}

		return isSingleResultExpected();
	}

	public boolean isSingleResultExpected() {
		return singleResultExpected;
	}

	public void setSingleResultExpected(final boolean singleResultExpected) {
		this.singleResultExpected = singleResultExpected;
	}

	public boolean isSingleResult() {
		return (eor && isSingleResultExpected() && lastResult != null);
	}

	public boolean reachedEOR() {
		return eor;
	}

	@Override
	public void signalEORReached() {
		eor = true;
	}

	public byte[] getLastResult() {
		return lastResult == null ? null : lastResult.bytes;
	}
}
