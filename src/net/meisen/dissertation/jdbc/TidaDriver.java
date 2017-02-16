package net.meisen.dissertation.jdbc;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * The driver of a JDBC for a tida-server.
 * 
 * @author pmeisen
 * 
 */
public class TidaDriver implements Driver {
	static {
		try {
			DriverManager.registerDriver(new TidaDriver());
		} catch (final SQLException e) {
			throw new IllegalStateException(
					"Unable to register the TidaDriver.", e);
		}
	}

	@Override
	public boolean acceptsURL(final String url) throws SQLException {
		return parseURL(url, null) != null;
	}

	@Override
	public TidaConnection connect(final String url, final Properties info)
			throws SQLException {

		// return null if the driver cannot handle the url
		final DriverProperties properties = parseURL(url, info);
		if (properties == null) {
			return null;
		} else {
			return new TidaConnection(properties);
		}
	}

	@Override
	public int getMajorVersion() {
		return Constants.getVersion().getMajorAsInt();
	}

	@Override
	public int getMinorVersion() {
		return Constants.getVersion().getMinorAsInt();
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(final String url,
			final Properties info) throws SQLException {
		final DriverProperties p = parseURL(url, info);

		if (p == null) {
			return new DriverPropertyInfo[0];
		} else {
			return p.getDriverPropertyInfo();
		}
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw TidaSqlExceptions.createNotSupportedException(2003);
	}

	/**
	 * Method used to parse the {@code url} into the different
	 * {@code DriverProperties}. The additional {@code defaults} can be used to
	 * set not specified values.
	 * 
	 * @param rawUrl
	 *            the url to be parsed
	 * @param defaults
	 *            the defaults to be used if not specified within the
	 *            {@code url}, can be {@code null}
	 * 
	 * @return the parsed {@code DriverProperties}, returns {@code null} if the
	 *         specified {@code rawUrl} is not a valid url for a tida
	 *         implementation (i.e. if the protocol is not equal to
	 *         {@link Constants#URL_PREFIX}).
	 * 
	 * @throws SQLException if an exception occurs
	 */
	protected DriverProperties parseURL(final String rawUrl, final Properties defaults) throws SQLException {
		final StringBuilder sb = new StringBuilder(rawUrl);

		// make sure we have the correct prefix
		if (!rawUrl.startsWith(Constants.URL_PREFIX)) {
			return null;
		} else {
			sb.delete(0, Constants.URL_PREFIX.length());
		}

		// check available data
		final int atSeparator = sb.lastIndexOf("@");
		final StringBuilder credentials;
		final StringBuilder server;
		if (atSeparator < 0) {
			credentials = null;
			server = sb;
		} else {
			credentials = new StringBuilder(sb.substring(0, atSeparator));
			server = new StringBuilder(sb.substring(atSeparator + 1));
		}

		// check credentials
		String user;
		String password;
		if (credentials != null) {
			final int userSeparator = credentials.indexOf(":");
			if (userSeparator < 0) {
				user = credentials.toString();
				password = "";
			} else {
				user = credentials.substring(0, userSeparator);
				password = credentials.substring(userSeparator + 1);
			}
		} else {
			user = null;
			password = null;
		}

		// check for a portSeparator
		final int portSeparator = server.lastIndexOf(":");
		String host;
		String port;
		if (portSeparator < 0) {
			host = server.toString();
			port = null;
		} else {
			host = server.substring(0, portSeparator);
			port = server.substring(portSeparator + 1);
		}

		// check for defaults
		if (host == null || host.trim().isEmpty()) {
			host = defaults == null ? null : defaults
					.getProperty(DriverProperties.PROPERTY_HOST);
		}
		if (port == null || port.trim().isEmpty()) {
			port = defaults == null ? null : defaults
					.getProperty(DriverProperties.PROPERTY_PORT);
		}
		if (user == null || user.trim().isEmpty()) {
			user = defaults == null ? null : defaults
					.getProperty(DriverProperties.PROPERTY_USER);
		}
		if (password == null || password.trim().isEmpty()) {
			password = defaults == null ? null : defaults
					.getProperty(DriverProperties.PROPERTY_PASSWORD);
		}

		// throw exception if invalid
		final int portNr;
		if (port == null) {
			throw TidaSqlExceptions.createException(2000);
		} else if (host == null || host.trim().isEmpty()) {
			throw TidaSqlExceptions.createException(2001);
		} else {
			try {
				portNr = Integer.parseInt(port);
			} catch (final NumberFormatException e) {
				throw TidaSqlExceptions.createException(2002, e, port);
			}
		}

		// create the properties
		final DriverProperties properties = new DriverProperties(rawUrl, user,
				password, host, portNr);

		// apply other defaults
		properties.applyDefaults(defaults);

		return properties;
	}
}
