package net.meisen.dissertation.jdbc;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

/**
 * The driver of a JDBC for a tida-server.
 * 
 * @author pmeisen
 * 
 */
public class TidaDriver implements Driver {
	private static final String PROPERTY_PORT = "port";
	private static final String PROPERTY_HOST = "host";
	private static final String PROPERTY_USER = "user";
	private static final String PROPERTY_PASSWORD = "password";

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
		final ServerProperties properties = parseURL(url, info);
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
		final ServerProperties p = parseURL(url, info);

		final DriverPropertyInfo hostProp = new DriverPropertyInfo(
				PROPERTY_HOST, p == null ? null : p.getHost());
		hostProp.required = true;
		hostProp.description = "the host of the tida-server to connect to";

		final DriverPropertyInfo portProp = new DriverPropertyInfo(
				PROPERTY_PORT, p == null ? null : "" + p.getPort());
		portProp.required = true;
		portProp.description = "the port of the tida-server to connect to";

		final DriverPropertyInfo userProp = new DriverPropertyInfo(
				PROPERTY_USER, p == null ? null : p.getUser());
		portProp.required = true;
		portProp.description = "the user used to connect to the tida-server";

		final DriverPropertyInfo passwordProp = new DriverPropertyInfo(
				PROPERTY_PASSWORD, p == null ? null : p.getPassword());
		portProp.required = true;
		portProp.description = "the password used to connect to the tida-server";

		// create the array and return it
		return new DriverPropertyInfo[] { hostProp, portProp, userProp,
				passwordProp };
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	/**
	 * Method used to parse the {@code url} into the different
	 * {@code ServerProperties}. The additional {@code defaults} can be used to
	 * set not specified values.
	 * 
	 * @param rawUrl
	 *            the url to be parsed
	 * @param defaults
	 *            the defaults to be used if not specified within the
	 *            {@code url}, can be {@code null}
	 * 
	 * @return the parsed {@code ServerProperties}, returns {@code null} if the
	 *         specified {@code rawUrl} is not a valid url for a tida
	 *         implementation (i.e. if the protocol is not equal to
	 *         {@link Constants#URL_PREFIX}).
	 * 
	 * @throws SQLException
	 */
	protected ServerProperties parseURL(final String rawUrl,
			final Properties defaults) throws SQLException {
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
					.getProperty(PROPERTY_HOST);
		}
		if (port == null || port.trim().isEmpty()) {
			port = defaults == null ? null : defaults
					.getProperty(PROPERTY_PORT);
		}
		if (user == null || user.trim().isEmpty()) {
			user = defaults == null ? null : defaults
					.getProperty(PROPERTY_USER);
		}
		if (password == null || password.trim().isEmpty()) {
			password = defaults == null ? null : defaults
					.getProperty(PROPERTY_PASSWORD);
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
		return new ServerProperties(user, password, host, portNr);
	}
}
