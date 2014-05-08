package net.meisen.dissertation.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import net.meisen.dissertation.jdbc.version.Manifest;
import net.meisen.dissertation.jdbc.version.ManifestInfo;
import net.meisen.dissertation.jdbc.version.Version;

/**
 * The driver of a JDBC for a tida-server.
 * 
 * @author pmeisen
 * 
 */
public class TidaDriver implements Driver {
	/**
	 * The url-prefix used to mark a url to be a tida-url.
	 */
	public static final String URL_PREFIX = "jdbc:tida://";
	/**
	 * The complete syntax of a tida-url.
	 */
	public static final String URL_SYNTAX = URL_PREFIX + "[host]:[port]";

	private static final String PROPERTY_PORT = "port";
	private static final String PROPERTY_HOST = "host";

	private Version version = null;

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
	public Connection connect(final String url, final Properties info)
			throws SQLException {

		// return null if the driver cannot handle the url
		parseURL(url, info);

		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getMajorVersion() {
		return getVersion().getMajorAsInt();
	}

	@Override
	public int getMinorVersion() {
		return getVersion().getMinorAsInt();
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

		// create the array and return it
		return new DriverPropertyInfo[] { hostProp, portProp };
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	/**
	 * Gets the version of the driver read from it's manifest.
	 * 
	 * @return the version
	 */
	public Version getVersion() {
		if (version == null) {
			try {
				final ManifestInfo info = Manifest
						.getManifestInfo(TidaDriver.class);
				version = Version.parse(info.getImplementationVersion());
			} catch (IOException e) {
				version = null;
			}
		}
		return version;
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
	 *         {@link #URL_PREFIX}).
	 * 
	 * @throws SQLException
	 */
	protected ServerProperties parseURL(final String rawUrl,
			final Properties defaults) throws SQLException {
		final StringBuilder sb = new StringBuilder(rawUrl);

		// make sure we have the correct prefix
		if (!rawUrl.startsWith(URL_PREFIX)) {
			return null;
		} else {
			sb.delete(0, URL_PREFIX.length());
		}

		// check for a portSeparator
		final int portSeparator = sb.indexOf(":");
		final String host;
		final String port;
		if (portSeparator < 0) {
			host = sb.toString();
			port = defaults == null ? null : defaults
					.getProperty(PROPERTY_PORT);
			if (port == null) {
				throw new SQLException(
						"The url does not specify any port, please use: "
								+ URL_SYNTAX);
			}
		} else {
			host = sb.substring(0, portSeparator);
			port = sb.substring(portSeparator + 1);
		}

		// check if the port is a valid number
		final int portNr;
		try {
			portNr = Integer.parseInt(port);
		} catch (final NumberFormatException e) {
			throw new SQLException("The specified port '" + port
					+ "' is not a valid number, please use: " + URL_SYNTAX);
		}

		// check the host
		final String hostName;
		if (host == null || host.trim().isEmpty()) {
			hostName = defaults == null ? null : defaults
					.getProperty(PROPERTY_HOST);

			if (hostName == null || hostName.trim().isEmpty()) {
				throw new SQLException(
						"The url must define a valid host, please use: "
								+ URL_SYNTAX);
			}
		} else {
			hostName = host;
		}

		// create the properties
		return new ServerProperties(portNr, hostName);
	}
}
