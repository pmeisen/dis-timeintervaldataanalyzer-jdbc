package net.meisen.dissertation.jdbc;

import java.util.Properties;

/**
 * The {@code ServerProperties} contain the set properties for the server, i.e.
 * {@code host} and {@code port}.
 * 
 * @author pmeisen
 * 
 */
public class ServerProperties {
	public static final String PROPERTY_PORT = "port";
	public static final String PROPERTY_HOST = "host";
	public static final String PROPERTY_USER = "user";
	public static final String PROPERTY_PASSWORD = "password";
	public static final String PROPERTY_RAWURL = "rawurl";

	private final String host;
	private final int port;
	private final String user;
	private final String password;

	private final String rawJdbc;

	private int timeout = 0;

	/**
	 * Constructor defining the port and host of the server.
	 * 
	 * @param rawJdbc
	 *            the raw JDBC string used
	 * @param port
	 *            the port of the server
	 * @param host
	 *            the host of the server
	 */
	public ServerProperties(final String rawJdbc, final String host,
			final int port) {
		this(rawJdbc, null, null, host, port);
	}

	/**
	 * Constructor defining the port and host of the server.
	 * 
	 * @param rawJdbc
	 *            the raw JDBC string used
	 * @param user
	 *            the username used for security
	 * @param password
	 *            the password used for security
	 * @param port
	 *            the port of the server
	 * @param host
	 *            the host of the server
	 */
	public ServerProperties(final String rawJdbc, final String user,
			final String password, final String host, final int port) {
		this.rawJdbc = rawJdbc;
		this.port = port;
		this.host = host;
		this.user = user;
		this.password = password;
	}

	/**
	 * Gets the specified port.
	 * 
	 * @return the specified port
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Gets the specified host.
	 * 
	 * @return the specified host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Gets the specified username.
	 * 
	 * @return the specified username
	 */
	public String getUser() {
		return user;
	}

	/**
	 * Gets the specified password.
	 * 
	 * @return the specified password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Gets the URL used to connect, without any user and password information.
	 * 
	 * @return the URL used to connect
	 */
	public String getURL() {
		return Constants.URL_SYNTAX.replace(Constants.URL_HOST_PLACEHOLDER,
				getHost()).replace(Constants.URL_PORT_PLACEHOLDER,
				"" + getPort());
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public String getRawJdbc() {
		return rawJdbc;
	}

	public String get(final String name) {
		if (PROPERTY_PORT.equals(name)) {
			return "" + getPort();
		} else if (PROPERTY_HOST.equals(name)) {
			return getHost();
		} else if (PROPERTY_USER.equals(name)) {
			return getUser();
		} else if (PROPERTY_RAWURL.equals(name)) {
			return getRawJdbc();
		} else if (PROPERTY_PASSWORD.equals(name)) {
			return "*";
		} else {
			return null;
		}
	}

	public Properties getProperties() {
		final Properties prop = new Properties();
		prop.setProperty(PROPERTY_PORT, "" + getPort());
		prop.setProperty(PROPERTY_HOST, getHost());
		prop.setProperty(PROPERTY_USER, getUser());
		prop.setProperty(PROPERTY_PASSWORD, get(PROPERTY_PASSWORD));
		prop.setProperty(PROPERTY_RAWURL, getRawJdbc());

		return prop;
	}

}
