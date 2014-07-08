package net.meisen.dissertation.jdbc;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

/**
 * The {@code DriverProperties} contain the set properties for the server, i.e.
 * {@code host} and {@code port}.
 * 
 * @author pmeisen
 * 
 */
public class DriverProperties {
	/**
	 * Property to specify the timeout of the client-connection in milliseconds.
	 */
	public static final String PROPERTY_TIMEOUT = "timeout";
	/**
	 * Property to specify the handler class used to handle resource requests.
	 */
	public static final String PROPERTY_HANDLERCLASS = "handlerclass";
	/**
	 * Property to specify the host of the tida-server to connect to.
	 */
	public static final String PROPERTY_PORT = "port";
	/**
	 * Property to specify the port of the tida-server to connect to.
	 */
	public static final String PROPERTY_HOST = "host";
	/**
	 * Property to specify the user used to connect to the tida-server.
	 */
	public static final String PROPERTY_USER = "user";
	/**
	 * Property to specify the password used to connect to the tida-server.
	 */
	public static final String PROPERTY_PASSWORD = "password";
	/**
	 * Property used to retrieve the raw-url used.
	 */
	public static final String PROPERTY_RAWURL = "rawurl";

	private final String host;
	private final int port;
	private final String user;
	private final String password;

	private final String rawJdbc;

	private String handlerClass = QueryResponseHandler.class.getName();
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
	public DriverProperties(final String rawJdbc, final String host,
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
	public DriverProperties(final String rawJdbc, final String user,
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

	/**
	 * Gets the timeout in milliseconds used for the connection to the server.
	 * If the client does not use the connection within the time, it is
	 * automatically disconnected.
	 * 
	 * @return the timeout in milliseconds
	 */
	public int getTimeout() {
		return timeout;
	}

	/**
	 * Sets the timeout in milliseconds used for the connection to the server.
	 * If the client does not use the connection within the time, it is
	 * automatically disconnected.
	 * 
	 * @param timeout
	 *            the timeout in milliseconds
	 */
	public void setTimeout(final int timeout) {
		this.timeout = timeout;
	}

	/**
	 * Gets the class of the {@code QueryResponseHandler} to be used.
	 * 
	 * @return the class of the {@code QueryResponseHandler} to be used
	 */
	public String getHandlerClass() {
		return handlerClass;
	}

	/**
	 * Sets the class of the {@code QueryResponseHandler} to be used.
	 * 
	 * @param handlerClass
	 *            the class of the {@code QueryResponseHandler} to be used
	 */
	public void setHandlerClass(final String handlerClass) {
		this.handlerClass = handlerClass;
	}

	/**
	 * Get the raw jdbc-url used to connect to the driver.
	 * 
	 * @return the raw jdbc-url used to connect to the driver
	 */
	public String getRawJdbc() {
		return rawJdbc;
	}

	/**
	 * Gets {@code this} as {@code DriverPropertyInfo}.
	 * 
	 * @return {@code this} as {@code DriverPropertyInfo}
	 * 
	 * @see DriverPropertyInfo
	 */
	public DriverPropertyInfo[] getDriverPropertyInfo() {
		final DriverPropertyInfo hostProp = new DriverPropertyInfo(
				DriverProperties.PROPERTY_HOST, getHost());
		hostProp.required = true;
		hostProp.description = "the host of the tida-server to connect to";

		final DriverPropertyInfo portProp = new DriverPropertyInfo(
				DriverProperties.PROPERTY_PORT, "" + getPort());
		portProp.required = true;
		portProp.description = "the port of the tida-server to connect to";

		final DriverPropertyInfo userProp = new DriverPropertyInfo(
				DriverProperties.PROPERTY_USER, getUser());
		userProp.required = true;
		userProp.description = "the user used to connect to the tida-server";

		final DriverPropertyInfo passwordProp = new DriverPropertyInfo(
				DriverProperties.PROPERTY_PASSWORD, getPassword());
		passwordProp.required = true;
		passwordProp.description = "the password used to connect to the tida-server";

		final DriverPropertyInfo timeoutProp = new DriverPropertyInfo(
				DriverProperties.PROPERTY_TIMEOUT, "" + getTimeout());
		timeoutProp.required = false;
		timeoutProp.description = "the timeout of the client-connection in milliseconds";

		final DriverPropertyInfo handlerProp = new DriverPropertyInfo(
				DriverProperties.PROPERTY_HANDLERCLASS, getHandlerClass());
		handlerProp.required = false;
		handlerProp.description = "handler class used to handle resource requests";

		// create the array and return it
		return new DriverPropertyInfo[] { hostProp, portProp, userProp,
				passwordProp, timeoutProp, handlerProp };
	}

	/**
	 * Get the property for the specified {@code name}.
	 * 
	 * @param name
	 *            the property to get the value for
	 * 
	 * @return the value of the property, or {@code null} if the property is
	 *         unknown
	 */
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
			return null;
		} else if (PROPERTY_TIMEOUT.equals(name)) {
			return "" + getTimeout();
		} else if (PROPERTY_HANDLERCLASS.equals(name)) {
			return getHandlerClass();
		} else {
			return null;
		}
	}

	/**
	 * Gets the properties of {@code this} as {@code Properties}.
	 * 
	 * @return the properties of {@code this} as {@code Properties}
	 * 
	 * @see Properties
	 */
	public Properties getProperties() {
		final Properties prop = new Properties();
		prop.setProperty(PROPERTY_PORT, "" + getPort());
		prop.setProperty(PROPERTY_HOST, getHost());
		prop.setProperty(PROPERTY_USER, getUser());
		prop.setProperty(PROPERTY_PASSWORD, get(PROPERTY_PASSWORD));
		prop.setProperty(PROPERTY_RAWURL, getRawJdbc());
		prop.setProperty(PROPERTY_TIMEOUT, "" + getTimeout());
		prop.setProperty(PROPERTY_HANDLERCLASS, getHandlerClass());

		return prop;
	}

	/**
	 * Applies the default properties to {@code this}.
	 * 
	 * @param defaults
	 *            the default properties to be applied
	 */
	public void applyDefaults(final Properties defaults) {
		if (defaults == null) {
			return;
		}

		// get the timeout
		final String defTimeout = defaults.getProperty(PROPERTY_TIMEOUT);
		if (defTimeout != null) {
			try {
				this.setTimeout(Integer.parseInt(defTimeout));
			} catch (final NumberFormatException e) {
				// ignore the value
			}
		}

		// get the handler
		final String defHandler = defaults.getProperty(PROPERTY_HANDLERCLASS);
		if (defHandler != null) {
			this.setHandlerClass(defHandler);
		}
	}
}
