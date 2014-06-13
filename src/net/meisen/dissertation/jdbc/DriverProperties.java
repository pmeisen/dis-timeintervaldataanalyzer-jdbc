package net.meisen.dissertation.jdbc;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

import net.meisen.dissertation.jdbc.protocol.Protocol.IResponseHandler;

/**
 * The {@code DriverProperties} contain the set properties for the server, i.e.
 * {@code host} and {@code port}.
 * 
 * @author pmeisen
 * 
 */
public class DriverProperties {
	public static final String PROPERTY_TIMEOUT = "timeout";
	public static final String PROPERTY_HANDLERCLASS = "handlerclass";
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

	private String handlerClass = QueryResponseHandler.class.getName();
	private int timeout = 0;

	private IResponseHandler handler = null;

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

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(final int timeout) {
		this.timeout = timeout;
	}

	public String getHandlerClass() {
		return handlerClass;
	}

	public void setHandlerClass(final String handlerClass) {
		this.handlerClass = handlerClass;
	}

	public String getRawJdbc() {
		return rawJdbc;
	}

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
		portProp.required = true;
		portProp.description = "the user used to connect to the tida-server";

		final DriverPropertyInfo passwordProp = new DriverPropertyInfo(
				DriverProperties.PROPERTY_PASSWORD, getPassword());
		portProp.required = true;
		portProp.description = "the password used to connect to the tida-server";

		final DriverPropertyInfo timeoutProp = new DriverPropertyInfo(
				DriverProperties.PROPERTY_TIMEOUT, "" + getTimeout());
		portProp.required = false;
		portProp.description = "the timeout of the client-connection";

		final DriverPropertyInfo handlerProp = new DriverPropertyInfo(
				DriverProperties.PROPERTY_HANDLERCLASS, getHandlerClass());
		portProp.required = false;
		portProp.description = "handler class used to handle resource requests";

		// create the array and return it
		return new DriverPropertyInfo[] { hostProp, portProp, userProp,
				passwordProp, timeoutProp, handlerProp };
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
		} else if (PROPERTY_TIMEOUT.equals(name)) {
			return "" + getTimeout();
		} else if (PROPERTY_HANDLERCLASS.equals(name)) {
			return getHandlerClass();
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
		prop.setProperty(PROPERTY_TIMEOUT, "" + getTimeout());
		prop.setProperty(PROPERTY_HANDLERCLASS, getHandlerClass());

		return prop;
	}

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
