package net.meisen.dissertation.jdbc;

/**
 * The {@code ServerProperties} contain the set properties for the server, i.e.
 * {@code host} and {@code port}.
 * 
 * @author pmeisen
 * 
 */
public class ServerProperties {
	private final String host;
	private final int port;
	private final String user;
	private final String password;

	/**
	 * Constructor defining the port and host of the server.
	 * 
	 * @param port
	 *            the port of the server
	 * @param host
	 *            the host of the server
	 */
	public ServerProperties(final String host, final int port) {
		this(null, null, host, port);
	}

	/**
	 * Constructor defining the port and host of the server.
	 * 
	 * @param user
	 *            the username used for security
	 * @param password
	 *            the password used for security
	 * @param port
	 *            the port of the server
	 * @param host
	 *            the host of the server
	 */
	public ServerProperties(final String user, final String password,
			final String host, final int port) {
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

}
