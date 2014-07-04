package net.meisen.dissertation.jdbc;

import java.util.Properties;

import net.meisen.dissertation.server.TidaServer;

import org.junit.After;
import org.junit.Before;

/**
 * A test base used to start a server.
 * 
 * @author pmeisen
 * 
 */
public abstract class TestBaseForConnections {

	/**
	 * The started server instance.
	 */
	protected TidaServer server;

	private int port;

	/**
	 * Start a test-server for testing purposes.
	 */
	@Before
	public void startServer() {
		port = 6666;

		final Properties properties = new Properties();
		properties.setProperty("tida.server.tsql.port", "" + getPort());
		properties.setProperty("tida.server.tsql.enabled", "true");
		properties.setProperty("tida.server.http.enabled", "false");

		server = TidaServer.create(properties);
		server.startAsync();
	}

	/**
	 * CleanUp after the test.
	 */
	@After
	public void shutdownServer() {
		server.shutdown(true);
	}

	/**
	 * Gets the used port of the server.
	 * 
	 * @return the used port of the server
	 */
	public int getPort() {
		return port;
	}

	/**
	 * Gets the JDBC-URL to connect to the server.
	 * 
	 * @return the JDBC-URL to connect to the server
	 */
	public String getJdbc() {
		return "jdbc:tida://localhost:" + getPort();
	}
}
