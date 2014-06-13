package net.meisen.dissertation.jdbc;

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

	/**
	 * Start a test-server for testing purposes.
	 * 
	 * @throws InterruptedException
	 *             if the server cannot be started
	 */
	@Before
	public void startServer() throws InterruptedException {
		server = TidaServer.create();
		server.startAsync();

		// wait for the server to start
		while (!server.isRunning()) {
			Thread.sleep(50);
		}
	}

	/**
	 * CleanUp after the test.
	 */
	@After
	public void shutdownServer() {
		server.shutdown(true);
	}
}
