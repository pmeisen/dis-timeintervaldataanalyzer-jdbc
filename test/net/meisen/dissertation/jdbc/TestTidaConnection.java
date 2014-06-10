package net.meisen.dissertation.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;

import net.meisen.dissertation.server.TidaServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the {@code TidaConnection} implementation.
 * 
 * @author pmeisen
 * 
 */
public class TestTidaConnection {
	private TidaServer server;

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
	 * Tests the implementation of {@code TidaConnection#isValid(int)}.
	 * 
	 * @throws Exception
	 *             if a problem occurred
	 */
	@Test
	public void testConnectionValidity() throws Exception {
		Connection conn;

		// just do some connections
		for (int i = 0; i < 1000; i++) {
			conn = DriverManager.getConnection("jdbc:tida://localhost:7001");
			assertEquals(TidaConnection.class, conn.getClass());

			// check the validity, must be true
			assertTrue(conn.isValid(100));
			assertTrue(conn.isValid(0));

			// close the connection, the answer must be false
			conn.close();
			assertFalse(conn.isValid(0));
		}

		// create a new connection
		conn = DriverManager.getConnection("jdbc:tida://localhost:7001");

		// shutdown the server and check the validity
		server.shutdown();
		assertFalse(conn.isValid(1000));
		conn.close();
	}

	/**
	 * CleanUp after the test.
	 */
	@After
	public void shutdownServer() {
		server.shutdown();
	}
}
