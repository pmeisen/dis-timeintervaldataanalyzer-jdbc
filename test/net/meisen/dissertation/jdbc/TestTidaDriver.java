package net.meisen.dissertation.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import net.meisen.dissertation.server.TidaServer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests the driver.
 * 
 * @author pmeisen
 * 
 */
public class TestTidaDriver {

	/**
	 * Rule to evaluate exceptions
	 */
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	/**
	 * Tests the parsing of a url.
	 * 
	 * @throws SQLException
	 *             if the url is invalid
	 */
	@Test
	public void testParsingWithNullProperties() throws SQLException {
		ServerProperties p;

		final TidaDriver driver = new TidaDriver();

		assertNull(driver.parseURL("jdbc:invalid://localhost", null));
		assertNotNull(driver.parseURL("jdbc:tida://localhost:7000", null));

		// get the properties and check those
		p = driver.parseURL("jdbc:tida://localhost:7000", null);
		assertEquals(7000, p.getPort());
		assertEquals("localhost", p.getHost());
		assertNull(p.getUser());
		assertNull(p.getPassword());

		p = driver.parseURL("jdbc:tida://philipp@meisen.net:7001", null);
		assertEquals(7001, p.getPort());
		assertEquals("meisen.net", p.getHost());
		assertEquals("philipp", p.getUser());
		assertNull(p.getPassword());

		p = driver.parseURL("jdbc:tida://philipp:secret@lalalala:7002", null);
		assertEquals(7002, p.getPort());
		assertEquals("lalalala", p.getHost());
		assertEquals("philipp", p.getUser());
		assertEquals("secret", p.getPassword());

		p = driver.parseURL("jdbc:tida://philipp:s@cr:et@lalalala:7002", null);
		assertEquals(7002, p.getPort());
		assertEquals("lalalala", p.getHost());
		assertEquals("philipp", p.getUser());
		assertEquals("s@cr:et", p.getPassword());
	}

	/**
	 * Tests the exception to be thrown when a port isn't specified.
	 * 
	 * @throws SQLException
	 *             the expected exception
	 */
	@Test
	public void testNoPortWithNullProperties() throws SQLException {
		thrown.expect(SQLException.class);
		thrown.expectMessage("does not specify any port");

		final TidaDriver driver = new TidaDriver();
		driver.parseURL("jdbc:tida://localhost", null);
	}

	/**
	 * Tests the exception to be thrown when an invalid port is specified.
	 * 
	 * @throws SQLException
	 *             the expected exception
	 */
	@Test
	public void testInvalidPortWithNullProperties() throws SQLException {
		thrown.expect(SQLException.class);
		thrown.expectMessage("'port' is not a valid number");

		final TidaDriver driver = new TidaDriver();
		driver.parseURL("jdbc:tida://localhost:port", null);
	}

	/**
	 * Tests the exception to be thrown when an invalid host is specified.
	 * 
	 * @throws SQLException
	 *             the expected exception
	 */
	@Test
	public void testInvalidHostWithNullProperties() throws SQLException {
		thrown.expect(SQLException.class);
		thrown.expectMessage("must define a valid host");

		final TidaDriver driver = new TidaDriver();
		driver.parseURL("jdbc:tida://  :7000", null);
	}

	/**
	 * Tests the parsing using pre-defined default properties.
	 * 
	 * @throws SQLException
	 *             if the parsing fails
	 */
	@Test
	public void testParsingWithProperties() throws SQLException {
		ServerProperties p;

		final TidaDriver driver = new TidaDriver();

		// create properties with a port
		final Properties portProperties = new Properties();
		portProperties.setProperty("port", "8080");

		// get the properties and check those
		p = driver.parseURL("jdbc:tida://localhost", portProperties);
		assertEquals(8080, p.getPort());
		assertEquals("localhost", p.getHost());

		// create properties with a host
		final Properties hostProperties = new Properties();
		hostProperties.setProperty("host", "myWorld");

		p = driver.parseURL("jdbc:tida://:6000", hostProperties);
		assertEquals(6000, p.getPort());
		assertEquals("myWorld", p.getHost());

		// create properties with a port & host
		final Properties portHostProperties = new Properties();
		portHostProperties.setProperty("host", "im");
		portHostProperties.setProperty("port", "666");

		p = driver.parseURL("jdbc:tida://", portHostProperties);
		assertEquals(666, p.getPort());
		assertEquals("im", p.getHost());

		// create properties with a port, host, username and password
		final Properties fullProperties = new Properties();
		fullProperties.setProperty("user", "philipp");
		fullProperties.setProperty("password", "secret");
		fullProperties.setProperty("host", "im");
		fullProperties.setProperty("port", "666");

		p = driver.parseURL("jdbc:tida://", fullProperties);
		assertEquals(666, p.getPort());
		assertEquals("im", p.getHost());
		assertEquals("philipp", p.getUser());
		assertEquals("secret", p.getPassword());
	}

	/**
	 * Tests the usage of
	 * {@code DriverManager#getConnection(String, Properties)}.
	 * 
	 * @throws SQLException
	 *             if an sql problem occures
	 * @throws InterruptedException
	 *             if waiting for the server failed
	 */
	@Test
	public void testUsageOfDriverManager() throws SQLException,
			InterruptedException {
		final TidaServer server = TidaServer.create();
		server.startAsync();

		// wait for the server to start
		while (!server.isRunning()) {
			Thread.sleep(50);
		}

		// check if the connections can be established
		try {
			assertNotNull(DriverManager.getConnection(
					"jdbc:tida://localhost:7001", "user", "pw"));
			assertNotNull(DriverManager
					.getConnection("jdbc:tida://localhost:7001"));
		} finally {
			server.shutdown();
		}
	}
}
