package net.meisen.dissertation.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.sql.DriverManager;
import java.sql.SQLException;

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
		final TidaDriver driver = new TidaDriver();

		assertNull(driver.parseURL("jdbc:invalid://localhost", null));
		assertNotNull(driver.parseURL("jdbc:tida://localhost:7000", null));

		// get the properties and check those
		final ServerProperties p = driver.parseURL(
				"jdbc:tida://localhost:7000", null);
		assertEquals(7000, p.getPort());
		assertEquals("localhost", p.getHost());
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

	@Test
	public void testParsingWithProperties() throws SQLException {
		final TidaDriver driver = new TidaDriver();

		assertNull(driver.parseURL("jdbc:invalid://localhost", null));
		assertNotNull(driver.parseURL("jdbc:tida://localhost:7000", null));

		// get the properties and check those
		final ServerProperties p = driver.parseURL(
				"jdbc:tida://localhost:7000", null);
		assertEquals(7000, p.getPort());
		assertEquals("localhost", p.getHost());
	}

	@Test
	public void testUsageOfDriverManager() throws SQLException {
		assertNotNull(DriverManager.getConnection("jdbc:tida://localhost:7001"));
	}
}
