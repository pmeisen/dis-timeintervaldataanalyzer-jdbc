package net.meisen.dissertation.jdbc.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import net.meisen.dissertation.jdbc.TestBaseWithConnection;

import org.junit.Test;

/**
 * Tests some more complex communications.
 * 
 * @author pmeisen
 * 
 */
public class TestCommunication extends TestBaseWithConnection {

	@Override
	public String getConfig() {
		return "net/meisen/dissertation/model/testShiroAuthConfig.xml";
	}

	@Override
	public String getJdbc() {
		return getJdbc("admin");
	}

	/**
	 * Method to create a JDBC for a specific user.
	 * 
	 * @param username
	 *            the name of the user to create the JDBC for
	 * 
	 * @return the created JDBC-URL
	 */
	public String getJdbc(final String username) {
		return "jdbc:tida://" + username + ":password@localhost:" + getPort();
	}

	/**
	 * Tests the usage of the protocol with an {@code AuthManager}.
	 * 
	 * @throws SQLException
	 *             if an unexpected exception is thrown
	 */
	@Test
	public void testProtocolUsage() throws SQLException {
		int counter;
		Exception exception;
		Statement stmt;
		ResultSet res;

		// add some users and roles
		stmt = conn.createStatement();
		stmt.executeUpdate("ADD USER 'eddie'   WITH PASSWORD 'password'");
		stmt.executeUpdate("ADD USER 'philipp' WITH PASSWORD 'password' WITH ROLES 'connect', 'superuser' WITH PERMISSIONS 'MODEL.testNumberModel.modify'");
		stmt.executeUpdate("ADD USER 'tobias'  WITH PASSWORD 'password' WITH ROLES 'readOnlyNumberModel', 'connect'");

		stmt.executeUpdate("ADD ROLE 'readOnlyNumberModel' WITH PERMISSIONS 'MODEL.testNumberModel.query'");
		stmt.executeUpdate("ADD ROLE 'connect'             WITH PERMISSIONS 'GLOBAL.connectTSQL'");
		stmt.executeUpdate("ADD ROLE 'superuser'           WITH PERMISSIONS 'GLOBAL.load', 'GLOBAL.get', 'GLOBAL.queryAll', 'GLOBAL.modifyAll'");
		stmt.close();

		// connect as the new users
		final Connection eddieConn = DriverManager
				.getConnection(getJdbc("eddie"));
		final Connection philippConn = DriverManager
				.getConnection(getJdbc("philipp"));
		final Connection tobiasConn = DriverManager
				.getConnection(getJdbc("tobias"));

		// check if eddie tries to load a model - the account cannot connect
		stmt = eddieConn.createStatement();
		try {
			exception = null;
			stmt.executeUpdate("LOAD FROM 'classpath:/net/meisen/dissertation/model/testNumberModel.xml'");
		} catch (final SQLException e) {
			exception = e;
		} finally {
			stmt.close();
		}
		assertNotNull(exception);
		assertTrue(exception.getMessage().contains("PermissionException"));
		assertTrue(exception.getMessage().contains("GLOBAL.connectTSQL"));

		// check if tobias tries to load a model - the account cannot load
		stmt = tobiasConn.createStatement();
		try {
			exception = null;
			stmt.executeUpdate("LOAD FROM 'classpath:/net/meisen/dissertation/model/testNumberModel.xml'");
		} catch (final SQLException e) {
			exception = e;
		} finally {
			stmt.close();
		}
		assertNotNull(exception);
		assertTrue(exception.getMessage().contains("PermissionException"));
		assertTrue(exception.getMessage().contains("GLOBAL.load"));

		// load the model using philipp - it's a superuser
		stmt = philippConn.createStatement();
		stmt.executeUpdate("LOAD FROM 'classpath:/net/meisen/dissertation/model/testNumberModel.xml'");
		stmt.close();

		// let's try to add some data with tobias (the account cannot modify)
		stmt = tobiasConn.createStatement();
		try {
			exception = null;
			stmt.executeUpdate("INSERT INTO testNumberModel ([START], [END], NUMBER) VALUES (2, 3, '100'), (1, 5, '100')");
		} catch (final SQLException e) {
			exception = e;
		} finally {
			stmt.close();
		}
		assertNotNull(exception);
		assertTrue(exception.getMessage().contains("PermissionException"));
		assertTrue(exception.getMessage().contains(
				"MODEL.testNumberModel.modify"));
		assertTrue(exception.getMessage().contains("GLOBAL.modifyAll"));

		// use philipp to insert data
		stmt = philippConn.createStatement();
		stmt.executeUpdate("INSERT INTO testNumberModel ([START], [END], NUMBER) VALUES (2, 3, '100'), (1, 5, '100')");
		stmt.close();

		// get some data as tobias - the account is allowed to query
		stmt = tobiasConn.createStatement();
		res = stmt
				.executeQuery("SELECT TRANSPOSE(TIMESERIES) OF COUNT(NUMBER) AS \"COUNT_ALIAS\" FROM testNumberModel IN [2, 5) FILTER BY NUMBER='100'");
		counter = 0;
		while (res.next()) {
			counter++;

			// check the general values
			assertEquals("COUNT_ALIAS", res.getString(1));
			assertEquals("" + (counter + 1), res.getString(3));
			assertEquals(counter + 1, res.getInt(4));

			// check the fact
			if (counter == 1 || counter == 2) {
				assertEquals(2.0, res.getDouble(2), 0.0);
			} else if (counter == 3) {
				assertEquals(1.0, res.getDouble(2), 0.0);
			} else {
				fail("Unexpected counter value '" + counter + "'");
			}
		}
		res.close();
		stmt.close();
		assertEquals(3, counter);

		// use philipp to unload the model - the account is not allowed
		stmt = philippConn.createStatement();
		try {
			exception = null;
			stmt.executeUpdate("UNLOAD testNumberModel");
		} catch (final SQLException e) {
			exception = e;
		} finally {
			stmt.close();
		}
		assertNotNull(exception);
		assertTrue(exception.getMessage().contains("PermissionException"));
		assertTrue(exception.getMessage().contains("GLOBAL.unload"));

		// use the admin to unload
		stmt = conn.createStatement();
		stmt.executeUpdate("UNLOAD testNumberModel");
		stmt.close();

		// cleanUp
		eddieConn.close();
		philippConn.close();
		tobiasConn.close();
	}
}
