package net.meisen.dissertation.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;

import net.meisen.dissertation.model.auth.permissions.Permission;
import net.meisen.dissertation.model.auth.permissions.PermissionLevel;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Tests the implementation of the {@code TidaDatabaseMetaData}.
 * 
 * @author pmeisen
 * 
 */
public class TestTidaDatabaseMetaData {

	/**
	 * Test some simple implementations considering the
	 * {@code TidaDatabaseMetaData}.
	 * 
	 * @author pmeisen
	 * 
	 */
	public static class TestSimple extends TestBaseWithConnection {

		@Override
		public boolean createTemporaryFolder() {
			return false;
		}

		/**
		 * Tests the implementation of
		 * {@link TidaDatabaseMetaData#getCatalogs()}.
		 * 
		 * @throws SQLException
		 *             if an unexpected exception occurs
		 */
		@Test
		public void testGetCatalogs() throws SQLException {
			final TidaDatabaseMetaData meta = new TidaDatabaseMetaData(conn);
			final ResultSet res = meta.getCatalogs();

			assertTrue(res.next());
			assertEquals("", res.getString("TABLE_CAT"));
			assertEquals("", res.getString(1));

			assertFalse(res.next());
		}

		/**
		 * Tests the implementation of {@link TidaDatabaseMetaData#getSchemas()}
		 * and {@link TidaDatabaseMetaData#getSchemas(String, String)}.
		 * 
		 * @throws SQLException
		 *             if an unexpected exception occurs
		 */
		@Test
		public void testGetSchemas() throws SQLException {
			final TidaDatabaseMetaData meta = new TidaDatabaseMetaData(conn);
			ResultSet res;

			res = meta.getSchemas();
			assertTrue(res.next());
			assertEquals("", res.getString("TABLE_SCHEM"));
			assertEquals("", res.getString(1));
			assertEquals("", res.getString("TABLE_CATALOG"));
			assertEquals("", res.getString(2));
			assertFalse(res.next());

			res = meta.getSchemas(null, null);
			assertTrue(res.next());
			assertEquals("", res.getString("TABLE_SCHEM"));
			assertEquals("", res.getString(1));
			assertEquals("", res.getString("TABLE_CATALOG"));
			assertEquals("", res.getString(2));
			assertFalse(res.next());

			res = meta.getSchemas("", "");
			assertTrue(res.next());
			assertEquals("", res.getString("TABLE_SCHEM"));
			assertEquals("", res.getString(1));
			assertEquals("", res.getString("TABLE_CATALOG"));
			assertEquals("", res.getString(2));
			assertFalse(res.next());

			res = meta.getSchemas("", "%");
			assertTrue(res.next());
			assertEquals("", res.getString("TABLE_SCHEM"));
			assertEquals("", res.getString(1));
			assertEquals("", res.getString("TABLE_CATALOG"));
			assertEquals("", res.getString(2));
			assertFalse(res.next());

			res = meta.getSchemas("", "_");
			assertFalse(res.next());
		}

		/**
		 * Tests the implementation of
		 * {@link TidaDatabaseMetaData#getTables(String, String, String, String[])}
		 * .
		 * 
		 * @throws SQLException
		 *             if an unexpected exception occurs
		 */
		@Test
		public void testGetTables() throws SQLException {
			final TidaDatabaseMetaData meta = new TidaDatabaseMetaData(conn);
			ResultSet res;

			// check an empty server
			res = meta.getTables(null, null, null, null);
			assertFalse(res.next());

			// load something on server side
			final TidaStatement stmt = conn.createStatement();
			stmt.executeUpdate("LOAD FROM 'classpath:/net/meisen/dissertation/model/testNumberModel.xml'");
			stmt.close();

			res = meta.getTables(null, null, null, null);
			assertTrue(res.next());
			assertEquals("", res.getString(1));
			assertEquals("", res.getString(2));
			assertEquals("testNumberModel", res.getString(3));
			assertEquals("TABLE", res.getString(4));
			assertFalse(res.next());

			res = meta.getTables(null, null, null, new String[] { "TABLE" });
			assertTrue(res.next());
			assertEquals("", res.getString(1));
			assertEquals("", res.getString(2));
			assertEquals("testNumberModel", res.getString(3));
			assertEquals("TABLE", res.getString(4));
			assertFalse(res.next());

			res = meta.getTables(null, null, null, new String[] { "VIEW" });
			assertFalse(res.next());
		}

		/**
		 * Test the retrieval of the major version.
		 */
		@Test
		public void testMajorVersionRetrieval() {
			final TidaDatabaseMetaData meta = new TidaDatabaseMetaData(conn);
			assertEquals(0, meta.extractMajorVersion("0.10.1"));
			assertEquals(-1, meta.extractMajorVersion(""));
			assertEquals(-1, meta.extractMajorVersion("TRUNK-SNAPSHOT"));
			assertEquals(-1, meta.extractMajorVersion("TRUNK"));
			assertEquals(5, meta.extractMajorVersion("5.10.2.1"));
		}

		/**
		 * Test the retrieval of the minor version.
		 */
		@Test
		public void testMinorVersionRetrieval() {
			final TidaDatabaseMetaData meta = new TidaDatabaseMetaData(conn);
			assertEquals(10, meta.extractMinorVersion("0.10.1"));
			assertEquals(-1, meta.extractMinorVersion(""));
			assertEquals(-1, meta.extractMinorVersion("TRUNK-SNAPSHOT"));
			assertEquals(-1, meta.extractMinorVersion("TRUNK"));
			assertEquals(20, meta.extractMinorVersion("5.20.2.1"));
		}

		/**
		 * Tests the retrieval of the {@code DriverProperties}.
		 * 
		 * @throws SQLException
		 *             if an unexpected error occurs
		 */
		@Test
		public void testGetClientInfoProperties() throws SQLException {
			final TidaDatabaseMetaData meta = new TidaDatabaseMetaData(conn);
			ResultSet res;

			// get the expected properties
			final DriverProperties expected = conn.getDriverProperties();

			// check an empty server
			res = meta.getClientInfoProperties();
			while (res.next()) {
				assertEquals(res.getObject(3), expected.get(res.getString(1)));
			}
		}
	}

	/**
	 * Tests the implementation using a database auth-manager.
	 * 
	 * @author pmeisen
	 * 
	 */
	public static class TestWithAuthManager extends TestBaseWithConnection {

		@Override
		public String getConfig() {
			return "net/meisen/dissertation/model/testShiroAuthConfig.xml";
		}

		@Override
		public String getJdbc() {
			return "jdbc:tida://admin:password@localhost:" + getPort();
		}

		/**
		 * Tests the implementation of
		 * {@link TidaDatabaseMetaData#getTablePrivileges(String, String, String)}
		 * .
		 * 
		 * @throws SQLException
		 *             if the meta information could not be retrieved
		 */
		@Test
		public void testGetTablePrivileges() throws SQLException {
			final TidaDatabaseMetaData meta = new TidaDatabaseMetaData(conn);
			TidaStatement stmt;
			ResultSet res;
			int counter, adminCounter;

			// check an empty server
			res = meta.getTablePrivileges(null, null, null);
			counter = 0;
			while (res.next()) {
				assertEquals("admin", res.getString("GRANTEE"));
				counter++;

				if ("*".equals(res.getString("TABLE_NAME"))) {
					assertTrue(res.getString("PRIVILEGE").startsWith("MODEL"));
				} else if ("".equals(res.getString("TABLE_NAME"))) {
					assertTrue(res.getString("PRIVILEGE").startsWith("GLOBAL"));
				}
			}
			assertEquals(Permission.values().length, counter);

			// load something on server side
			stmt = conn.createStatement();
			stmt.executeUpdate("LOAD FROM 'classpath:/net/meisen/dissertation/model/testNumberModel.xml'");
			stmt.close();

			// add a user
			stmt = conn.createStatement();
			stmt.executeUpdate("ADD USER 'philipp' WITH PASSWORD 'password' WITH PERMISSIONS 'GLOBAL.connectTSQL', 'MODEL.testNumberModel.query'");
			stmt.close();

			// check the privs of no model
			res = meta.getTablePrivileges(null, null, "");
			counter = 0;
			adminCounter = 0;
			while (res.next()) {
				assertEquals("", res.getString("TABLE_NAME"));

				if ("admin".equals(res.getString("GRANTEE"))) {
					adminCounter++;
				} else if ("philipp".equals(res.getString("GRANTEE"))) {
					assertEquals("GLOBAL.connectTSQL",
							res.getString("PRIVILEGE"));
				}

				counter++;
			}
			assertEquals(Permission.values(PermissionLevel.GLOBAL).length,
					adminCounter);
			assertEquals(counter, adminCounter + 1);

			// check the privs of a specific model
			res = meta.getTablePrivileges(null, null, "testNumberModel");
			counter = 0;
			adminCounter = 0;
			while (res.next()) {
				assertEquals("testNumberModel", res.getString("TABLE_NAME"));

				if ("admin".equals(res.getString("GRANTEE"))) {
					assertTrue(res.getString("PRIVILEGE").startsWith(
							"MODEL.testNumberModel."));
					adminCounter++;
				} else if ("philipp".equals(res.getString("GRANTEE"))) {
					assertEquals("MODEL.testNumberModel.query",
							res.getString("PRIVILEGE"));
				}

				counter++;
			}
			assertEquals(Permission.values(PermissionLevel.MODEL).length,
					adminCounter);
			assertEquals(counter, adminCounter + 1);
		}
	}

	/**
	 * A suite combining the different tests.
	 * 
	 * @author pmeisen
	 */
	@RunWith(Suite.class)
	@Suite.SuiteClasses({ TestSimple.class, TestWithAuthManager.class })
	public static class TestTidaDatabaseMetaDataSuite {
		// just the suite with all the tests defined here
	}
}
