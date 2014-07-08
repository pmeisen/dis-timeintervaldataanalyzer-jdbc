package net.meisen.dissertation.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests the implementation of the {@code TidaDatabaseMetaData}.
 * 
 * @author pmeisen
 * 
 */
public class TestTidaDatabaseMetaData extends TestBaseForConnections {

	private TidaConnection conn;

	/**
	 * Helper method to create a connection
	 * 
	 * @throws SQLException
	 *             if the connection could not be created
	 */
	@Before
	public void createConn() throws SQLException {
		this.conn = (TidaConnection) DriverManager.getConnection(getJdbc());
	}

	/**
	 * Tests the implementation of {@link TidaDatabaseMetaData#getCatalogs()}.
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
	 * Tests the implementation of {@link TidaDatabaseMetaData#getSchemas()} and
	 * {@link TidaDatabaseMetaData#getSchemas(String, String)}.
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
	 * {@link TidaDatabaseMetaData#getTables(String, String, String, String[])}.
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
