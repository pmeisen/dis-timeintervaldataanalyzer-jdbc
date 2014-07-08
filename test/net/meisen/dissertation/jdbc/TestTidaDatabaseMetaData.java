package net.meisen.dissertation.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

public class TestTidaDatabaseMetaData {

	private TidaConnection conn;

	/**
	 * Helper method to create a connection
	 * 
	 * @throws SQLException
	 *             if the connection could not be created
	 */
	@Before
	public void createConn() throws SQLException {
		this.conn = (TidaConnection) DriverManager
				.getConnection("jdbc:tida://localhost:6666");
	}

	/**
	 * Tests the implementation of {@link TidaDatabaseMetaData#getCatalogs()}.
	 * 
	 * @throws SQLException
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
	 * @throws SQLException 
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
}
