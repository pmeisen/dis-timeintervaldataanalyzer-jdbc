package net.meisen.dissertation.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import net.meisen.dissertation.exceptions.QueryEvaluationException;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests the implementation of a {@code ResultSet}.
 * 
 * @author pmeisen
 * 
 */
public class TestTidaResultSet extends TestBaseForConnections {

	/**
	 * Tests the execution of a result-set used for a manipulation.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	@Test
	public void testResultSetExecuteUpdate() throws SQLException {
		final Connection conn = DriverManager
				.getConnection("jdbc:tida://localhost:7001");
		final Statement stmt = conn.createStatement();

		// execute tells us what type we can expect
		assertEquals(
				0,
				stmt.executeUpdate("LOAD FROM 'classpath:/net/meisen/dissertation/model/testNumberModel.xml'"));

		// cleanUp
		stmt.close();
		conn.close();
	}

	/**
	 * Tests the execution of an update result-set.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	@Test
	public void testUpdateResultSet() throws SQLException {
		final Connection conn = DriverManager
				.getConnection("jdbc:tida://localhost:7001");
		assertTrue(conn instanceof TidaConnection);
		final TidaConnection tConn = (TidaConnection) conn;

		// get the one and only manager
		final ProtocolManager manager = tConn.getManager();

		// create a statement
		final Statement stmt = conn.createStatement();
		assertTrue(stmt instanceof TidaStatement);
		final TidaStatement tStmt = (TidaStatement) stmt;
		assertTrue(manager == tStmt.getManager());
		assertEquals(0, manager.sizeOfProtocols(tStmt));

		assertFalse(stmt
				.execute("LOAD FROM 'classpath:/net/meisen/dissertation/model/testNumberModel.xml'"));

		// cleanUp
		stmt.close();
		conn.close();
	}

	/**
	 * Tests the closing of a result-set and the correct handling of the
	 * manager.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	@Test
	public void testResultSetCloseOfResultSet() throws SQLException {
		final Connection conn = DriverManager
				.getConnection("jdbc:tida://localhost:7001");
		assertTrue(conn instanceof TidaConnection);
		final TidaConnection tConn = (TidaConnection) conn;

		// get the one and only manager
		final ProtocolManager manager = tConn.getManager();

		// create a statement
		final Statement stmt = conn.createStatement();
		assertTrue(stmt instanceof TidaStatement);
		final TidaStatement tStmt = (TidaStatement) stmt;
		assertTrue(manager == tStmt.getManager());
		assertEquals(0, manager.sizeOfProtocols(tStmt));

		// create a model we can use
		stmt.execute("LOAD FROM 'classpath:/net/meisen/dissertation/model/testNumberModel.xml'");

		// create some resultSets
		final ResultSet rs1 = stmt
				.executeQuery("SELECT TIMESERIES FROM testNumberModel");
		final ResultSet rs2 = stmt
				.executeQuery("SELECT TIMESERIES FROM testNumberModel");
		assertTrue(rs1 instanceof TidaResultSet);
		assertTrue(rs2 instanceof TidaResultSet);
		final TidaResultSet trs1 = (TidaResultSet) rs1;
		final TidaResultSet trs2 = (TidaResultSet) rs2;
		assertTrue(manager == trs1.getManager());
		assertTrue(manager == trs2.getManager());

		// check the manager
		assertEquals(2, manager.sizeOfOwners());
		assertEquals(1, manager.sizeOfScopes());
		assertEquals(2, manager.sizeOfProtocols(tStmt));
		assertEquals(0, manager.sizeOfProtocols(trs1));
		assertEquals(0, manager.sizeOfProtocols(trs2));
		assertFalse(manager.isOwner(trs1));
		assertTrue(manager.isOwner(trs2));
		assertTrue(manager.isOwner(tStmt));

		// close the ResultSets
		rs1.close();
		assertTrue(rs1.isClosed());
		rs2.close();
		assertTrue(rs2.isClosed());

		// the connection of the statement is still available
		assertEquals(1, manager.sizeOfOwners());
		assertEquals(1, manager.sizeOfScopes());
		assertEquals(1, manager.sizeOfProtocols(tStmt));
		assertEquals(0, manager.sizeOfProtocols(trs1));
		assertEquals(0, manager.sizeOfProtocols(trs2));
		assertFalse(manager.isOwner(trs1));
		assertFalse(manager.isOwner(trs2));
		assertTrue(manager.isOwner(tStmt));

		// create a new ResultSet
		final ResultSet rs3 = stmt
				.executeQuery("SELECT TRANSPOSE(TIMESERIES) FROM testNumberModel");

		assertTrue(rs3 instanceof TidaResultSet);
		final TidaResultSet trs3 = (TidaResultSet) rs3;
		assertTrue(manager == trs3.getManager());

		// check the manager
		assertEquals(1, manager.sizeOfOwners());
		assertEquals(1, manager.sizeOfScopes());
		assertEquals(1, manager.sizeOfProtocols(tStmt));
		assertEquals(0, manager.sizeOfProtocols(trs1));
		assertEquals(0, manager.sizeOfProtocols(trs2));
		assertEquals(0, manager.sizeOfProtocols(trs3));
		assertFalse(manager.isOwner(trs3));
		assertTrue(manager.isOwner(tStmt));

		// close the new ResultSet
		rs3.close();
		assertTrue(rs3.isClosed());

		// check the manager once more
		assertEquals(1, manager.sizeOfOwners());
		assertEquals(1, manager.sizeOfScopes());
		assertEquals(1, manager.sizeOfProtocols(tStmt));
		assertEquals(0, manager.sizeOfProtocols(trs1));
		assertEquals(0, manager.sizeOfProtocols(trs2));
		assertEquals(0, manager.sizeOfProtocols(trs3));
		assertFalse(manager.isOwner(trs3));
		assertTrue(manager.isOwner(tStmt));

		// close the statement
		stmt.close();
		assertTrue(stmt.isClosed());

		// check the manager
		assertEquals(0, manager.sizeOfOwners());
		assertEquals(0, manager.sizeOfScopes());
		assertEquals(0, manager.sizeOfProtocols(tStmt));
		assertEquals(0, manager.sizeOfProtocols(trs1));
		assertEquals(0, manager.sizeOfProtocols(trs2));
		assertEquals(0, manager.sizeOfProtocols(trs3));
		assertFalse(manager.isClosed());

		// finally close the connection
		conn.close();
		assertTrue(conn.isClosed());

		// check the manager
		assertTrue(manager.isClosed());
	}

	/**
	 * Tests the usage of time-outs.
	 * 
	 * @throws SQLException
	 *             if an unexpected error occurs
	 */
	@Test
	public void testResultSetTimeout() throws SQLException {
		final Connection conn = DriverManager
				.getConnection("jdbc:tida://localhost:7001");
		assertTrue(conn instanceof TidaConnection);
		final TidaConnection tConn = (TidaConnection) conn;

		// get the one and only manager
		final ProtocolManager manager = tConn.getManager();

		// create a statement
		final Statement stmt = conn.createStatement();
		assertTrue(stmt instanceof TidaStatement);
		final TidaStatement tStmt = (TidaStatement) stmt;
		tStmt.setQueryTimeoutInMs(1);

		// create a model we can use
		Exception exception = null;
		try {
			stmt.execute("LOAD FROM 'classpath:/net/meisen/dissertation/model/testNumberModel.xml'");
		} catch (final SQLException e) {
			exception = e;
		}
		assertNotNull(exception);
		assertTrue(exception.getMessage().contains(
				"exceeded the defined time-out"));

		// increase the timeout again
		tStmt.setQueryTimeoutInMs(0);
		stmt.execute("UNLOAD testNumberModel");

		// get some more Results
		final ResultSet rs1 = stmt
				.executeQuery("SELECT TIMESERIES FROM testNumberModel");
		final ResultSet rs2 = stmt
				.executeQuery("SELECT TRANSPOSE(TIMESERIES) FROM testNumberModel");
		assertTrue(rs1 instanceof TidaResultSet);
		assertTrue(rs2 instanceof TidaResultSet);
		final TidaResultSet trs1 = (TidaResultSet) rs1;
		final TidaResultSet trs2 = (TidaResultSet) rs2;

		// check the manager, it should use the statement's connection
		assertEquals(2, manager.sizeOfOwners());
		assertEquals(1, manager.sizeOfScopes());
		assertEquals(2, manager.sizeOfProtocols(tStmt));
		assertEquals(0, manager.sizeOfProtocols(trs1));
		assertEquals(0, manager.sizeOfProtocols(trs2));
		assertFalse(manager.isOwner(trs1));
		assertTrue(manager.isOwner(trs2));
		assertTrue(manager.isOwner(tStmt));

		// check the exception
		exception = null;
		try {
			rs1.next();
		} catch (final SQLException e) {
			exception = e;
		}
		assertNotNull(exception);
		assertTrue(
				exception.getMessage(),
				exception.getMessage().contains(
						"[" + QueryEvaluationException.class.getSimpleName()
								+ "]"));
		assertTrue(exception.getMessage(),
				exception.getMessage().contains("'testNumberModel"));

		// close the statement and the connection
		stmt.close();
		conn.close();
	}

	/**
	 * Tests the value retrieval of an update statement.
	 * 
	 * @throws SQLException
	 *             if an unexpected error occurs
	 */
	@Test
	public void testExecuteUpdate() throws SQLException {
		final Connection conn = DriverManager
				.getConnection("jdbc:tida://localhost:7001");
		final Statement stmt = conn.createStatement();
		stmt.executeUpdate("LOAD FROM 'classpath:/net/meisen/dissertation/model/testNumberModel.xml'");

		// execute INSERT using update
		final int res = stmt
				.executeUpdate("INSERT INTO testNumberModel ([START], [END], NUMBER) VALUES (2, 3, '100')");
		assertEquals(1, res);
		assertNull(stmt.getResultSet());
		assertEquals(-1, stmt.getUpdateCount());

		// execute INSERT using just execute
		assertFalse(stmt
				.execute("INSERT INTO testNumberModel ([START], [END], NUMBER) VALUES (1, 3, '200'), (5, 5, '100')"));
		assertEquals(2, stmt.getUpdateCount());
		assertEquals(-1, stmt.getUpdateCount());
		assertNull(stmt.getResultSet());

		// close everything
		conn.close();
	}

	@Test
	@Ignore
	public void testExecuteSelect() throws SQLException {
		final Connection conn = DriverManager
				.getConnection("jdbc:tida://localhost:7001");
		final Statement stmt = conn.createStatement();
		stmt.executeUpdate("LOAD FROM 'classpath:/net/meisen/dissertation/model/testNumberModel.xml'");

		// execute INSERT using update
		stmt.executeUpdate("INSERT INTO testNumberModel ([START], [END], NUMBER) VALUES (2, 3, '100')");

		// execute INSERT using just execute
		final ResultSet res = stmt
				.executeQuery("SELECT TIMESERIES FROM testNumberModel");
		assertNotNull(res);
		assertNull(stmt.getResultSet());
		assertEquals(-1, stmt.getUpdateCount());
		
		System.out.println(Arrays.asList(((TidaResultSet) res).getHeaderTypes()));

		final ResultSetMetaData metaData = res.getMetaData();
		System.out.println(metaData.getColumnCount());

		// close everything
		conn.close();
	}
}
