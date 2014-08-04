package net.meisen.dissertation.jdbc;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;

/**
 * An implementation based on {@code TestBaseForConnections} which enables a
 * connection.
 * 
 * @author pmeisen
 * 
 */
public class TestBaseWithConnection extends TestBaseForConnections {
	/**
	 * The connection to the server
	 */
	protected TidaConnection conn;

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
	 * Cleans up and closes the connection.
	 * 
	 * @throws SQLException
	 *             if the connection could not be closed
	 */
	@After
	public void closeConn() throws SQLException {
		if (this.conn != null) {
			this.conn.close();
		}
	}
}
