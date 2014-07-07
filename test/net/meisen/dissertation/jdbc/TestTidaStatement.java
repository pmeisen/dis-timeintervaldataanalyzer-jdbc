package net.meisen.dissertation.jdbc;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests the implementation of {@code TidaStatement}.
 * 
 * @author pmeisen
 * 
 */
public class TestTidaStatement {

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
	 * Tests the replacing if no place-holder is presented.
	 * 
	 * @throws SQLException
	 *             if the creation of the statement fails
	 */
	@Test
	public void testNoPlaceholdersInSelect() throws SQLException {
		final String sql = "SELECT TIMESERIES FROM MODELID";
		final TidaStatement s = new TidaStatement(conn, sql);

		assertEquals(0, s.getPlaceholders().size());
		assertEquals(sql, s.replacePlaceholder());
	}

	/**
	 * Tests the replacing if one place-holder is presented.
	 * 
	 * @throws SQLException
	 *             if the creation of the statement fails
	 */
	@Test
	public void testSinglePlaceholderInSelect() throws SQLException {
		final String sql = "select timeSeries from model in [03.03.2014,05.03.2014) filter by singleEqual=?";
		final TidaStatement s = new TidaStatement(conn, sql);

		assertEquals(1, s.getPlaceholders().size());
		assertEquals(
				"select timeSeries from model in [03.03.2014,05.03.2014) filter by singleEqual=NULL",
				s.replacePlaceholder());
	}

	/**
	 * Tests the replacing if multiple place-holder is presented.
	 * 
	 * @throws SQLException
	 *             if the creation of the statement fails
	 */
	@Test
	public void testMultiplePlaceholderInSelect() throws SQLException {
		final String sql = "select timeseries from model in [03.03.2014,05.03.2014) filter by NOT HALLO=? AND !HELLO=?";
		final TidaStatement s = new TidaStatement(conn, sql);

		assertEquals(2, s.getPlaceholders().size());
		assertEquals(
				"select timeseries from model in [03.03.2014,05.03.2014) filter by NOT HALLO=NULL AND !HELLO=NULL",
				s.replacePlaceholder());
	}

	/**
	 * Tests the replacing if no place-holder is presented.
	 * 
	 * @throws SQLException
	 *             if the creation of the statement fails
	 */
	@Test
	public void testNoPlaceholdersInInsert() throws SQLException {
		final String sql = "INSERT INTO MyModel ([START], [END-], NAME, PRIORITY) VALUES (20.01.1981, 20.02.2004, 'Philipp', '1')";
		final TidaStatement s = new TidaStatement(conn, sql);

		assertEquals(0, s.getPlaceholders().size());
		assertEquals(sql, s.replacePlaceholder());
	}

	/**
	 * Tests the replacing if one place-holder is presented.
	 * 
	 * @throws SQLException
	 *             if the creation of the statement fails
	 */
	@Test
	public void testSinglePlaceholderInInsert() throws SQLException {
		final String sql = "INSERT INTO MyModel ([START], [END-], NAME) VALUES (20.01.1981, 20.02.2004, ?)";
		final TidaStatement s = new TidaStatement(conn, sql);

		assertEquals(1, s.getPlaceholders().size());
		assertEquals(
				"INSERT INTO MyModel ([START], [END-], NAME) VALUES (20.01.1981, 20.02.2004, NULL)",
				s.replacePlaceholder());
	}

	/**
	 * Tests the replacing if multiple place-holder is presented.
	 * 
	 * @throws SQLException
	 *             if the creation of the statement fails
	 */
	@Test
	public void testMultiplePlaceholderInInsert() throws SQLException {
		final String sql = "INSERT INTO MyModel ([START], [END-], NAME, PRIORITY) VALUES (20.01.1981, 20.02.2004, ?, ?)";
		final TidaStatement s = new TidaStatement(conn, sql);

		assertEquals(2, s.getPlaceholders().size());
		assertEquals(
				"INSERT INTO MyModel ([START], [END-], NAME, PRIORITY) VALUES (20.01.1981, 20.02.2004, NULL, NULL)",
				s.replacePlaceholder());
	}

	/**
	 * Tests the replacement of place-holders when their are quotes and comments
	 * etc.
	 * 
	 * @throws SQLException
	 *             if the creation of the statement fails
	 */
	@Test
	public void testQuotation() throws SQLException {
		String sql;
		TidaStatement s;

		sql = "select timeSeries from model in [03.03.2014,05.03.2014) filter by singleEqual='MyValue?'";
		s = new TidaStatement(conn, sql);
		assertEquals(0, s.getPlaceholders().size());
		assertEquals(sql, s.replacePlaceholder());

		sql = "select timeSeries from model in [03.03.2014,05.03.2014) filter by singleEqual='MyValueWith\\'?'";
		s = new TidaStatement(conn, sql);
		assertEquals(0, s.getPlaceholders().size());
		assertEquals(sql, s.replacePlaceholder());

		sql = "select timeSeries from model in [03.03.2014,05.03.2014) filter by singleEqual='MyValueWith\\'?\\''";
		s = new TidaStatement(conn, sql);
		assertEquals(0, s.getPlaceholders().size());
		assertEquals(sql, s.replacePlaceholder());

		sql = "select timeSeries from model in [03.03.2014,05.03.2014) filter by singleEqual='MyValueWith\\'\\'?\\''";
		s = new TidaStatement(conn, sql);
		assertEquals(0, s.getPlaceholders().size());
		assertEquals(sql, s.replacePlaceholder());

		sql = "select timeSeries from model in [03.03.2014,05.03.2014) filter by NOT HALLO=? AND !HELLO='VALUE' AND HI=? OR WELCOME='?'";
		s = new TidaStatement(conn, sql);
		assertEquals(2, s.getPlaceholders().size());
		assertEquals(
				"select timeSeries from model in [03.03.2014,05.03.2014) filter by NOT HALLO=NULL AND !HELLO='VALUE' AND HI=NULL OR WELCOME='?'",
				s.replacePlaceholder());

		sql = "select timeSeries from model in [03.03.2014,05.03.2014) filter by NOT HALLO=? AND !HELLO='VALUE' AND HI=   ?   OR WELCOME='?'";
		s = new TidaStatement(conn, sql);
		assertEquals(2, s.getPlaceholders().size());
		assertEquals(
				"select timeSeries from model in [03.03.2014,05.03.2014) filter by NOT HALLO=NULL AND !HELLO='VALUE' AND HI=   NULL   OR WELCOME='?'",
				s.replacePlaceholder());

		sql = "select timeSeries from model in [03.03.2014,05.03.2014) filter by NOT HALLO=? AND !HELLO='VALUE?VALUE' AND HI='?' OR WELCOME='?NOT'";
		s = new TidaStatement(conn, sql);
		assertEquals(1, s.getPlaceholders().size());

		sql = "select timeSeries from model in [03.03.2014,05.03.2014) filter by NOT HALLO='The Word is \\'?\\''";
		s = new TidaStatement(conn, sql);
		assertEquals(0, s.getPlaceholders().size());
		assertEquals(sql, s.replacePlaceholder());

		sql = "select timeSeries from model in [03.03.2014,05.03.2014) filter by NOT HALLO='The \\\\ Word is \\'?\\''";
		s = new TidaStatement(conn, sql);
		assertEquals(0, s.getPlaceholders().size());
		assertEquals(sql, s.replacePlaceholder());
	}

	/**
	 * Tests the usage of date replacements.
	 * 
	 * @throws SQLException
	 *             if some unexpected error occurs
	 */
	@Test
	public void testDateReplacement() throws SQLException {

		final String sql = "INSERT INTO MyModel ([START], [END-], NAME, PRIORITY) VALUES (20.01.1981, 20.02.2004, ?, ?)";
		final TidaStatement s = new TidaStatement(conn, sql);
		s.setTimestamp(1, new Timestamp(0));

		final Calendar cal = Calendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		s.setTimestamp(2, new Timestamp(0), cal);

		assertEquals(2, s.getPlaceholders().size());
		assertEquals(
				"INSERT INTO MyModel ([START], [END-], NAME, PRIORITY) VALUES (20.01.1981, 20.02.2004, '01.01.1970 01:00:00', '01.01.1970 00:00:00')",
				s.replacePlaceholder());
	}

	/**
	 * Tests the replacement of numbers.
	 * 
	 * @throws SQLException
	 *             if some unexpected error occurs
	 */
	@Test
	public void testNumberReplacement() throws SQLException {

		final String sql = "INSERT INTO MyModel (VAL1, VAL2, VAL3, VAL4, VAL5, VAL6, VAL7) VALUES (?, ?, ?, ?, ?, ?, ?)";
		final TidaStatement s = new TidaStatement(conn, sql);
		s.setByte(1, Byte.MAX_VALUE);
		s.setShort(2, Short.MIN_VALUE);
		s.setInt(3, Integer.MAX_VALUE);
		s.setLong(4, Long.MIN_VALUE);
		s.setFloat(5, (float) 123.123);
		s.setDouble(6, 21324.390812);
		s.setBigDecimal(7, new BigDecimal("700.13"));

		assertEquals(7, s.getPlaceholders().size());
		assertEquals(
				"INSERT INTO MyModel (VAL1, VAL2, VAL3, VAL4, VAL5, VAL6, VAL7) VALUES ('"
						+ Byte.MAX_VALUE + "', '" + Short.MIN_VALUE + "', '"
						+ Integer.MAX_VALUE + "', '" + Long.MIN_VALUE
						+ "', '123.123', '21324.390812', '700.13')",
				s.replacePlaceholder());
	}
}
