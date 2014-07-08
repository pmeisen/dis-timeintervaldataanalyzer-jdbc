package net.meisen.dissertation.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import org.junit.Test;

/**
 * Tests the implementation of {@code ObjectArrayResultSet}.
 * 
 * @author pmeisen
 * 
 */
public class TestObjectArrayResultSet {

	/**
	 * Tests the creation of a regular expression based on a {@code LIKE}
	 * statement.
	 */
	@Test
	public void testCreateRegEx() {

		// the empty pattern
		assertEquals("", ObjectArrayResultSet.createRegEx(""));

		// the %-pattern
		assertEquals(".*", ObjectArrayResultSet.createRegEx("%"));

		// the one-char-pattern
		assertEquals(".", ObjectArrayResultSet.createRegEx("_"));

		// pattern without any markers
		assertEquals("\\QA simple text!\\E",
				ObjectArrayResultSet.createRegEx("A simple text!"));

		// some more complex patterns
		assertEquals("\\QThis\\E.*\\Qor nothing, I need %, don't I?\\E",
				ObjectArrayResultSet
						.createRegEx("This%or nothing, I need ?%, don't I??"));
		assertEquals(".\\Q_?%\\E.*\\Q%\\E.\\Q??\\E",
				ObjectArrayResultSet.createRegEx("_?_???%%?%_???"));

		// check some invalid quotations
		assertEquals("\\Q?\\E", ObjectArrayResultSet.createRegEx("?"));
		assertEquals(".*\\Q?\\E", ObjectArrayResultSet.createRegEx("%?"));
		assertEquals(".*\\Q?\\E", ObjectArrayResultSet.createRegEx("%??"));
		assertEquals("\\QIs that correct? \\E.*\\Q No it isn't!\\E",
				ObjectArrayResultSet
						.createRegEx("Is that correct? % No it isn't!"));
	}

	/**
	 * Tests the iteration over an {@code ObjectArrayResultSet}.
	 * 
	 * @throws SQLException
	 *             if some unexpected error occurs
	 */
	@Test
	public void testIteration() throws SQLException {
		ObjectArrayResultSet set;

		set = new ObjectArrayResultSet(new String[] { "MYCOL1", "MYCOL2" },
				new Object[][] { new Object[] { 1, "Katze" },
						new Object[] { 2, "Hund" } });
		assertTrue(set.next());
		assertEquals(1, set.getInt("MYCOL1"));
		assertEquals("Katze", set.getString("MYCOL2"));
		assertTrue(set.next());
		assertEquals(2, set.getInt("MYCOL1"));
		assertEquals("Hund", set.getString("MYCOL2"));
		assertFalse(set.next());

		set = new ObjectArrayResultSet(new String[] { "MYCOL1", "MYCOL2" },
				new Object[][] { new Object[] { 1, "Katze" },
						new Object[] { 2, "Hund" } }, "MYCOL1", "_");
		assertTrue(set.next());
		assertEquals(1, set.getInt("MYCOL1"));
		assertEquals("Katze", set.getString("MYCOL2"));
		assertTrue(set.next());
		assertEquals(2, set.getInt("MYCOL1"));
		assertEquals("Hund", set.getString("MYCOL2"));
		assertFalse(set.next());

		set = new ObjectArrayResultSet(new String[] { "MYCOL1", "MYCOL2" },
				new Object[][] { new Object[] { 1, "Katze" },
						new Object[] { 2, "Hund" } }, "MYCOL2", "Hund");

		assertTrue(set.next());
		assertEquals(2, set.getInt("MYCOL1"));
		assertEquals("Hund", set.getString("MYCOL2"));
		assertFalse(set.next());

		set = new ObjectArrayResultSet(new String[] { "MYCOL1", "MYCOL2" },
				new Object[][] { new Object[] { 1, "Katze" },
						new Object[] { 2, "Hund" } }, "MYCOL2", "H%");

		assertTrue(set.next());
		assertEquals(2, set.getInt("MYCOL1"));
		assertEquals("Hund", set.getString("MYCOL2"));
		assertFalse(set.next());

		set = new ObjectArrayResultSet(new String[] { "MYCOL1", "MYCOL2" },
				new Object[][] { new Object[] { 1, "Katze" },
						new Object[] { 2, "Hund" } }, "MYCOL2", "_");

		assertFalse(set.next());
	}
}
