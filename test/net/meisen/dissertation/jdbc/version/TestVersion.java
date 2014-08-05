package net.meisen.dissertation.jdbc.version;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests the implementation of the {@code Version}.
 * 
 * @author pmeisen
 * 
 */
public class TestVersion {

	/**
	 * Tests the implementation of the {@link Version#parse(String)}.
	 */
	@Test
	public void testParsing() {
		Version version;

		version = Version.parse("0.10.2");
		assertEquals(0, version.getMajorAsInt());
		assertEquals(10, version.getMinorAsInt());
		assertEquals(2, version.getBuildAsInt());
		assertEquals(0, version.getRevisionAsInt());
		assertFalse(version.isSnapshot());

		version = Version.parse("10.0.0.2");
		assertEquals(10, version.getMajorAsInt());
		assertEquals(0, version.getMinorAsInt());
		assertEquals(0, version.getBuildAsInt());
		assertEquals(2, version.getRevisionAsInt());
		assertFalse(version.isSnapshot());

		version = Version.parse("10.0.0.2-SNAPSHOT");
		assertEquals(10, version.getMajorAsInt());
		assertEquals(0, version.getMinorAsInt());
		assertEquals(0, version.getBuildAsInt());
		assertEquals(2, version.getRevisionAsInt());
		assertTrue(version.isSnapshot());

		version = Version.parse("TRUNK-SNAPSHOT");
		assertEquals(0, version.getMajorAsInt());
		assertEquals(0, version.getMinorAsInt());
		assertEquals(0, version.getBuildAsInt());
		assertEquals(0, version.getRevisionAsInt());
		assertEquals("TRUNK", version.getPrefix());
		assertTrue(version.isSnapshot());
	}
}
