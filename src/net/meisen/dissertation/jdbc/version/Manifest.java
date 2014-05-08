package net.meisen.dissertation.jdbc.version;

import java.io.IOException;

/**
 * Helper class concerning the manifest
 * 
 * @author pmeisen
 * 
 */
public class Manifest {

	/**
	 * Creates the <code>ManifestInfo</code> for the passed <code>Class</code>,
	 * only and only if the passed <code>Class</code> is wrapped in a jar-file
	 * and if it is not <code>null</code>
	 * 
	 * @param clazz
	 *            the <code>Class</code> to read the <code>ManifestInfo</code>
	 *            of
	 * 
	 * @return the <code>ManifestInfo</code> of the specified <code>Class</code>
	 *         , or <code>null</code> if the specified <code>Class</code> was
	 *         <code>null</code> or not wrapped in a jar
	 * 
	 * @throws IOException
	 *             if the manifest cannot be read
	 */
	public static ManifestInfo getManifestInfo(final Class<?> clazz)
			throws IOException {

		try {
			return new ManifestInfo(clazz);
		} catch (final IllegalArgumentException e) {
			return null;
		}
	}
}
