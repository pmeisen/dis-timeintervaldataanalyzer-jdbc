package net.meisen.dissertation.jdbc.version;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Class which provides information about a manifest
 * 
 * @author pmeisen
 */
public class ManifestInfo {

	/**
	 * Typical name used to provide the <code>Manifest's</code> version
	 */
	public final static String MANIFEST_VERSION = "Manifest-Version";
	/**
	 * Typical name used to provide the used version of <code>Ant</code>
	 */
	public final static String ANT_VERSION = "Ant-Version";
	/**
	 * Typical name used to provide the created by information
	 */
	public final static String CREATED_BY = "Created-By";
	/**
	 * Typical name used to provide the built by information
	 */
	public final static String BUILT_BY = "Built-By";
	/**
	 * Typical name used to provide the built date
	 */
	public final static String BUILT_DATE = "Built-Date";
	/**
	 * Typical name used to provide the implementation's vendor
	 */
	public final static String IMPL_VENDOR = "Implementation-Vendor";
	/**
	 * Typical name used to provide the implementation's title
	 */
	public final static String IMPL_TITLE = "Implementation-Title";
	/**
	 * Typical name used to provide the implementation's version
	 */
	public final static String IMPL_VERSION = "Implementation-Version";
	/**
	 * Typical name used to provide the SVN revision
	 */
	public final static String SVN_REVISION = "SVN-Revision";

	/**
	 * The <code>Map</code> with all the read attributes
	 */
	protected final Map<String, String> values = new HashMap<String, String>();

	/**
	 * The <code>ManifestInfo</code> default constructor
	 * 
	 * @param manifest
	 *            the <code>Manifest</code> to be read
	 * 
	 * @see net.meisen.dissertation.jdbc.version.Manifest#getManifestInfo(Class)
	 */
	public ManifestInfo(final Manifest manifest) {
		readManifest(manifest);
	}

	/**
	 * Creates the <code>ManifestInfo</code> for the specified
	 * <code>Class</code>
	 * 
	 * @param clazz
	 *            the class to create the <code>ManifestInfo</code> for
	 * @throws IOException
	 *             if the <code>Manifest</code> cannot be read
	 * @throws IllegalArgumentException
	 *             if the passed <code>clazz</code> was <code>null</code> or if
	 *             it was not wrapped within a jar
	 */
	public ManifestInfo(final Class<?> clazz) throws IOException,
			IllegalArgumentException {
		if (clazz == null) {
			throw new IllegalArgumentException(
					"The passed Class cannot be null.");
		} else {
			final String className = clazz.getSimpleName() + ".class";
			final String classPath = clazz.getResource(className).toString();
			if (!classPath.startsWith("jar")) {
				throw new IllegalArgumentException(
						"The passed Class must be part of a jar-file");
			} else {
				final String manifestPath = classPath.substring(0,
						classPath.lastIndexOf("!") + 1)
						+ "/META-INF/MANIFEST.MF";
				final java.util.jar.Manifest javaManifest = new Manifest(
						new URL(manifestPath).openStream());

				readManifest(javaManifest);
			}
		}
	}

	/**
	 * Reads the <code>Manifest</code> and applies the properties to the one of
	 * the <code>ManifestInfo</code>
	 * 
	 * @param manifest
	 *            the <code>Manifest</code> to be read
	 */
	protected void readManifest(final Manifest manifest) {
		final Attributes attr = manifest.getMainAttributes();

		for (Map.Entry<Object, Object> entry : attr.entrySet()) {
			final Object key = entry.getKey();
			final Object value = entry.getValue();

			if (key != null && value != null) {
				values.put(key.toString(), value.toString());
			} else if (key != null) {
				values.put(key.toString(), null);
			}
		}
	}

	/**
	 * Get a specific information from the <code>Manifest</code>
	 * 
	 * @param info
	 *            the identifier of the information to be read
	 * @return the information, <code>null</code> if no information was attached
	 */
	public String getInfo(final String info) {
		return values.get(info);
	}

	/**
	 * Get the implementation's title stored in the <code>Manifest</code>
	 * 
	 * @return the implementation's title, or <code>null</code> if not specified
	 */
	public String getImplementationTitle() {
		return getInfo(IMPL_TITLE);
	}

	/**
	 * Get the implementation's version stored in the <code>Manifest</code>
	 * 
	 * @return the implementation's version, or <code>null</code> if not
	 *         specified
	 */
	public String getImplementationVersion() {
		return getInfo(IMPL_VERSION);
	}

	/**
	 * Get the <code>Manifest's</code> version as specified in the
	 * <code>Manifest</code>
	 * 
	 * @return the <code>Manifest's</code> version, or <code>null</code> if not
	 *         specified
	 */
	public String getManifestVersion() {
		return getInfo(MANIFEST_VERSION);
	}

	/**
	 * Get the <code>Ant's</code> version as specified in the
	 * <code>Manifest</code>
	 * 
	 * @return the <code>Ant's</code> version, or <code>null</code> if not
	 *         specified
	 */
	public String getAntVersion() {
		return getInfo(ANT_VERSION);
	}

	/**
	 * Get the <code>Created By</code> information as specified in the
	 * <code>Manifest</code>
	 * 
	 * @return the <code>Created By</code> information as specified in the
	 *         <code>Manifest</code>
	 */
	public String getCreatedBy() {
		return getInfo(CREATED_BY);
	}

	/**
	 * Get the SVN revision as specified in the <code>Manifest</code>
	 * 
	 * @return the SVN revision, or <code>null</code> if not specified
	 */
	public String getSVNRevision() {
		return getInfo(SVN_REVISION);
	}
}
