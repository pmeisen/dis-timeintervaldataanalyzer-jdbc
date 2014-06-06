package net.meisen.dissertation.jdbc;

import java.io.IOException;

import net.meisen.dissertation.jdbc.version.Manifest;
import net.meisen.dissertation.jdbc.version.ManifestInfo;
import net.meisen.dissertation.jdbc.version.Version;

/**
 * Some constants used by the driver.
 * 
 * @author pmeisen
 * 
 */
public class Constants {
	/**
	 * Placeholder used within the {@link #URL_FULL_SYNTAX} and
	 * {@link #URL_SYNTAX} for the host.
	 */
	public static final String URL_HOST_PLACEHOLDER = "[host]";
	/**
	 * Placeholder used within the {@link #URL_FULL_SYNTAX} and
	 * {@link #URL_SYNTAX} for the port.
	 */
	public static final String URL_PORT_PLACEHOLDER = "[port]";
	/**
	 * Placeholder used within the {@link #URL_FULL_SYNTAX} and
	 * {@link #URL_SYNTAX} for the user.
	 */
	public static final String URL_USER_PLACEHOLDER = "[user]";
	/**
	 * Placeholder used within the {@link #URL_FULL_SYNTAX} and
	 * {@link #URL_SYNTAX} for the password.
	 */
	public static final String URL_PW_PLACEHOLDER = "[password]";
	/**
	 * The url-prefix used to mark a url to be a tida-url.
	 */
	public static final String URL_PREFIX = "jdbc:tida://";
	/**
	 * The complete syntax of a tida-url.
	 */
	public static final String URL_FULL_SYNTAX = URL_PREFIX
			+ URL_USER_PLACEHOLDER + ":" + URL_PW_PLACEHOLDER + "@"
			+ URL_HOST_PLACEHOLDER + ":" + URL_PORT_PLACEHOLDER;
	/**
	 * The simplified syntax of a tida-url.
	 */
	public static final String URL_SYNTAX = URL_PREFIX + URL_HOST_PLACEHOLDER
			+ ":" + URL_PORT_PLACEHOLDER;

	private static Version version = null;
	private static ManifestInfo manifestInfo = null;

	/**
	 * Gets the manifest information.
	 * 
	 * @return the manifest information
	 */
	public static ManifestInfo getManifestInfo() {
		if (manifestInfo == null) {
			try {
				manifestInfo = Manifest.getManifestInfo(Constants.class);
			} catch (final IOException e) {
				manifestInfo = null;
			}
		}

		return manifestInfo;
	}

	/**
	 * Gets the version of the driver read from it's manifest.
	 * 
	 * @return the version
	 */
	public static Version getVersion() {
		if (version == null) {
			final ManifestInfo manifestInfo = getManifestInfo();
			if (manifestInfo == null) {
				version = null;
			} else {
				version = Version
						.parse(manifestInfo.getImplementationVersion());
			}
		}
		return version;
	}
}
