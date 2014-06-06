package net.meisen.dissertation.jdbc.version;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Version class can be used to parse a standard version string into its
 * four components, MAJOR.MINOR.BUILD.REVISION.
 * 
 * @author pmeisen
 * 
 */
public class Version implements Serializable, Cloneable, Comparable<Version> {
	private static final long serialVersionUID = -4316270526722986552L;

	/**
	 * A pattern to match the standard version format
	 * MAJOR.MINOR.BUILD.REVISION.
	 */
	private static final Pattern STD_VERSION_PATT = Pattern
			.compile("^([^\\d]*?)(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?(?:\\.(\\d+))?(.*)$");
	private static final Pattern SNAPSHOT_ID_PATT = Pattern.compile("^"
			+ Pattern.quote("-SNAPSHOT") + "$");

	/**
	 * Everything before the version in the string that was parsed.
	 */
	private String prefix;

	/**
	 * Everything after the version in the string that was parsed.
	 */
	private String suffix;

	/**
	 * The String that was parsed to create this version object.
	 */
	private String rawVersion;

	/**
	 * The version's MAJOR component.
	 */
	private String major = "0";
	/**
	 * The version's MINOR component.
	 */
	private String minor = "0";
	/**
	 * The version's BUILD component.
	 */
	private String build = "0";
	/**
	 * The version's REVISION component.
	 */
	private String revision = "0";

	/**
	 * Initialize a new Version object that is set to "0.0.0.0".
	 */
	public Version() {
	}
	
	/**
	 * Gets the string that was parsed to create this Version object. This
	 * string may not accurately reflect the current values of the Version's
	 * components.
	 * 
	 * @return The string that was parsed to create this Version object. This
	 *         string may not accurately reflect the current values of the
	 *         Version's components.
	 */
	public String toStringRaw() {
		return this.rawVersion;
	}

	/**
	 * Gets everything before the version in the string that was parsed.
	 * 
	 * @return Everything before the version in the string that was parsed.
	 */
	public String getPrefix() {
		return this.prefix;
	}

	/**
	 * Gets everything after the version in the string that was parsed.
	 * 
	 * @return Everything after the version in the string that was parsed.
	 */
	public String getSuffix() {
		return this.suffix;
	}

	/**
	 * Parses a new Version object from a String.
	 * 
	 * @param toParse
	 *            The String object to parse.
	 * @return a new Version object.
	 * 
	 * @throws IllegalArgumentException
	 *             When there is an error parsing the String.
	 */
	public static Version parse(final String toParse)
			throws IllegalArgumentException {
		if (toParse == null) {
			throw new IllegalArgumentException(
					"Error parsing version from null String");
		}

		final Matcher m = STD_VERSION_PATT.matcher(toParse);
		if (!m.find()) {
			throw new IllegalArgumentException(String.format(
					"Error parsing version from '%s'", toParse));
		}

		Version v = new Version();
		v.rawVersion = toParse;
		v.prefix = m.group(1);

		final String major = m.group(2);
		if (major != null && !major.equals("")) {
			v.setMajor(major);
		}

		final String minor = m.group(3);
		if (minor != null && !minor.equals("")) {
			v.setMinor(minor);
		}

		final String build = m.group(4);
		if (build != null && !build.equals("")) {
			v.setBuild(build);
		}

		final String revision = m.group(5);
		if (revision != null && !revision.equals("")) {
			v.setRevision(m.group(5));
		}

		v.suffix = m.group(6);

		return v;
	}

	/**
	 * Gets the version's MAJOR component.
	 * 
	 * @return The version's MAJOR component.
	 */
	public String getMajor() {
		return this.major;
	}

	/**
	 * Sets the version's MAJOR component.
	 * 
	 * @param toSet
	 *            The version's MAJOR component.
	 * @throws IllegalArgumentException
	 *             When a null or non-numeric value is given.
	 */
	public void setMajor(final String toSet) throws IllegalArgumentException {
		if (toSet == null) {
			throw new IllegalArgumentException("Argument is null");
		}

		if (!toSet.matches("\\d+")) {
			throw new IllegalArgumentException("Argument is not numeric");
		}

		if (this.numberOfComponents < 1) {
			this.numberOfComponents = 1;
		}

		this.major = toSet;
	}

	/**
	 * Sets the version's MAJOR component.
	 * 
	 * @param toSet
	 *            The version's MAJOR component.
	 */
	public void setMajor(final int toSet) {
		setMajor(String.valueOf(toSet));
	}

	/**
	 * The version's MAJOR component as an integer.
	 * 
	 * @return the MAJOR component as integer
	 */
	public int getMajorAsInt() {
		return Integer.parseInt(this.major);
	}

	/**
	 * Gets the version's MINOR component.
	 * 
	 * @return The version's MINOR component.
	 */
	public String getMinor() {
		return this.minor;
	}

	/**
	 * Sets the version's MINOR component.
	 * 
	 * @param toSet
	 *            The version's MINOR component.
	 * @throws IllegalArgumentException
	 *             When a null or non-numeric value is given.
	 */
	public void setMinor(final String toSet) throws IllegalArgumentException {
		if (toSet == null) {
			throw new IllegalArgumentException("Argument is null");
		}

		if (!toSet.matches("\\d+")) {
			throw new IllegalArgumentException("Argument is not numeric");
		}

		if (this.numberOfComponents < 2) {
			this.numberOfComponents = 2;
		}

		this.minor = toSet;
	}

	/**
	 * Sets the version's MINOR component.
	 * 
	 * @param toSet
	 *            The version's MINOR component.
	 */
	public void setMinor(int toSet) {
		setMinor(String.valueOf(toSet));
	}

	/**
	 * The version's MINOR component as an integer.
	 * 
	 * @return the MINOR component as an integer
	 */
	public int getMinorAsInt() {
		return Integer.parseInt(this.minor);
	}

	/**
	 * The version's BUILD component as an integer.
	 * 
	 * @return the BUILD component as an integer
	 */
	public int getBuildAsInt() {
		return Integer.parseInt(this.build);
	}

	/**
	 * Gets the version's BUILD component.
	 * 
	 * @return The version's BUILD component.
	 */
	public String getBuild() {
		return this.build;
	}

	/**
	 * Sets the version's BUILD component.
	 * 
	 * @param toSet
	 *            The version's BUILD component.
	 * @throws IllegalArgumentException
	 *             When a null or non-numeric value is given.
	 */
	public void setBuild(final String toSet) throws IllegalArgumentException {
		if (toSet == null) {
			throw new IllegalArgumentException("Argument is null");
		}

		if (!toSet.matches("\\d+")) {
			throw new IllegalArgumentException("Argument is not numeric");
		}

		if (this.numberOfComponents < 3) {
			this.numberOfComponents = 3;
		}

		this.build = toSet;
	}

	/**
	 * Sets the version's BUILD component.
	 * 
	 * @param toSet
	 *            The version's BUILD component.
	 */
	public void setBuild(final int toSet) {
		setBuild(String.valueOf(toSet));
	}

	/**
	 * The version's REVISION component as an integer.
	 * 
	 * @return the REVISION component as an integer
	 */
	public int getRevisionAsInt() {
		return Integer.parseInt(this.revision);
	}

	/**
	 * Gets the version's REVISION component.
	 * 
	 * @return The version's REVISION component.
	 */
	public String getRevision() {
		return this.revision;
	}

	/**
	 * Sets the version's REVISION component.
	 * 
	 * @param toSet
	 *            The version's REVISION component.
	 * @throws IllegalArgumentException
	 *             When a null or non-numeric value is given.
	 */
	public void setRevision(String toSet) throws IllegalArgumentException {
		if (toSet == null) {
			throw new IllegalArgumentException("Argument is null");
		}

		if (!toSet.matches("\\d+")) {
			throw new IllegalArgumentException("Argument is not numeric");
		}

		if (this.numberOfComponents < 4) {
			this.numberOfComponents = 4;
		}

		this.revision = toSet;
	}

	/**
	 * Sets the version's REVISION component.
	 * 
	 * @param toSet
	 *            The version's REVISION component.
	 */
	public void setRevision(int toSet) {
		setRevision(String.valueOf(toSet));
	}

	/**
	 * The number of components that make up the version. The value will always
	 * be between 1 (inclusive) and 4 (inclusive).
	 */
	private int numberOfComponents;

	/**
	 * Gets the number of components that make up the version. The value will
	 * always be between 1 (inclusive) and 4 (inclusive).
	 * 
	 * @return The number of components that make up the version. The value will
	 *         always be between 1 (inclusive) and 4 (inclusive).
	 */
	public int getNumberOfComponents() {
		return this.numberOfComponents;
	}

	/**
	 * Sets the number of components that make up the version.
	 * 
	 * @param toSet
	 *            The number of components that make up the version. Values less
	 *            than 1 are treated as 1. Values greater than 4 are treated as
	 *            4.
	 */
	public void setNumberOfComponents(int toSet) {
		if (toSet < 1) {
			toSet = 1;
		} else if (toSet > 4) {
			toSet = 4;
		}

		this.numberOfComponents = toSet;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		Version v = new Version();

		v.rawVersion = this.rawVersion;
		v.prefix = this.prefix;
		v.suffix = this.suffix;

		v.numberOfComponents = this.numberOfComponents;

		v.major = this.major;
		v.minor = this.minor;
		v.build = this.build;
		v.revision = this.revision;

		return v;
	}

	@Override
	public boolean equals(Object toCompare) {
		// Compare pointers
		if (toCompare == this) {
			return true;
		}

		// Compare types
		if (!(toCompare instanceof Version)) {
			return false;
		}

		return compareTo((Version) toCompare) == 0;
	}

	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	@Override
	public String toString() {
		final String version = String.format("%s.%s.%s.%s", this.major,
				this.minor, this.build, this.revision);

		if (isSnapshot()) {
			return version + suffix;
		} else {
			return version;
		}
	}

	/**
	 * Gets the version as a string including the prefix and suffix.
	 * 
	 * @return The version as a string including the prefix and suffix.
	 */
	public String toStringWithPrefixAndSuffix() {
		return String.format("%s%s.%s.%s.%s%s", this.prefix == null ? ""
				: this.prefix, this.major, this.minor, this.build,
				this.revision, this.suffix == null ? "" : this.suffix);
	}

	/**
	 * Gets the version as a string using the specified number of components.
	 * 
	 * @param components
	 *            The number of components. Values less than 1 will be treated
	 *            as 1 and values greater than 4 will be treated as 4.
	 * @return The version as a string using the specified number of components.
	 */
	public String toString(int components) {
		StringBuilder buff = new StringBuilder();
		buff.append(this.major);

		if (components > 4) {
			components = 4;
		}

		switch (components) {
		case 2: {
			buff.append(String.format(".%s", this.minor));
			break;
		}
		case 3: {
			buff.append(String.format(".%s.%s", this.minor, this.build));
			break;
		}
		case 4: {
			buff.append(String.format(".%s.%s.%s", this.minor, this.build,
					this.revision));
			break;
		}
		}

		return buff.toString();
	}

	/**
	 * Gets the version as a string including the prefix and suffix using the
	 * specified number of components.
	 * 
	 * @param components
	 *            The number of components. Values less than 1 will be treated
	 *            as 1 and values greater than 4 will be treated as 4.
	 * @return The version as a string including the prefix and suffix using the
	 *         specified number of components.
	 */
	public String toStringWithPrefixAndSuffix(int components) {
		StringBuilder buff = new StringBuilder();

		if (prefix != null) {
			buff.append(prefix);
		}

		buff.append(major);

		if (components > 4) {
			components = 4;
		}

		switch (components) {
		case 2: {
			buff.append(String.format(".%s", this.minor));
			break;
		}
		case 3: {
			buff.append(String.format(".%s.%s", this.minor, this.build));
			break;
		}
		case 4: {
			buff.append(String.format(".%s.%s.%s", this.minor, this.build,
					this.revision));
			break;
		}
		}

		if (suffix != null) {
			buff.append(suffix);
		}

		return buff.toString();
	}

	private static int compareInts(final int x, final int y) {
		if (x == y) {
			return 0;
		} else if (x < y) {
			return -1;
		} else {
			return 1;
		}
	}

	@Override
	public int compareTo(final Version toCompare) {
		int result = toString().compareTo(toCompare.toString());
		int snapshot = new Boolean(toCompare.isSnapshot())
				.compareTo(isSnapshot());
		if (result == 0) {
			return snapshot;
		}

		result = compareInts(getMajorAsInt(), toCompare.getMajorAsInt());
		if (result != 0) {
			return result;
		}

		result = compareInts(getMinorAsInt(), toCompare.getMinorAsInt());
		if (result != 0) {
			return result;
		}

		result = compareInts(getBuildAsInt(), toCompare.getBuildAsInt());
		if (result != 0) {
			return result;
		}

		result = compareInts(getRevisionAsInt(), toCompare.getRevisionAsInt());
		if (result != 0) {
			return result;
		}

		return snapshot;
	}

	/**
	 * Adds a whole integer (positive or negative) to a version component.
	 * 
	 * @param toAdd
	 *            The whole integer (positive or negative) to add.
	 * @param toAddTo
	 *            The version component to add the integer to.
	 * @return A string representing the sum that contains at least the same
	 *         number of digits (padded at the left side) as the original
	 *         version component.
	 */
	private static String add(final int toAdd, final String toAddTo) {
		int l = toAddTo.length();
		int i = Integer.parseInt(toAddTo);
		i += toAdd;

		if (i < 0) {
			i = 0;
		}

		String f = String.format("%%0%sd", l);
		String s = String.format(f, i);
		return s;
	}

	/**
	 * Adds a whole (positive or negative) integer to the MAJOR component of the
	 * version. If the number to add is negative and results in a sum less than
	 * 0, the sum is set to 0.
	 * 
	 * @param toAdd
	 *            A whole (positive or negative) integer.
	 */
	public void addMajor(final int toAdd) {
		this.major = add(toAdd, this.major);
	}

	/**
	 * Adds a whole (positive or negative) integer to the MINOR component of the
	 * version. If the number to add is negative and results in a sum less than
	 * 0, the sum is set to 0.
	 * 
	 * @param toAdd
	 *            A whole (positive or negative) integer.
	 */
	public void addMinor(final int toAdd) {
		this.minor = add(toAdd, this.minor);
	}

	/**
	 * Adds a whole (positive or negative) integer to the BUILD component of the
	 * version. If the number to add is negative and results in a sum less than
	 * 0, the sum is set to 0.
	 * 
	 * @param toAdd
	 *            A whole (positive or negative) integer.
	 */
	public void addBuild(final int toAdd) {
		build = add(toAdd, this.build);
	}

	/**
	 * Adds a whole (positive or negative) integer to the REVISION component of
	 * the version. If the number to add is negative and results in a sum less
	 * than 0, the sum is set to 0.
	 * 
	 * @param toAdd
	 *            A whole (positive or negative) integer.
	 */
	public void addRevision(final int toAdd) {
		revision = add(toAdd, this.revision);
	}

	/**
	 * Checks if the <code>Version</code> is marked as a snapshot
	 * 
	 * @return <code>true</code> if the <code>Version</code> is a snapshot,
	 *         otherwise <code>false</code>
	 */
	public boolean isSnapshot() {
		final Matcher m = SNAPSHOT_ID_PATT.matcher(suffix);
		return m.find();
	}
}
