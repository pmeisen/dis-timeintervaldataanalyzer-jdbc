package net.meisen.dissertation.jdbc.protocol;

/**
 * A {@code QueryType} defines the type of a statement send to the server. The
 * type can be used to query for data or to manipulate the settings or schemas
 * on the server-side.
 * 
 * @author pmeisen
 * 
 */
public enum QueryType {
	/**
	 * A statement which is used to query for data.
	 */
	QUERY((byte) 100),
	/**
	 * A statement which is used to manipulate settings or schemas.
	 */
	MANIPULATION((byte) 101);

	private final byte id;

	private QueryType(final byte id) {
		this.id = id;
	}

	/**
	 * Gets the byte identifier used to identify the type.
	 * 
	 * @return the byte identifier used to identify the type
	 */
	public byte getId() {
		return id;
	}

	/**
	 * Finds the {@code QueryType} for the specified {@code id}.
	 * 
	 * @param id
	 *            the identifier to find the {@code QueryType} for
	 * 
	 * @return the found {@code QueryType} or {@code null} if non could be found
	 */
	public static QueryType find(final byte id) {
		for (final QueryType type : QueryType.values()) {
			if (id == type.getId()) {
				return type;
			}
		}
		return null;
	}
}
