package net.meisen.dissertation.jdbc.protocol;

/**
 * The status of a query fired against the database. The status can be changed
 * during the processing. Nevertheless the server or client has to evaluate the
 * current status and acknowledge the reading and realization. The way on how to
 * do this is defined in the {@code Protocol}.
 * 
 * @author pmeisen
 * 
 * @see Protocol
 * 
 */
public enum QueryStatus {
	/**
	 * This status indicates that the query should be further processed.
	 */
	PROCESS((byte) 125),
	/**
	 * This status indicates that the query should be further processed and
	 * generated identifiers should be send.
	 */
	PROCESSANDGETIDS((byte) 126),
	/**
	 * This status indicates that the query should not be processed anymore.
	 */
	CANCEL((byte) 127);

	private final byte id;

	private QueryStatus(final byte id) {
		this.id = id;
	}

	/**
	 * Gets the identifier send within the {@code Protocol} to inform the other
	 * side about the status.
	 * 
	 * @return the identifier send within the {@code Protocol}
	 */
	public byte getId() {
		return id;
	}

	/**
	 * Finds the concrete {@code QueryStatus} for the specified {@code id}. The
	 * method returns {@code null} if the identifier is invalid.
	 * 
	 * @param id
	 *            the identifier to get the {@code QueryStatus} for
	 * 
	 * @return the found {@code QueryStatus} instance, or {@code null} if non
	 *         can be found
	 */
	public static QueryStatus find(final byte id) {
		for (final QueryStatus type : QueryStatus.values()) {
			if (id == type.getId()) {
				return type;
			}
		}
		return null;
	}
}
