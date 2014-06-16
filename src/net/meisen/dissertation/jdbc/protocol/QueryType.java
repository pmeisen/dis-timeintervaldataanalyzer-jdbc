package net.meisen.dissertation.jdbc.protocol;

public enum QueryType {
	QUERY((byte) 100), MANIPULATION((byte) 101);

	private final byte id;

	private QueryType(final byte id) {
		this.id = id;
	}

	public byte getId() {
		return id;
	}

	public static QueryType find(final byte id) {
		for (final QueryType type : QueryType.values()) {
			if (id == type.getId()) {
				return type;
			}
		}
		return null;
	}
}
