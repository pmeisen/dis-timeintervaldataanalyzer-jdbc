package net.meisen.dissertation.jdbc.protocol;

public enum QueryStatus {
	PROCESS((byte) 125), PROCESSANDGETIDS((byte) 126), CANCEL((byte) 127);

	private final byte id;

	private QueryStatus(final byte id) {
		this.id = id;
	}

	public byte getId() {
		return id;
	}

	public static QueryStatus find(final byte id) {
		for (final QueryStatus type : QueryStatus.values()) {
			if (id == type.getId()) {
				return type;
			}
		}
		return null;
	}
}
