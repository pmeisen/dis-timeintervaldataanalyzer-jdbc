package net.meisen.dissertation.jdbc.protocol;

import java.util.Date;

public enum DataType {

	/**
	 * The byte data-type.
	 */
	BYTE((byte) 1, new Class<?>[] { Byte.class, byte.class }),
	/**
	 * The short data-type.
	 */
	SHORT((byte) 2, new Class<?>[] { Short.class, short.class }),
	/**
	 * The int data-type.
	 */
	INT((byte) 3, new Class<?>[] { Integer.class, int.class }),
	/**
	 * The long data-type.
	 */
	LONG((byte) 4, new Class<?>[] { Long.class, long.class }),
	/**
	 * The string data-type.
	 */
	STRING((byte) 5, new Class<?>[] { String.class }),
	/**
	 * The date data-type.
	 */
	DATE((byte) 6, new Class<?>[] { Date.class }),
	/**
	 * The double data-type.
	 */
	DOUBLE((byte) 7, new Class<?>[] { Double.class, double.class });

	private final byte id;
	private final Class<?>[] clazzes;

	private DataType(final byte id, final Class<?>[] clazzes) {
		this.id = id;
		this.clazzes = clazzes;
	}

	public byte getId() {
		return id;
	}

	public Class<?> getRepresentorClass() {
		return clazzes == null || clazzes.length == 0 ? null : clazzes[0];
	}

	public boolean isClass(final Class<?> clazz) {
		if (clazzes == null) {
			return false;
		}

		// search for the Class
		for (final Class<?> c : clazzes) {
			if (c.equals(clazz)) {
				return true;
			}
		}
		for (final Class<?> c : clazzes) {
			if (c.isAssignableFrom(clazz)) {
				return true;
			}
		}

		return false;
	}

	public static DataType find(final Class<?> clazz) {
		for (final DataType type : DataType.values()) {
			if (type.isClass(clazz)) {
				return type;
			}
		}
		return null;
	}

	public static DataType find(final byte id) {
		for (final DataType type : DataType.values()) {
			if (id == type.getId()) {
				return type;
			}
		}
		return null;
	}

	public static boolean isSupported(final Class<?> clazz) {
		return find(clazz) != null;
	}
}