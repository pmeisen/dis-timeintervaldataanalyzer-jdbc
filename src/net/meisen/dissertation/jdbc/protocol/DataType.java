package net.meisen.dissertation.jdbc.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Types;
import java.util.Date;

public enum DataType {

	/**
	 * The byte data-type.
	 */
	BYTE((byte) 1, ("" + Byte.MAX_VALUE).length(), 0, true, Types.TINYINT,
			new Class<?>[] { Byte.class, byte.class }),
	/**
	 * The short data-type.
	 */
	SHORT((byte) 2, ("" + Short.MAX_VALUE).length(), 0, true, Types.SMALLINT,
			new Class<?>[] { Short.class, short.class }),
	/**
	 * The int data-type.
	 */
	INT((byte) 3, ("" + Integer.MAX_VALUE).length(), 0, true, Types.INTEGER,
			new Class<?>[] { Integer.class, int.class }),
	/**
	 * The long data-type.
	 */
	LONG((byte) 4, ("" + Long.MAX_VALUE).length(), 0, true, Types.BIGINT,
			new Class<?>[] { Long.class, long.class }),
	/**
	 * The date data-type.
	 */
	DATE((byte) 6, "##.##.#### ##.##.##,###".length(), 0, false,
			Types.TIMESTAMP, new Class<?>[] { Date.class }),
	/**
	 * The double data-type.
	 */
	DOUBLE((byte) 7, 15, 15, true, Types.DOUBLE, new Class<?>[] { Double.class,
			double.class }),
	/**
	 * The string data-type.
	 */
	STRING((byte) 5, 0, 0, false, Types.VARCHAR,
			new Class<?>[] { String.class });

	private final byte id;
	private final Class<?>[] clazzes;
	private final boolean signed;
	private final int sqlType;
	private final int precision;
	private final int scale;

	private DataType(final byte id, final int precision, final int scale,
			final boolean signed, final int sqlType, final Class<?>[] clazzes) {
		this.id = id;
		this.clazzes = clazzes;
		this.signed = signed;
		this.sqlType = sqlType;
		this.precision = precision;
		this.scale = scale;
	}

	public int getPrecision() {
		return precision;
	}

	public int getScale() {
		return scale;
	}

	public int getSqlType() {
		return sqlType;
	}

	public boolean isSigned() {
		return signed;
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

	@SuppressWarnings("unchecked")
	public Object read(final DataInputStream is) throws IOException {
		if (BYTE.equals(this)) {
			return is.readByte();
		} else if (SHORT.equals(this)) {
			return is.readShort();
		} else if (INT.equals(this)) {
			return is.readInt();
		} else if (LONG.equals(this)) {
			return is.readLong();
		} else if (DATE.equals(this)) {
			return new Date((Long) is.readLong());
		} else if (DOUBLE.equals(this)) {
			return is.readDouble();
		} else if (STRING.equals(this)) {
			final int length = is.readInt();
			final byte[] bytes = new byte[length];
			is.read(bytes);
			return new String(bytes, "UTF8");
		} else {
			throw new IllegalStateException("The read-method of dataType '"
					+ this + "' is not implemented.");
		}
	}

	public void write(final DataOutputStream os, final Object object)
			throws IOException {
		if (object == null) {

		}

		if (BYTE.equals(this)) {
			os.writeByte((Byte) object);
		} else if (SHORT.equals(this)) {
			os.writeShort((Short) object);
		} else if (INT.equals(this)) {
			os.writeInt((Integer) object);
		} else if (LONG.equals(this)) {
			os.writeLong((Long) object);
		} else if (DATE.equals(this)) {
			os.writeLong(((Date) object).getTime());
		} else if (DOUBLE.equals(this)) {
			os.writeDouble((Double) object);
		} else if (STRING.equals(this)) {
			final String s = (String) object;
			os.writeInt(s.length());
			os.write(s.getBytes("UTF8"));
		} else {
			throw new IllegalStateException("The write-method of dataType '"
					+ this + "' is not implemented.");
		}
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