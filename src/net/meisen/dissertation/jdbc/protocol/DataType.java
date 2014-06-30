package net.meisen.dissertation.jdbc.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Types;
import java.util.Date;

/**
 * The {@code DataTypes} available to be transfered by the {@code Protocol}.
 * 
 * @author pmeisen
 * 
 */
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
	 * The string data-type.
	 */
	STRING((byte) 5, 0, 0, false, Types.VARCHAR,
			new Class<?>[] { String.class }),
	/**
	 * The date data-type.
	 */
	DATE((byte) 6, "##.##.#### ##.##.##,###".length(), 0, false,
			Types.TIMESTAMP, new Class<?>[] { Date.class }),
	/**
	 * The double data-type.
	 */
	DOUBLE((byte) 7, 15, 15, true, Types.DOUBLE, new Class<?>[] { Double.class,
			double.class });

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

	/**
	 * Gets the precision of the data-type.
	 * 
	 * @return the precision of the data-type.
	 */
	public int getPrecision() {
		return precision;
	}

	/**
	 * Gets the scale of the data-type.
	 * 
	 * @return the scale of the data-type.
	 */
	public int getScale() {
		return scale;
	}

	/**
	 * Gets the sql-type of the data-type.
	 * 
	 * @return the sql-type of the data-type.
	 */
	public int getSqlType() {
		return sqlType;
	}

	/**
	 * Checks if the data-type is a numeric, signed value.
	 * 
	 * @return {@code true} if the data-type is a numeric value, which is
	 *         signed, otherwise {@code false}
	 */
	public boolean isSigned() {
		return signed;
	}

	/**
	 * Gets the identifier of the data-type.
	 * 
	 * @return the identifier of the data-type.
	 */
	public byte getId() {
		return id;
	}

	/**
	 * Gets the class used to represent the data-type.
	 * 
	 * @return the class used to represent the data-type.
	 */
	public Class<?> getRepresentorClass() {
		return clazzes == null || clazzes.length == 0 ? null : clazzes[0];
	}

	/**
	 * Checks if the specified {@code clazz} is supported by the {@code this}.
	 * 
	 * @param clazz
	 *            the class to be checked
	 * 
	 * @return {@code true} if it is supported, otherwise {@code false}
	 */
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

	/**
	 * Reads {@code this} data-type from the specified {@code is}.
	 * 
	 * @param in
	 *            the {@code DataInput} to read from
	 * 
	 * @return the object read
	 * 
	 * @throws IOException
	 *             if an IO-exception occurs
	 */
	public Object read(final DataInput in) throws IOException {
		byte nullIndicator = in.readByte();
		if (nullIndicator == 0) {
			return null;
		} else {
			if (BYTE.equals(this)) {
				return in.readByte();
			} else if (SHORT.equals(this)) {
				return in.readShort();
			} else if (INT.equals(this)) {
				return in.readInt();
			} else if (LONG.equals(this)) {
				return in.readLong();
			} else if (DATE.equals(this)) {
				return new Date((Long) in.readLong());
			} else if (DOUBLE.equals(this)) {
				return in.readDouble();
			} else if (STRING.equals(this)) {
				final int length = in.readInt();
				final byte[] bytes = new byte[length];
				in.readFully(bytes);
				return new String(bytes, "UTF8");
			} else {
				throw new IllegalStateException("The read-method of dataType '"
						+ this + "' is not implemented.");
			}
		}
	}

	/**
	 * Writes {@code this} data-type from the specified {@code os}.
	 * 
	 * @param out
	 *            the {@code DataOutput} to write to
	 * @param object
	 *            the object to be written
	 * 
	 * @throws IOException
	 *             if an IO-exception occurs
	 */
	public void write(final DataOutput out, final Object object)
			throws IOException {
		if (object == null) {
			out.writeByte(0);
		} else {
			out.writeByte(1);

			if (BYTE.equals(this)) {
				out.writeByte((Byte) object);
			} else if (SHORT.equals(this)) {
				out.writeShort((Short) object);
			} else if (INT.equals(this)) {
				out.writeInt((Integer) object);
			} else if (LONG.equals(this)) {
				out.writeLong((Long) object);
			} else if (DATE.equals(this)) {
				out.writeLong(((Date) object).getTime());
			} else if (DOUBLE.equals(this)) {
				out.writeDouble((Double) object);
			} else if (STRING.equals(this)) {
				final String s = (String) object;
				out.writeInt(s.length());
				out.write(s.getBytes("UTF8"));
			} else {
				throw new IllegalStateException(
						"The write-method of dataType '" + this
								+ "' is not implemented.");
			}
		}
	}

	/**
	 * Checks if the specified {@code DataType} is an integer.
	 * 
	 * @return {@code true} if the type is an integer, otherwise {@code false}
	 */
	public boolean isInteger() {
		final Class<?> repClass = getRepresentorClass();

		return repClass != null
				&& (repClass.equals(Byte.class) || repClass.equals(Short.class)
						|| repClass.equals(Integer.class) || repClass
							.equals(Long.class));
	}

	/**
	 * Finds the {@code DataType} for the specified {@code clazz}.
	 * 
	 * @param clazz
	 *            the class to get the {@code DataType} for
	 * 
	 * @return the {@code DataType} for the specified {@code clazz} or
	 *         {@code null} if no {@code DataType} can be found
	 */
	public static DataType find(final Class<?> clazz) {
		for (final DataType type : DataType.values()) {
			if (type.isClass(clazz)) {
				return type;
			}
		}
		return null;
	}

	/**
	 * Finds the {@code DataType} for the specified {@code id}.
	 * 
	 * @param id
	 *            the identifier to get the {@code DataType} for
	 * 
	 * @return the {@code DataType} for the specified {@code id} or {@code null}
	 *         if no {@code DataType} can be found
	 */
	public static DataType find(final byte id) {
		for (final DataType type : DataType.values()) {
			if (id == type.getId()) {
				return type;
			}
		}
		return null;
	}

	/**
	 * Checks if the {@code clazz} is supported by any {@code DataType}.
	 * 
	 * @param clazz
	 *            the class to be checked
	 * 
	 * @return {@code true} if it is supported, otherwise {@code false}
	 * 
	 * @see #find(Class)
	 */
	public static boolean isSupported(final Class<?> clazz) {
		return find(clazz) != null;
	}
}