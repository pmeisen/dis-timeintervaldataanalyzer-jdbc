package net.meisen.dissertation.jdbc.protocol;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * A {@code RetrievedValue} is the value retrieved from the server or the client
 * during a communication.
 * 
 * @author pmeisen
 * 
 */
public class RetrievedValue {

	/**
	 * The type of the retrieved value.
	 */
	private final ResponseType type;
	/**
	 * The bytes of the value retrieved
	 */
	private final byte[] bytes;

	/**
	 * The {@code InputStream} to read from.
	 */
	protected DataInputStream dis;

	/**
	 * Creates a retrieved value of the specified {@code type} with the
	 * specified read {@code bytes}.
	 * 
	 * @param type
	 *            the type of the {@code RetrievedValue}
	 * @param bytes
	 *            the bytes read
	 */
	public RetrievedValue(final ResponseType type, final byte[] bytes) {
		this.type = type;
		this.bytes = bytes;
	}

	/**
	 * Checks if {@code this} is one of the specified {@code types}.
	 * 
	 * @param types
	 *            the types to be checked
	 * @return {@code true} if {@code this} is on eof the specified types,
	 *         otherwise {@code false}
	 */
	public boolean is(final ResponseType... types) {
		for (final ResponseType type : types) {
			if (this.getType().equals(type)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Tries to read the {@code RetrievedValue} as a single integer
	 * 
	 * @return the integer represented by {@code this}
	 * 
	 * @throws IOException
	 *             if the {@code RetrievedValue} cannot be read as integer
	 */
	public int getInt() throws IOException {
		checkType(ResponseType.INT);

		final DataInputStream dis = getDataInputStream();
		final int res = dis.readInt();
		dis.close();

		return res;
	}

	/**
	 * Gets the integer retrieved from {@link #getInt()} as array.
	 * 
	 * @return the integer retrieved from {@link #getInt()} as array
	 * 
	 * @throws IOException
	 *             if the {@code RetrievedValue} cannot be read as integer
	 */
	public int[] getInts() throws IOException {
		return new int[] { getInt() };
	}

	/**
	 * Gets the integer retrieved from {@link #getInt()} as array.
	 * 
	 * @return the integer retrieved from {@link #getInt()} as array
	 * 
	 * @throws IOException
	 *             if the {@code RetrievedValue} cannot be read as integer
	 */
	public Integer[] getIntegers() throws IOException {
		return new Integer[] { getInt() };
	}

	public DataType[] getHeader() throws IOException {
		checkType(ResponseType.HEADER);

		final DataType[] dts = new DataType[getBytes().length];
		for (int i = 0; i < getBytes().length; i++) {
			final byte id = getBytes()[i];
			final DataType dt = DataType.find(id);
			dts[i] = dt;
		}

		return dts;
	}

	public String getMessage() throws IOException {
		checkType(ResponseType.MESSAGE);
		return new String(getBytes(), "UTF8");
	}

	public String getResourceDemand() throws IOException {
		checkType(ResponseType.RESOURCE_DEMAND);
		return new String(getBytes(), "UTF8");
	}

	public byte[] getResource() throws IOException {
		checkType(ResponseType.RESOURCE);
		return getBytes();
	}

	public boolean isEOR() {
		return is(ResponseType.EOR);
	}

	public boolean isCancel() {
		return is(ResponseType.CANCEL);
	}

	public void checkType(final ResponseType... expected) {
		if (!is(expected)) {
			throw new IllegalStateException("Expected to read one of '"
					+ Arrays.asList(expected) + "', but got a '" + getType()
					+ "'.");
		}
	}

	public String getString() throws IOException {
		final DataInputStream dis = getDataInputStream();
		final int length = dis.readInt();
		final byte[] b = new byte[length];
		dis.read(b);
		dis.close();

		return new String(b, "UTF8");
	}

	protected DataInputStream getDataInputStream() {
		if (dis == null) {
			dis = new DataInputStream(new ByteArrayInputStream(getBytes()));
		}
		return dis;
	}

	public ResponseType getType() {
		return type;
	}

	public byte[] getBytes() {
		return bytes;
	}
}
