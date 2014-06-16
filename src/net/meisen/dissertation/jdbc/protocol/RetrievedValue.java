package net.meisen.dissertation.jdbc.protocol;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class RetrievedValue {

	public final ResponseType type;
	public final byte[] bytes;

	protected DataInputStream dis;

	public RetrievedValue(final ResponseType type, final byte[] bytes) {
		this.type = type;
		this.bytes = bytes;
	}

	public boolean is(final ResponseType... types) {
		for (final ResponseType type : types) {
			if (this.type.equals(type)) {
				return true;
			}
		}
		return false;
	}

	public byte[] getResult() throws IOException {
		checkType(ResponseType.RESULT);
		return bytes;
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

	public Class<?>[] getHeader() throws IOException {
		checkType(ResponseType.HEADER);

		final Class<?>[] clazzes = new Class<?>[bytes.length];
		for (int i = 0; i < bytes.length; i++) {
			final byte id = bytes[i];
			final DataType dt = DataType.find(id);
			clazzes[i] = dt.getRepresentorClass();
		}

		return clazzes;
	}

	public String getMessage() throws IOException {
		checkType(ResponseType.MESSAGE);
		return new String(bytes, "UTF8");
	}

	public String getResourceDemand() throws IOException {
		checkType(ResponseType.RESOURCE_DEMAND);
		return new String(bytes, "UTF8");
	}

	public byte[] getResource() throws IOException {
		checkType(ResponseType.RESOURCE);
		return bytes;
	}

	public boolean isEOR() {
		return is(ResponseType.EOR);
	}

	public void checkType(final ResponseType... expected) {
		if (!is(expected)) {
			throw new IllegalStateException("Expected to read a '" + expected
					+ "', but got a '" + type + "'.");
		}
	}

	protected String getString() throws IOException {
		final DataInputStream dis = getDataInputStream();
		final int length = dis.readInt();
		final byte[] b = new byte[length];
		dis.read(b);
		dis.close();

		return new String(b, "UTF8");
	}

	protected DataInputStream getDataInputStream() {
		if (dis == null) {
			dis = new DataInputStream(new ByteArrayInputStream(bytes));
		}
		return dis;
	}
}
