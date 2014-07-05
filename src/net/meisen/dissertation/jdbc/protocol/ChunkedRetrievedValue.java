package net.meisen.dissertation.jdbc.protocol;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * A {@code ChunkedRetrievedValue} is a special form of {@code RetrievedValue}.
 * The value is retrieved in chunks, i.e. not all at once. This implementation
 * is normally used to send e.g. arrays of unknown size.
 * 
 * @author pmeisen
 * 
 */
public class ChunkedRetrievedValue extends RetrievedValue {

	private final byte[][] chunks;

	/**
	 * Creates the {@code ChunkedRetrievedValue} of the specified {@code type}
	 * with the specified chunks.
	 * 
	 * @param type
	 *            the {@code ResponseType} of the chunks
	 * @param bytes
	 *            the different chunks retrieved
	 */
	public ChunkedRetrievedValue(final ResponseType type, final byte[][] bytes) {
		super(type, bytes == null || bytes.length == 0 ? new byte[0] : bytes[0]);
		chunks = bytes;
	}

	@Override
	public int[] getInts() throws IOException {
		checkType(ResponseType.INT, ResponseType.INT_ARRAY);

		if (this.getType().equals(ResponseType.INT)) {
			return new int[] { getInt() };
		} else {
			final int[] ints = new int[chunks.length];
			for (int i = 0; i < chunks.length; i++) {
				final DataInputStream dis = new DataInputStream(
						new ByteArrayInputStream(chunks[i]));
				ints[i] = dis.readInt();
				dis.close();
			}

			return ints;
		}
	}

	@Override
	public Integer[] getIntegers() throws IOException {
		checkType(ResponseType.INT, ResponseType.INT_ARRAY);

		if (this.getType().equals(ResponseType.INT)) {
			return new Integer[] { getInt() };
		} else {
			final Integer[] ints = new Integer[chunks.length];
			for (int i = 0; i < chunks.length; i++) {
				final DataInputStream dis = new DataInputStream(
						new ByteArrayInputStream(chunks[i]));
				ints[i] = dis.readInt();
				dis.close();
			}

			return ints;
		}
	}

	/**
	 * Gets the header-names defined by {@code this}.
	 * 
	 * @return the read names
	 * 
	 * @throws IOException
	 *             if the string cannot be interpreted
	 * @throws IllegalStateException
	 *             if the type is unequal to {@link ResponseType#HEADERNAMES}
	 */
	public String[] getHeaderNames() throws IOException, IllegalStateException {
		checkType(ResponseType.HEADERNAMES);

		final String[] headerNames = new String[chunks.length];
		for (int i = 0; i < chunks.length; i++) {
			final String headerName = new String(chunks[i], "UTF8");
			headerNames[i] = headerName;
		}

		return headerNames;
	}
}
