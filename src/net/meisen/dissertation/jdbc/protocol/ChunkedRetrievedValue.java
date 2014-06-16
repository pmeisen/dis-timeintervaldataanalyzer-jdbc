package net.meisen.dissertation.jdbc.protocol;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class ChunkedRetrievedValue extends RetrievedValue {

	private final byte[][] chunks;

	public ChunkedRetrievedValue(final ResponseType type, final byte[][] bytes) {
		super(type, bytes == null || bytes.length == 0 ? new byte[0] : bytes[0]);
		chunks = bytes;
	}

	@Override
	public int[] getInts() throws IOException {
		checkType(ResponseType.INT, ResponseType.INT_ARRAY);

		if (this.type.equals(ResponseType.INT)) {
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

		if (this.type.equals(ResponseType.INT)) {
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

	public String[] getHeaderNames() throws IOException {
		final String[] headerNames = new String[chunks.length];
		for (int i = 0; i < chunks.length; i++) {
			final String headerName = new String(chunks[i], "UTF8");
			headerNames[i] = headerName;
		}

		return headerNames;
	}
}
