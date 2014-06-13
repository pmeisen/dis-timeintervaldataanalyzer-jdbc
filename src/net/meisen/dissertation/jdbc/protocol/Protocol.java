package net.meisen.dissertation.jdbc.protocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.util.Date;

public class Protocol implements Closeable {

	public static enum DataType {

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

	public static class WrappedException extends RuntimeException {
		public WrappedException(final String msg) {
			super(msg);
		}
	}

	public static enum ResponseType {

		EXCEPTION((byte) 1), RESULT((byte) 2), MESSAGE((byte) 3), RESOURCE_DEMAND(
				(byte) 4), RESOURCE((byte) 5), EOR((byte) 6, false, false), HEADER(
				(byte) 7), HEADERNAMES((byte) 8, true, true), INT((byte) 9,
				true, Integer.SIZE / 8);

		private final byte id;
		private final boolean hasData;
		private final boolean chunked;
		private final boolean fixed;
		private final int size;

		private ResponseType(final byte id) {
			this(id, true, false, false, -1);
		}

		private ResponseType(final byte id, final boolean fixed, final int size) {
			this(id, true, false, fixed, size);
		}

		private ResponseType(final byte id, final boolean hasData,
				final boolean chunked) {
			this(id, hasData, chunked, false, -1);
		}

		private ResponseType(final byte id, final boolean hasData,
				final boolean chunked, final boolean fixed, final int size) {
			this.id = id;
			this.hasData = hasData;
			this.chunked = chunked;
			this.fixed = fixed;
			this.size = fixed ? size : -1;
		}

		public byte getId() {
			return id;
		}

		public static ResponseType find(final byte id) {
			for (final ResponseType type : ResponseType.values()) {
				if (id == type.getId()) {
					return type;
				}
			}

			return null;
		}

		public boolean hasData() {
			return hasData;
		}

		public boolean isChunked() {
			return chunked;
		}

		public boolean isFixed() {
			return fixed;
		}

		public int getSize() {
			return size;
		}
	}

	public static class RetrievedValue {

		public final ResponseType type;
		public final byte[] bytes;

		protected DataInputStream dis;

		public RetrievedValue(final ResponseType type, final byte[] bytes) {
			this.type = type;
			this.bytes = bytes;
		}

		public boolean is(final ResponseType type) {
			return this.type.equals(type);
		}

		public byte[] getResult() throws IOException {
			checkType(ResponseType.RESULT);
			return bytes;
		}

		public int getInt() throws IOException {
			checkType(ResponseType.INT);

			final DataInputStream dis = getDataInputStream();
			return dis.readInt();
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

		public void checkType(final ResponseType expected) {
			if (!is(expected)) {
				throw new IllegalStateException("Expected to read a '"
						+ expected + "', but got a '" + type + "'.");
			}
		}

		protected String getString() throws IOException {
			final DataInputStream dis = getDataInputStream();
			final int length = dis.readInt();
			final byte[] b = new byte[length];
			dis.read(b);

			return new String(b, "UTF8");
		}

		protected DataInputStream getDataInputStream() {
			if (dis == null) {
				dis = new DataInputStream(new ByteArrayInputStream(bytes));
			}
			return dis;
		}
	}

	public static class ChunkedRetrievedValue extends RetrievedValue {

		private final byte[][] chunks;

		public ChunkedRetrievedValue(final ResponseType type,
				final byte[][] bytes) {
			super(type, bytes == null || bytes.length == 0 ? new byte[0]
					: bytes[0]);
			chunks = bytes;
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

	public static interface IResponseHandler {
		public InputStream getResourceStream(final String resource);

		public void setHeader(final Class<?>[] header);

		public void setHeaderNames(final String[] header);

		public boolean handleResult(final RetrievedValue value);

		public void signalEORReached();
	}

	private final DataInputStream is;
	private final DataOutputStream os;

	/**
	 * The communication will take place over the {@code socket}. The
	 * {@code Communication} instance will just use the provided input- and
	 * output-stream to do so. Therefore the {@code socket} must be closed by
	 * the creating instance.
	 * 
	 * @param socket
	 *            the socket to create the communication on
	 * 
	 * @throws IOException
	 *             if the streams cannot be retrieved
	 */
	public Protocol(final Socket socket) throws IOException {
		this(socket.getInputStream(), socket.getOutputStream());
	}

	public Protocol(final InputStream is, final OutputStream os) {
		this.is = new DataInputStream(new BufferedInputStream(is));
		this.os = new DataOutputStream(new BufferedOutputStream(os));
	}

	public void writeInt(final int value) throws IOException {
		os.writeByte(ResponseType.INT.getId());
		os.writeInt(value);
	}

	public void writeException(final Exception exception) throws IOException {
		final String msg = " [" + exception.getClass().getSimpleName() + "]: "
				+ exception.getLocalizedMessage();

		write(ResponseType.EXCEPTION, msg.getBytes("UTF8"));
	}

	public void writeResult(final byte[] result) throws IOException {
		write(ResponseType.RESULT, result);
	}

	public void writeEndOfResult() throws IOException {
		write(ResponseType.EOR);
	}

	public void writeResourceDemand(final String resource) throws IOException {
		write(ResponseType.RESOURCE_DEMAND, resource.getBytes("UTF8"));
	}

	public void writeResource(final InputStream resource) throws IOException {
		final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

		int nRead;
		byte[] data = new byte[4096];

		while ((nRead = resource.read(data, 0, data.length)) != -1) {
			buffer.write(data, 0, nRead);
		}
		buffer.flush();
		buffer.close();

		write(ResponseType.RESOURCE, buffer.toByteArray());
	}

	public void writeHeader(final Class<?>[] headerTypes) throws IOException {
		if (headerTypes == null) {
			throw new NullPointerException(
					"A header of null-types cannot be written.");
		}

		final byte[] bytes = new byte[headerTypes.length];
		for (int i = 0; i < headerTypes.length; i++) {
			final DataType dt = DataType.find(headerTypes[i]);
			if (dt == null) {
				throw new IllegalArgumentException("Unsupported header-type '"
						+ headerTypes[i].getName() + "'.");
			} else {
				bytes[i] = dt.getId();
			}
		}

		write(ResponseType.HEADER, bytes);
	}

	public void writeHeaderNames(final String[] headerNames) throws IOException {
		if (headerNames == null || headerNames.length == 0) {
			// don't write it at all
		}

		// write the names of the header
		os.writeByte(ResponseType.HEADERNAMES.getId());
		os.writeInt(headerNames.length);
		for (final String headerName : headerNames) {
			writeString(headerName);
		}
		os.flush();
	}

	protected void writeString(final String value) throws IOException {
		if (value == null) {
			os.writeInt(0);
		} else {
			final byte[] bytes = value.getBytes("UTF8");
			os.writeInt(bytes.length);
			os.write(bytes);
		}
	}

	public void writeResource(final byte[] resource) throws IOException {
		write(ResponseType.RESOURCE, resource);
	}

	public RetrievedValue read() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);

		return value;
	}

	public byte[] readResult() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);
		return value.getResult();
	}

	public String readMessage() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);
		return value.getMessage();
	}

	public String readResourceDemand() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);
		return value.getResourceDemand();
	}

	public Class<?>[] readHeader() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);
		return value.getHeader();
	}

	public byte[] readResource() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);
		return value.getResource();
	}

	public void write(final String msg) throws IOException {
		write(ResponseType.MESSAGE, msg.getBytes("UTF8"));
	}

	public void write(final ResponseType type, byte... bytes)
			throws IOException {
		os.writeByte(type.getId());

		// only write something if something is there
		if (type.hasData()) {
			os.writeInt(bytes.length);
			os.write(bytes);
		}
		os.flush();
	}

	public synchronized void writeAndHandle(final String msg,
			final IResponseHandler handler) throws IOException {
		write(msg);
		handleResponse(handler);
	}

	public void handleResponse(final IResponseHandler handler)
			throws IOException {

		boolean read = true;
		while (read) {
			final RetrievedValue value = read();
			if (value.isEOR()) {
				if (handler != null) {
					handler.signalEORReached();
				}
				read = false;
			} else if (value.is(ResponseType.RESOURCE_DEMAND)) {
				final String resource = value.getResourceDemand();

				if (handler == null) {
					// write nothing as resource
					writeResource(new ByteArrayInputStream(new byte[0]));
				} else {
					writeResource(handler.getResourceStream(resource));
				}
			} else if (value.is(ResponseType.HEADER)) {
				if (handler != null) {
					handler.setHeader(value.getHeader());
				}
			} else if (value.is(ResponseType.HEADERNAMES)) {
				if (handler == null) {
					// nothing to do
				} else if (value instanceof ChunkedRetrievedValue) {
					handler.setHeaderNames(((ChunkedRetrievedValue) value)
							.getHeaderNames());
				} else {
					handler.setHeaderNames(new String[] { new String(value
							.getResult(), "UTF8") });
				}
			} else {
				if (handler != null) {
					read = handler.handleResult(value);
				}
			}
		}
	}

	protected void checkException(final RetrievedValue value)
			throws WrappedException {
		if (value.is(ResponseType.EXCEPTION)) {
			try {
				throw new WrappedException(new String(value.bytes, "UTF8"));
			} catch (final UnsupportedEncodingException e) {
				// ignore
			}
		}
	}

	protected RetrievedValue _read() throws IOException {

		// first read the type
		final ResponseType type = ResponseType.find(is.readByte());

		// make sure the type is valid
		if (type == null) {
			throw new IllegalArgumentException(
					"Invalid protocol used for communication.");
		} else if (type.hasData()) {

			if (type.isChunked()) {
				final int chunkSize = is.readInt();
				final byte[][] chunks = new byte[chunkSize][];
				for (int i = 0; i < chunkSize; i++) {
					final int size = is.readInt();
					chunks[i] = new byte[size];
					is.read(chunks[i]);
				}

				return new ChunkedRetrievedValue(type, chunks);
			} else if (type.isFixed()) {
				final byte[] bytes = new byte[type.getSize()];
				is.read(bytes);

				return new RetrievedValue(type, bytes);
			} else {
				final int size = is.readInt();
				final byte[] bytes = new byte[size];
				is.read(bytes);

				return new RetrievedValue(type, bytes);
			}
		} else {
			return new RetrievedValue(type, null);
		}
	}

	@Override
	public void close() throws IOException {
		this.is.close();
		this.os.close();
	}

	@Override
	public int hashCode() {
		return is.hashCode();
	}
}
