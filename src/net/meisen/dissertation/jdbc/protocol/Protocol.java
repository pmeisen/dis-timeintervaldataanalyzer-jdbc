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

public class Protocol implements Closeable {

	public static class WrappedException extends RuntimeException {
		public WrappedException(final String msg) {
			super(msg);
		}
	}

	public static enum ResponseType {

		EXCEPTION((byte) 1), RESULT((byte) 2), MESSAGE((byte) 3), RESOURCE_DEMAND(
				(byte) 4), RESOURCE((byte) 5), EOR((byte) 6, false);

		private final byte id;
		private final boolean hasData;

		private ResponseType(final byte id) {
			this(id, true);
		}

		private ResponseType(final byte id, final boolean hasData) {
			this.id = id;
			this.hasData = hasData;
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
	}

	public static class RetrievedValue {
		public final ResponseType type;
		public final byte[] bytes;

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

		public String getString() throws IOException {
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
	}

	public static interface IResponseHandler {
		public InputStream getResourceStream(final String resource);

		public void handleResult(final byte[] result);
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

	public String readString() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);
		return value.getString();
	}

	public String readResourceDemand() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);
		return value.getResourceDemand();
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
				read = false;
			} else if (value.is(ResponseType.RESOURCE_DEMAND)) {
				final String resource = value.getResourceDemand();

				if (handler == null) {
					// write nothing as resource
					writeResource(new ByteArrayInputStream(new byte[0]));
				} else {
					writeResource(handler.getResourceStream(resource));
				}
			} else {
				if (handler != null) {
					handler.handleResult(value.getResult());
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
			final int size = is.readInt();
			final byte[] bytes = new byte[size];
			is.read(bytes);

			return new RetrievedValue(type, bytes);
		} else {
			return new RetrievedValue(type, null);
		}
	}

	@Override
	public void close() throws IOException {
		this.is.close();
		this.os.close();
	}
}
