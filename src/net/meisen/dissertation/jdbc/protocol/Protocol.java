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

	private boolean inCommunication = false;

	private final DataInputStream is;
	private final DataOutputStream os;

	/**
	 * The communication will take place over the {@code socket}. The
	 * {@code Protocol} instance will just use the provided input- and
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
		os.flush();
	}

	public void writeInts(final int value) throws IOException {
		writeInts(new int[] { value });
	}

	public void writeInts(final int[] values) throws IOException {
		os.writeByte(ResponseType.INT_ARRAY.getId());
		os.writeInt(values.length);
		for (int i = 0; i < values.length; i++) {
			os.writeInt(values[i]);
		}
		os.flush();
	}

	public void writeException(final Exception exception) throws IOException {
		final String msg = " [" + exception.getClass().getSimpleName() + "]: "
				+ exception.getLocalizedMessage();

		write(ResponseType.EXCEPTION, msg.getBytes("UTF8"));
	}

	public void writeResult(final DataType[] header, final Object[] values)
			throws IOException {

		// make sure the type is correct
		if (header.length != values.length) {
			throw new IllegalArgumentException(
					"The amount of header does not fit the amount of specified values ('"
							+ header.length + "' != '" + values.length + "').");
		}

		// generate the bytes to be written
		os.writeByte(ResponseType.RESULT.getId());
		for (int i = 0; i < header.length; i++) {
			final DataType dt = header[i];
			dt.write(os, values[i]);
		}
		os.flush();
	}

	public Object[] readResult(final DataType[] header) throws IOException {

		final Object[] result = new Object[header.length];
		for (int i = 0; i < header.length; i++) {
			final DataType dt = header[i];
			result[i] = dt.read(is);
		}

		return result;
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

	public DataType[] writeHeader(final Class<?>[] headerTypes)
			throws IOException {
		if (headerTypes == null) {
			throw new NullPointerException(
					"A header of null-types cannot be written.");
		}

		final DataType[] dts = new DataType[headerTypes.length];
		final byte[] bytes = new byte[headerTypes.length];
		for (int i = 0; i < headerTypes.length; i++) {
			final DataType dt = DataType.find(headerTypes[i]);
			if (dt == null) {
				throw new IllegalArgumentException("Unsupported header-type '"
						+ headerTypes[i].getName() + "'.");
			} else {
				dts[i] = dt;
				bytes[i] = dt.getId();
			}
		}

		write(ResponseType.HEADER, bytes);

		return dts;
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

	public void writeMeta(final QueryType queryType, final Class<?>[] header,
			final String[] headerNames) throws IOException {
		writeQueryType(queryType);
		writeHeader(header);
		writeHeaderNames(headerNames);
	}

	public void writeResource(final byte[] resource) throws IOException {
		write(ResponseType.RESOURCE, resource);
	}

	public RetrievedValue read() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);

		return value;
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

	public DataType[] readHeader() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);
		return value.getHeader();
	}

	public byte[] readResource() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);
		return value.getResource();
	}

	public QueryType readQueryType() throws IOException {
		final byte marker = is.readByte();
		return QueryType.find(marker);
	}

	public QueryStatus readQueryStatus() throws IOException {
		final byte marker = is.readByte();
		return QueryStatus.find(marker);
	}

	public void writeMessage(final String msg) throws IOException {
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

	public void writeQueryType(final QueryType type) throws IOException {
		os.writeByte(type.getId());
		os.flush();
	}

	public void writeQueryStatus(final QueryStatus status) throws IOException {
		os.writeByte(status.getId());
		os.flush();
	}

	public boolean initializeCommunication(final String msg,
			final IResponseHandler handler) throws IOException {

		// finish any old communication
		if (inCommunication) {
			System.out.println("START " + msg);
			while (!handleResponse(null)) {
				System.out.println("HANDLING");
				// do nothing keep reading
			}
			System.out.println("DONE HANDLING");
		}

		// start the new communication
		inCommunication = true;

		// reset the handler to handle a new communication
		if (handler != null) {
			handler.resetHandler();
		}

		// write the message
		writeMessage(msg);

		// get the type of the query
		final QueryType queryType = readQueryType();
		if (queryType == null) {
			throw new IllegalStateException(
					"Expected a queryType to be send, got something else.");
		}

		// determine if the query should be handled and write the status
		final QueryStatus status = handler == null ? QueryStatus.PROCESS
				: handler.doHandleQueryType(queryType);
		writeQueryStatus(status);

		// depending on the status read the rest or not
		if (QueryStatus.CANCEL.equals(status)) {
			while (!handleResponse(null)) {
				/*
				 * do nothing, but clear everything on the socket, until an end
				 * is read
				 */
			}
			return false;
		} else {
			return true;
		}
	}

	public synchronized boolean writeAndHandle(final String msg,
			final IResponseHandler handler) throws IOException {
		if (initializeCommunication(msg, handler)) {
			handleResponse(handler);

			return true;
		} else {
			return false;
		}
	}

	public boolean handleResponse(final IResponseHandler handler)
			throws IOException {

		DataType[] header = null;
		boolean eorReached = false;
		boolean read = true;
		while (read) {
			final RetrievedValue value = read();

			if (value.isEOR()) {
				if (handler != null) {
					handler.signalEORReached();
				}
				read = false;
				eorReached = true;
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
				} else {
					header = value.getHeader();
				}
			} else if (value.is(ResponseType.HEADERNAMES)) {
				if (handler == null) {
					// nothing to do
				} else if (value instanceof ChunkedRetrievedValue) {
					handler.setHeaderNames(((ChunkedRetrievedValue) value)
							.getHeaderNames());
				} else {
					handler.setHeaderNames(new String[] { value.getString() });
				}
			} else if (value.is(ResponseType.RESULT)) {
				if (handler != null) {
					final Object[] result = readResult(handler.getHeader());
					read = handler.handleResult(value.getType(), result);
				} else {
					readResult(header);
				}
			} else if (value.is(ResponseType.INT)
					|| value.is(ResponseType.INT_ARRAY)) {
				if (handler != null) {
					read = handler.handleResult(value.getType(),
							value.getIntegers());
				}
			} else {
				throw new IllegalStateException("Cannot handle the result '"
						+ value + "'.");
			}
		}

		if (eorReached) {
			inCommunication = false;
		}

		return eorReached;
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
		final byte typeId = is.readByte();
		final ResponseType type = ResponseType.find(typeId);

		// make sure the type is valid
		if (type == null) {
			throw new IllegalArgumentException(
					"Invalid protocol used for communication (unknown type '"
							+ typeId + "').");
		} else if (type.hasData()) {

			if (type.isChunked()) {
				final int chunkSize = is.readInt();
				final byte[][] chunks = new byte[chunkSize][];

				if (type.isFixed()) {
					for (int i = 0; i < chunkSize; i++) {
						chunks[i] = new byte[type.getFixedSize()];
						is.read(chunks[i]);
					}

				} else {
					for (int i = 0; i < chunkSize; i++) {
						final int size = is.readInt();
						chunks[i] = new byte[size];
						is.read(chunks[i]);
					}
				}

				return new ChunkedRetrievedValue(type, chunks);
			} else if (type.isFixed()) {
				final byte[] bytes = new byte[type.getFixedSize()];
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
		inCommunication = false;
		
		this.is.close();
		this.os.close();
	}

	@Override
	public int hashCode() {
		return is.hashCode();
	}
}
