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
import java.util.HashSet;
import java.util.Set;

import net.meisen.dissertation.jdbc.QueryResponseHandler;

/**
 * A {@code Protocol} to communicate with the server.
 * 
 * @author pmeisen
 * 
 */
public class Protocol implements Closeable {

	/*
	 * Do a validation by checking all used byte-identifiers. The identifiers
	 * have to be unique across all the different kinds, i.e. QueryStatus,
	 * QueryType and ResponseType.
	 */
	static {
		final Set<Byte> usedBytes = new HashSet<Byte>();
		for (final QueryStatus status : QueryStatus.values()) {
			assert usedBytes.add(status.getId());
		}
		for (final QueryType type : QueryType.values()) {
			assert usedBytes.add(type.getId());
		}
		for (final ResponseType type : ResponseType.values()) {
			assert usedBytes.add(type.getId());
		}
	}

	private boolean inCommunication;

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

	/**
	 * Constructor to define the {@code InputStream} and {@code OutputStream} to
	 * communicate over.
	 * 
	 * @param is
	 *            the {@code InputStream} used for the communication
	 * @param os
	 *            the {@code OutputStream} used for the communication
	 */
	public Protocol(final InputStream is, final OutputStream os) {
		this.is = new DataInputStream(new BufferedInputStream(is));
		this.os = new DataOutputStream(new BufferedOutputStream(os));

		markCommunicationAsFinal(null);
	}

	/**
	 * Writes an integer.
	 * 
	 * @param value
	 *            the integer to be written
	 * 
	 * @throws IOException
	 *             if the {@code value} cannot be written
	 */
	public void writeInt(final int value) throws IOException {
		os.writeByte(ResponseType.INT.getId());
		os.writeInt(value);
		os.flush();
	}

	/**
	 * Writes a single integer as array.
	 * 
	 * @param value
	 *            the integer to be written
	 * 
	 * @throws IOException
	 *             if the {@code value} cannot be written
	 */
	public void writeInts(final int value) throws IOException {
		writeInts(new int[] { value });
	}

	/**
	 * Writes an integer-array.
	 * 
	 * @param values
	 *            the integers to be written
	 * 
	 * @throws IOException
	 *             if the {@code values} cannot be written
	 */
	public void writeInts(final int[] values) throws IOException {
		os.writeByte(ResponseType.INT_ARRAY.getId());
		os.writeInt(values.length);
		for (int i = 0; i < values.length; i++) {
			os.writeInt(values[i]);
		}
		os.flush();
	}

	/**
	 * Writes an {@code exception}.
	 * 
	 * @param exception
	 *            the {@code exception} to be written
	 * 
	 * @throws IOException
	 *             if the {@code exception} cannot be written
	 */
	public void writeException(final Exception exception) throws IOException {
		final String msg = " [" + exception.getClass().getSimpleName() + "]: "
				+ exception.getLocalizedMessage();

		write(ResponseType.EXCEPTION, msg.getBytes("UTF8"));
	}

	/**
	 * Writes a result, i.e. the {@code values}.
	 * 
	 * @param header
	 *            the header's types of the values to be written
	 * @param values
	 *            the values to be written
	 * 
	 * @throws IOException
	 *             if the {@code exception} cannot be written
	 */
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

	/**
	 * Reads an result.
	 * 
	 * @param header
	 *            the header of the values to be read
	 * 
	 * @return the read result
	 * 
	 * @throws IOException
	 *             if the result cannot be read
	 */
	public Object[] readResult(final DataType[] header) throws IOException {

		final Object[] result = new Object[header.length];
		for (int i = 0; i < header.length; i++) {
			final DataType dt = header[i];
			result[i] = dt.read(is);
		}

		return result;
	}

	/**
	 * Writes a flag meaning end-of-response.
	 * 
	 * @throws IOException
	 *             if the flag cannot be written
	 */
	public void writeEndOfResponse() throws IOException {
		write(ResponseType.EOR);
	}

	/**
	 * Writes a flag meaning end-of-meta data.
	 * 
	 * @throws IOException
	 *             if the flag cannot be written
	 */
	public void writeEndOfMeta() throws IOException {
		write(ResponseType.EOM);
	}

	/**
	 * Writes a flag meaning cancelled.
	 * 
	 * @throws IOException
	 *             if the flag cannot be written
	 */
	public void writeCancellation() throws IOException {
		write(ResponseType.CANCEL);
	}

	/**
	 * Writes a {@code ResourceDemand}, whereby the {@code resource} specifies
	 * which resource is demanded.
	 * 
	 * @param resource
	 *            the resource needed
	 * 
	 * @throws IOException
	 *             if the demand cannot be written
	 */
	public void writeResourceDemand(final String resource) throws IOException {
		write(ResponseType.RESOURCE_DEMAND, resource.getBytes("UTF8"));
	}

	/**
	 * Writes a {@code Resource}, whereby the {@code resource} is written
	 * directly to the socket.
	 * 
	 * @param resource
	 *            the resource to be written
	 * 
	 * @throws IOException
	 *             if the resource cannot be written
	 */
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

	/**
	 * Writes a {@code Header} to the socket.
	 * 
	 * @param headerTypes
	 *            the types to be written
	 * 
	 * @return the written types as {@code DataType} instance
	 * 
	 * @throws IOException
	 *             if the header cannot be written
	 */
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

	/**
	 * Writes the names of the {@code Header} to the socket.
	 * 
	 * @param headerNames
	 *            the names to be written
	 * 
	 * @throws IOException
	 *             if the names cannot be written
	 */
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

	/**
	 * Writes the specified {@code credential} to the socket.
	 * 
	 * @param username
	 *            the user part of the credential
	 * @param password
	 *            the password part of the credential
	 * 
	 * @throws IOException
	 *             if the credential cannot be written
	 */
	public void writeCredential(final String username, final String password)
			throws IOException {
		os.writeByte(ResponseType.CREDENTIALS.getId());
		os.writeInt(2);
		writeString(username);
		writeString(password);
		os.flush();
	}

	/**
	 * Writes the specified {@code value} to the socket.
	 * 
	 * @param value
	 *            the string to be written
	 * 
	 * @throws IOException
	 *             if the string cannot be written
	 */
	protected void writeString(final String value) throws IOException {
		if (value == null) {
			os.writeInt(0);
		} else {
			final byte[] bytes = value.getBytes("UTF8");
			os.writeInt(bytes.length);
			os.write(bytes);
		}
	}

	/**
	 * Writes the complete meta-information (i.e. {@code queryType},
	 * {@code header} and {@code headerNames}) to the socket.
	 * 
	 * @param queryType
	 *            the {@code QueryType} to be written
	 * @param header
	 *            the header to be written
	 * @param headerNames
	 *            the names to be written
	 * 
	 * @throws IOException
	 *             if the names cannot be written
	 */
	public void writeMeta(final QueryType queryType, final Class<?>[] header,
			final String[] headerNames) throws IOException {
		writeQueryType(queryType);
		writeHeader(header);
		writeHeaderNames(headerNames);
	}

	/**
	 * Writes a {@code Resource}, whereby the {@code resource} is written
	 * directly to the socket.
	 * 
	 * @param resource
	 *            the resource to be written
	 * 
	 * @throws IOException
	 *             if the resource cannot be written
	 */
	public void writeResource(final byte[] resource) throws IOException {
		write(ResponseType.RESOURCE, resource);
	}

	/**
	 * Reads the next value retrieved. The method blocks until the next value is
	 * read.
	 * 
	 * @return the value read
	 * 
	 * @throws IOException
	 *             if an exception occurred during the read
	 */
	public RetrievedValue read() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);

		return value;
	}

	/**
	 * Reads the credential.
	 * 
	 * @return the read credential
	 * @throws IOException
	 *             if an error occurred on client- or server-side, or if the
	 *             retrieved value is not a credential
	 */
	public String[] readCredential() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);

		if (value instanceof ChunkedRetrievedValue) {
			return ((ChunkedRetrievedValue) value).getCredentials();
		} else {
			return new String[] { value.getString(), "" };
		}
	}

	/**
	 * Reads a message.
	 * 
	 * @return the read message
	 * 
	 * @throws IOException
	 *             if an error occurred on client- or server-side, or if the
	 *             retrieved value is not a message
	 */
	public String readMessage() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);
		return value.getMessage();
	}

	/**
	 * Reads a resource-demand.
	 * 
	 * @return the read resource-demand
	 * 
	 * @throws IOException
	 *             if an error occurred on client- or server-side, or if the
	 *             retrieved value is not a resource-demand
	 */
	public String readResourceDemand() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);
		return value.getResourceDemand();
	}

	/**
	 * Reads a resource.
	 * 
	 * @return the read resource
	 * 
	 * @throws IOException
	 *             if an error occurred on client- or server-side, or if the
	 *             retrieved value is not a resource
	 */
	public byte[] readResource() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);

		if (value.isCancel()) {
			return null;
		} else {
			return value.getResource();
		}
	}

	/**
	 * Reads a header.
	 * 
	 * @return the read header
	 * 
	 * @throws IOException
	 *             if an error occurred on client- or server-side, or if the
	 *             retrieved value is not a header
	 */
	public DataType[] readHeader() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);
		return value.getHeader();
	}

	/**
	 * Reads a {@code QueryType}.
	 * 
	 * @return the read {@code QueryType}
	 * 
	 * @throws IOException
	 *             if an error occurred on client- or server-side, or if the
	 *             retrieved value is not a {@code QueryType}
	 */
	public QueryType readQueryType() throws IOException {
		final byte marker = is.readByte();

		final QueryType queryType = QueryType.find(marker);

		// check if we got an exception
		if (queryType == null) {
			final RetrievedValue value = _read(marker);
			checkException(value);
		}

		return queryType;
	}

	/**
	 * Reads a {@code QueryStatus}.
	 * 
	 * @return the read {@code QueryStatus}
	 * 
	 * @throws IOException
	 *             if an error occurred on client- or server-side, or if the
	 *             retrieved value is not a {@code QueryStatus}
	 */
	public QueryStatus readQueryStatus() throws IOException {
		final byte marker = is.readByte();
		final QueryStatus queryStatus = QueryStatus.find(marker);

		// check if we got an exception
		if (queryStatus == null) {
			final RetrievedValue value = _read(marker);
			checkException(value);
		}

		return queryStatus;
	}

	/**
	 * Writes a message (i.e. {@link ResponseType#MESSAGE}) to the other side.
	 * 
	 * @param msg
	 *            the message to be written
	 * 
	 * @throws IOException
	 *             if the message cannot be written
	 */
	public void writeMessage(final String msg) throws IOException {
		write(ResponseType.MESSAGE, msg.getBytes("UTF8"));
	}

	/**
	 * Writes the {@code bytes} of the specified {@code type}.
	 * 
	 * @param type
	 *            the type of the {@code bytes} to be send
	 * @param bytes
	 *            the data to be send
	 * 
	 * @throws IOException
	 *             if writing fails
	 */
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

	/**
	 * Writes the {@code QueryType} to the socket.
	 * 
	 * @param type
	 *            the {@code QueryType} to be written
	 * 
	 * @throws IOException
	 *             if the {@code QueryType} cannot be written
	 */
	public void writeQueryType(final QueryType type) throws IOException {
		os.writeByte(type.getId());
		os.flush();
	}

	/**
	 * Writes the {@code QueryStatus} to the socket.
	 * 
	 * @param status
	 *            the {@code QueryStatus} to be written
	 * 
	 * @throws IOException
	 *             if the {@code QueryStatus} cannot be written
	 */
	public void writeQueryStatus(final QueryStatus status) throws IOException {
		os.writeByte(status.getId());
		os.flush();
	}

	/**
	 * Initializes a communication with the server for the specified {@code msg}
	 * . The retrieved information from the server during the initialization are
	 * evaluated by {@code this} as well as the {@code handler}.
	 * 
	 * @param msg
	 *            the message to be initialized
	 * @param handler
	 *            the handler used to determine if the queryType should be
	 *            processed (using
	 *            {@link IResponseHandler#doHandleQueryType(QueryType)}), can be
	 *            {@code null} if so the query will be processed for sure
	 * 
	 * @return {@code true} if the initialization is valid and accepted by the
	 *         server and the client, otherwise {@code false}
	 * 
	 * @throws IOException
	 *             if an unexpected communication error occurs
	 */
	public boolean initializeCommunication(final String msg,
			final IResponseHandler handler) throws IOException {

		// finish any old communication
		if (inCommunication) {
			throw new IllegalStateException(
					"Cannot initialize any new connection, while another communication is running, make sure the connection is closed correctly.");
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

	/**
	 * Marks any currently communication as finalized. This method should only
	 * be called by an external method, if an exception occurred, which leads to
	 * the end of the communication. The next method of the server should be a
	 * response to a new communication.
	 * 
	 * @param handler
	 */
	public void markCommunicationAsFinal(final IResponseHandler handler) {
		if (handler != null) {
			handler.signalEORReached();
		}

		inCommunication = false;
	}

	/**
	 * Writes the specified {@code msg} and handles the response to it using the
	 * specified {@code handler}. The response is only handled once, i.e.
	 * additional reads are needed if the end of the response is not reached
	 * with one read. The method returns {@code true} if the message was sent
	 * successfully, otherwise {@code false}.
	 * 
	 * @param msg
	 *            the message to be send
	 * @param handler
	 *            the handler to handle the response with
	 * 
	 * @return {@code true} if the message was sent successfully, otherwise
	 *         {@code false}
	 * 
	 * @throws IOException
	 *             if an exception occurres during handling
	 */
	public boolean writeAndHandle(final String msg,
			final IResponseHandler handler) throws IOException {
		if (initializeCommunication(msg, handler)) {
			handleResponse(handler);

			return true;
		} else {
			return false;
		}
	}

	/**
	 * This method is only used by the server side which expects a message to
	 * arrive and nothing else. If something arrived on the input side, this
	 * method will check if it's a message {@code false} or a cancel statement
	 * {@code true}. If nothing is available {@code null} is returned. <br/>
	 * <br/>
	 * <b>Note:</b> The retrieved message will be discarded if no array of
	 * length 1 or more is passed
	 * 
	 * @param message
	 *            container to get the message send (if one was send)
	 * 
	 * @return {@code true} if the current handling is canceled, the client-side
	 *         expects to send a {@code ResponseType#EOR} as fast as possible,
	 *         {@code false} if another message was send (the message can be
	 *         retrieved by passing an at least 1-length array)
	 * 
	 * @throws IOException
	 */
	public Boolean peekForCancel(final String[] message) throws IOException {

		if (is.available() > 0) {
			final RetrievedValue value = read();
			checkException(value);

			if (value.isCancel()) {
				return true;
			} else if (value.is(ResponseType.MESSAGE)) {
				if (message != null && message.length != 0) {
					message[0] = value.getMessage();
				}

				return false;
			} else {
				throw new IllegalStateException(
						"Retrieving anything different from a message or a cancel statement.");
			}
		} else {
			return null;
		}
	}

	/**
	 * Waits for a message to be send on the input. All cancellations are
	 * ignored and any other retrieval leads to an exception.
	 * 
	 * @return the read message
	 * 
	 * @throws IOException
	 *             if an error occurres during the read
	 */
	public String waitForMessage() throws IOException {
		final RetrievedValue value = _read();
		checkException(value);

		// ignore anything canceling
		if (value.isCancel()) {
			return waitForMessage();
		} else {
			return value.getMessage();
		}
	}

	/**
	 * Handles the next item of or the complete response using the specified
	 * {@code handler}. The method returns {@code true} if the end of the
	 * response was reached, otherwise {@code false} is returned, which means
	 * that more data has to be handled to process the response complete.
	 * 
	 * @param handler
	 *            the {@code ResponseHandler} used to handle the (next) response
	 * 
	 * @return {@code true} if the end of the response was reached, otherwise
	 *         {@code false}
	 * 
	 * @throws IOException
	 *             if the response could not be handled
	 */
	public boolean handleResponse(final IResponseHandler handler)
			throws IOException {

		boolean eorReached = false;
		boolean read = true;
		while (read) {
			final RetrievedValue value = read();

			// write the cancellation if the thread is interrupted
			if (Thread.interrupted()) {
				writeCancellation();

				/*
				 * Skip any resource demand at this point, the cancellation is
				 * send. Any additional write is not expected at this point by
				 * the server.
				 */
				if (value.is(ResponseType.RESOURCE_DEMAND)) {
					continue;
				}
			}

			if (value.isEOR()) {
				read = false;
				eorReached = true;
			} else if (value.isCancel()) {
				throw new IllegalStateException(
						"Cancellation cannot be used within a query handling, it can only be used by the client-side to interrupt at server-side.");
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
					handler.handleResult(value.getType(), value.getHeader());
				}
			} else if (value.is(ResponseType.HEADERNAMES)) {
				if (handler == null) {
					// nothing to do
				} else if (value instanceof ChunkedRetrievedValue) {
					handler.handleResult(value.getType(),
							((ChunkedRetrievedValue) value).getHeaderNames());
				} else {
					handler.handleResult(value.getType(),
							new String[] { value.getString() });
				}
			} else if (value.is(ResponseType.RESULT)) {
				if (handler == null) {
					throw new IllegalStateException(
							"Cannot read a result without any header.");
				} else {
					final Object[] result = readResult(handler.getHeader());
					read = handler.handleResult(value.getType(), result);
				}
			} else if (value.is(ResponseType.INT)
					|| value.is(ResponseType.INT_ARRAY)) {
				if (handler != null) {
					read = handler.handleResult(value.getType(),
							value.getIntegers());
				}
			} else if (value.is(ResponseType.EOM)) {
				if (handler != null) {
					read = handler.handleResult(value.getType(), null);
				}
			} else if (value.is(ResponseType.CREDENTIALS)) {
				throw new IllegalStateException(
						"Credentials cannot be part of a response.");
			} else {
				throw new IllegalStateException("Cannot handle the result '"
						+ value + "'.");
			}
		}

		if (eorReached) {
			markCommunicationAsFinal(handler);
		}

		return eorReached;
	}

	/**
	 * Checks if an exception was thrown on the other side of the communication.
	 * If so the exception is thrown on {@code this} side as well, using a
	 * {@code WrappedException}.
	 * 
	 * @param value
	 *            the {@code RetrievedValue} to be checked for an exception
	 * 
	 * @throws WrappedException
	 *             the exception found, if one was found
	 */
	protected void checkException(final RetrievedValue value)
			throws WrappedException {
		if (value.is(ResponseType.EXCEPTION)) {
			try {
				throw new WrappedException(new String(value.getBytes(), "UTF8"));
			} catch (final UnsupportedEncodingException e) {
				// ignore
			}
		}
	}

	/**
	 * Internally used method to read the next {@code RetrievedValue} from the
	 * input.
	 * 
	 * @return the {@code RetrievedValue} of the read
	 * 
	 * @throws IOException
	 *             if the type cannot be read
	 * 
	 * @see ResponseType
	 */
	protected RetrievedValue _read() throws IOException {
		return _read(is.readByte());
	}

	/**
	 * Internally used method to read bytes from the input of the specified
	 * {@code ResponseType}.
	 * 
	 * @param typeId
	 *            the type to be read
	 * 
	 * @return the {@code RetrievedValue} of the read
	 * 
	 * @throws IOException
	 *             if the type cannot be read
	 * 
	 * @see ResponseType
	 */
	protected RetrievedValue _read(final byte typeId) throws IOException {
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
		markCommunicationAsFinal(null);

		this.is.close();
		this.os.close();
	}

	@Override
	public int hashCode() {
		return is.hashCode();
	}
}
