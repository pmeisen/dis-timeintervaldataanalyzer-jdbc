package net.meisen.dissertation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import net.meisen.dissertation.jdbc.protocol.DataType;
import net.meisen.dissertation.jdbc.protocol.IResponseHandler;
import net.meisen.dissertation.jdbc.protocol.Protocol;
import net.meisen.dissertation.jdbc.protocol.QueryStatus;
import net.meisen.dissertation.jdbc.protocol.QueryType;
import net.meisen.dissertation.jdbc.protocol.ResponseType;
import net.meisen.dissertation.jdbc.protocol.RetrievedValue;
import net.meisen.dissertation.jdbc.protocol.WrappedException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the implemented {@code Protocol}.
 * 
 * @author pmeisen
 * 
 */
public class TestProtocol {
	private static interface ITestHandler {
		public void answer(final int msgNr, final RetrievedValue val,
				final Protocol serverSideProtocol) throws IOException;
	}

	private static class TestResponseHandler implements IResponseHandler {

		@Override
		public InputStream getResourceStream(final String resource) {
			fail("Not expected!");
			return null;
		}

		@Override
		public boolean handleResult(final ResponseType type,
				final Object[] result) {
			fail("Not expected!");

			return false;
		}

		@Override
		public DataType[] getHeader() {
			fail("Not expected!");
			return null;
		}

		@Override
		public void signalEORReached() {
			// nothing to do
		}

		@Override
		public QueryStatus doHandleQueryType(final QueryType queryType) {
			return QueryStatus.PROCESS;
		}

		@Override
		public void resetHandler() {
			// nothing to be reseted
		}
	}

	private Thread serverThread;
	private Socket clientSideSocket;
	private Protocol clientSideProtocol;

	private ITestHandler serverHandler;
	private int testCounter = 0;

	/**
	 * Initializes an infrastructure for testing the communication.
	 * 
	 * @throws Exception
	 *             if an unexpected problem occurs
	 */
	@Before
	public void init() throws Exception {

		serverThread = new Thread() {

			@Override
			public void run() {
				try {
					final ServerSocket serverSocket = new ServerSocket(6060);
					final Socket serverSideSocket = serverSocket.accept();
					final Protocol serverSideProtocol = new Protocol(
							serverSideSocket);

					int nr = 0;
					while (!isInterrupted()) {

						// get the message to be handled
						final RetrievedValue val = serverSideProtocol.read();
						if (val.is(ResponseType.MESSAGE)
								&& "END".equals(val.getMessage())) {
							continue;
						}

						// tell the client what type of message was send
						serverSideProtocol.writeQueryType(QueryType.QUERY);

						// read the status if we should proceed
						final QueryStatus status = serverSideProtocol
								.readQueryStatus();
						assertNotNull(status);

						// decide
						if (QueryStatus.PROCESS.equals(status)) {
							serverHandler.answer(nr, val, serverSideProtocol);
						} else {
							serverSideProtocol.writeEndOfResponse();
						}
						nr++;
					}

					serverSideProtocol.close();
					serverSideSocket.close();
					serverSocket.close();
				} catch (final IOException e) {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}
		};
		serverThread.start();

		clientSideSocket = new Socket("localhost", 6060);
		clientSideProtocol = new Protocol(clientSideSocket);
	}

	/**
	 * Tests the sending and receiving of the header-schema.
	 * 
	 * @throws Exception
	 *             if an unexpected problem occurs
	 */
	@Test
	public void testProtocolHeader() throws Exception {
		serverHandler = new ITestHandler() {

			@Override
			public void answer(final int msgNr, final RetrievedValue val,
					final Protocol serverSideProtocol) throws IOException {
				final Class<?>[] c;

				switch (msgNr) {
				case 0:
					c = new Class<?>[] { int.class };
					break;
				case 1:
					c = new Class<?>[] { Byte.class, byte.class, Short.class,
							short.class, Integer.class, int.class, Long.class,
							long.class, String.class, Date.class, double.class,
							Double.class };
					break;
				case 2:
					c = new Class<?>[] { UUID.class };
					break;
				default:
					fail("Unexpected message");
					return;
				}

				try {
					serverSideProtocol.writeHeader(c);
				} catch (final Exception e) {
					serverSideProtocol.writeException(e);
				}
				serverSideProtocol.writeEndOfResponse();
			}
		};

		// let's read some headers
		final IResponseHandler clientHandler = new TestResponseHandler() {

			@Override
			public boolean handleResult(final ResponseType type,
					final Object[] result) {
				assertEquals(ResponseType.HEADER, type);
				final DataType[] header = (DataType[]) result;

				switch (testCounter) {
				case 0:
					assertEquals(1, header.length);
					assertEquals(Integer.class, header[0].getRepresentorClass());
					break;
				case 1:
					assertEquals(12, header.length);
					assertEquals(Byte.class, header[0].getRepresentorClass());
					assertEquals(Byte.class, header[1].getRepresentorClass());
					assertEquals(Short.class, header[2].getRepresentorClass());
					assertEquals(Short.class, header[3].getRepresentorClass());
					assertEquals(Integer.class, header[4].getRepresentorClass());
					assertEquals(Integer.class, header[5].getRepresentorClass());
					assertEquals(Long.class, header[6].getRepresentorClass());
					assertEquals(Long.class, header[7].getRepresentorClass());
					assertEquals(String.class, header[8].getRepresentorClass());
					assertEquals(Date.class, header[9].getRepresentorClass());
					assertEquals(Double.class, header[10].getRepresentorClass());
					assertEquals(Double.class, header[11].getRepresentorClass());
					break;
				default:
					fail("No test result defined for test: " + testCounter);
				}

				return true;
			}
		};

		// simple one class
		{
			clientSideProtocol.writeAndHandle("0", clientHandler);
			testCounter++;
		}

		// all available classes
		{
			clientSideProtocol.writeAndHandle("1", clientHandler);
			testCounter++;
		}

		// exception with unsupported type
		{
			boolean exception = false;
			try {
				clientSideProtocol.writeAndHandle("2", clientHandler);
			} catch (final Exception e) {
				exception = true;
				assertEquals(WrappedException.class, e.getClass());
				assertTrue(
						e.getMessage(),
						e.getMessage().contains(
								"Unsupported header-type '"
										+ UUID.class.getName() + "'"));
			}
			assertTrue(exception);
			testCounter++;
		}
	}

	/**
	 * Tests the sending and receiving of the int-values.
	 * 
	 * @throws Exception
	 *             if an unexpected problem occurs
	 */
	@Test
	public void testProtocolInt() throws Exception {
		serverHandler = new ITestHandler() {

			@Override
			public void answer(final int msgNr, final RetrievedValue val,
					final Protocol serverSideProtocol) throws IOException {

				for (int i = 0; i < 1000; i++) {
					serverSideProtocol.writeInt(i);
				}
				serverSideProtocol.writeEndOfResponse();
			}
		};

		// let's read some headers
		final IResponseHandler clientHandler = new TestResponseHandler() {

			int nr = 0;

			@Override
			public boolean handleResult(final ResponseType type,
					final Object[] result) {

				assertTrue(ResponseType.INT.equals(type));
				assertEquals(1, result.length);
				assertTrue(result[0] instanceof Integer);
				assertEquals(nr, result[0]);

				testCounter = nr;
				nr++;

				// keep reading
				return true;
			}
		};

		clientSideProtocol.writeAndHandle("0", clientHandler);
		assertEquals(999, testCounter);
	}

	/**
	 * Tests the sending of integers.
	 * 
	 * @throws Exception
	 *             if an unexpected exception occurrs
	 */
	@Test
	public void testProtocolInts() throws Exception {
		serverHandler = new ITestHandler() {

			@Override
			public void answer(final int msgNr, final RetrievedValue val,
					final Protocol serverSideProtocol) throws IOException {

				final Random rnd = new Random();

				for (int i = 0; i < 1000; i++) {
					final int[] values = new int[i];
					for (int k = 0; k < i; k++) {
						values[k] = rnd.nextInt();
					}
					serverSideProtocol.writeInts(values);
				}
				serverSideProtocol.writeEndOfResponse();
			}
		};

		// let's read some headers
		final IResponseHandler clientHandler = new TestResponseHandler() {

			int nr = 0;

			@Override
			public boolean handleResult(final ResponseType type,
					final Object[] result) {

				assertTrue(ResponseType.INT_ARRAY.equals(type));
				assertEquals(nr, result.length);

				testCounter = nr;
				nr++;

				// keep reading
				return true;
			}
		};

		clientSideProtocol.writeAndHandle("0", clientHandler);
		assertEquals(999, testCounter);
	}

	/**
	 * Tests the implementation of the protocol to send and receive
	 * header-names.
	 * 
	 * @throws Exception
	 *             if an unexpected problem occurs
	 */
	@Test
	public void testProtocolHeaderNames() throws Exception {
		serverHandler = new ITestHandler() {

			@Override
			public void answer(final int msgNr, final RetrievedValue val,
					final Protocol serverSideProtocol) throws IOException {
				final String[] headerNames;

				switch (msgNr) {
				case 0:
					headerNames = new String[] {};
					break;
				case 1:
					headerNames = new String[] { "First", "Änother",
							"What so ever" };
					break;
				default:
					fail("Unexpected message");
					return;
				}

				serverSideProtocol.writeHeaderNames(headerNames);
				serverSideProtocol.writeEndOfResponse();
			}
		};

		final IResponseHandler clientHandler = new TestResponseHandler() {

			@Override
			public boolean handleResult(final ResponseType type,
					final Object[] result) {
				assertEquals(ResponseType.HEADERNAMES, type);
				final String[] headerNames = (String[]) result;

				switch (testCounter) {
				case 0:
					assertEquals(0, headerNames.length);
					break;
				case 1:
					assertEquals(3, headerNames.length);
					assertEquals("First", headerNames[0]);
					assertEquals("Änother", headerNames[1]);
					assertEquals("What so ever", headerNames[2]);
					break;
				default:
					fail("No test result defined for test: " + testCounter);
				}

				return true;
			}
		};

		// no names
		{
			clientSideProtocol.writeAndHandle("0", clientHandler);
			testCounter++;
		}

		// several names
		{
			clientSideProtocol.writeAndHandle("1", clientHandler);
			testCounter++;
		}
	}

	/**
	 * Tests the stepwise reading of data.
	 * 
	 * @throws Exception
	 *             if an unexpected problem occurs
	 */
	@Test
	public void testDrippinRead() throws Exception {

		serverHandler = new ITestHandler() {

			@Override
			public void answer(final int msgNr, final RetrievedValue val,
					final Protocol serverSideProtocol) throws IOException {

				for (int i = 0; i < 1000; i++) {
					serverSideProtocol.writeResult(
							new DataType[] { DataType.STRING },
							new Object[] { (msgNr + "-" + i) });
				}
				serverSideProtocol.writeEndOfResponse();
			}
		};

		final IResponseHandler clientHandler = new TestResponseHandler() {

			@Override
			public boolean handleResult(final ResponseType type,
					final Object[] result) {

				final String msg = (String) result[0];
				assertNotNull(msg);
				assertEquals("0-" + testCounter, msg);

				// do not read the next result
				return false;
			}

			@Override
			public DataType[] getHeader() {
				return new DataType[] { DataType.STRING };
			}
		};

		assertTrue(clientSideProtocol.initializeCommunication("0",
				clientHandler));
		for (int i = 0; i < 1000; i++) {
			testCounter = i;
			clientSideProtocol.handleResponse(clientHandler);
		}
	}

	/**
	 * Tests the reading and writing of results.
	 * 
	 * @throws Exception
	 *             if an unexpected exception occurrs
	 */
	@Test
	public void testReadAndWriteResults() throws Exception {
		serverHandler = new ITestHandler() {

			@Override
			public void answer(final int msgNr, final RetrievedValue val,
					final Protocol serverSideProtocol) throws IOException {

				for (int i = 0; i < 1000; i++) {
					serverSideProtocol.writeResult(
							new DataType[] { DataType.STRING },
							new Object[] { (msgNr + "-" + i) });
				}
				serverSideProtocol.writeEndOfResponse();
			}
		};

		final IResponseHandler clientHandler = new TestResponseHandler() {

			@Override
			public boolean handleResult(final ResponseType type,
					final Object[] result) {

				final String msg = (String) result[0];
				assertNotNull(msg);
				assertEquals("0-" + testCounter, msg);

				// do not read the next result
				return false;
			}

			@Override
			public DataType[] getHeader() {
				return new DataType[] { DataType.STRING };
			}
		};

		assertTrue(clientSideProtocol.initializeCommunication("0",
				clientHandler));
		for (int i = 0; i < 1000; i++) {
			testCounter = i;
			clientSideProtocol.handleResponse(clientHandler);
		}
	}

	/**
	 * Cleans up behind the test.
	 * 
	 * @throws Exception
	 *             if an unexpected problem occurs
	 */
	@After
	public void cleanUp() throws Exception {

		// finish the server thread
		serverThread.interrupt();
		clientSideProtocol.writeMessage("END");
		serverThread.join();

		// cleanUp
		clientSideProtocol.close();
		clientSideSocket.close();
	}
}
