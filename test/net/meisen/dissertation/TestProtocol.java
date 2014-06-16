package net.meisen.dissertation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

import net.meisen.dissertation.jdbc.protocol.ChunkedRetrievedValue;
import net.meisen.dissertation.jdbc.protocol.IResponseHandler;
import net.meisen.dissertation.jdbc.protocol.Protocol;
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
		public boolean handleResult(RetrievedValue value) {
			fail("Not expected!");

			return false;
		}

		@Override
		public void setHeaderNames(String[] header) {
			fail("Not expected!");
		}

		@Override
		public void setHeader(final Class<?>[] header) {
			fail("Not expected!");
		}

		@Override
		public void signalEORReached() {
			// nothing to do
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
						final RetrievedValue val = serverSideProtocol.read();
						serverHandler.answer(nr, val, serverSideProtocol);
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
					if ("END".equals(val.getMessage())) {
						return;
					}
					c = null;
				}

				try {
					serverSideProtocol.writeHeader(c);
				} catch (final Exception e) {
					serverSideProtocol.writeException(e);
				}
				serverSideProtocol.writeEndOfResult();
			}
		};

		// let's read some headers
		final IResponseHandler clientHandler = new TestResponseHandler() {

			@Override
			public void setHeader(final Class<?>[] header) {
				switch (testCounter) {
				case 0:
					assertEquals(1, header.length);
					assertEquals(Integer.class, header[0]);
					break;
				case 1:
					assertEquals(12, header.length);
					assertEquals(Byte.class, header[0]);
					assertEquals(Byte.class, header[1]);
					assertEquals(Short.class, header[2]);
					assertEquals(Short.class, header[3]);
					assertEquals(Integer.class, header[4]);
					assertEquals(Integer.class, header[5]);
					assertEquals(Long.class, header[6]);
					assertEquals(Long.class, header[7]);
					assertEquals(String.class, header[8]);
					assertEquals(Date.class, header[9]);
					assertEquals(Double.class, header[10]);
					assertEquals(Double.class, header[11]);
					break;
				default:
					fail("No test result defined for test: " + testCounter);
				}
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
				if (msgNr > 0 && "END".equals(val.getMessage())) {
					return;
				}

				for (int i = 0; i < 1000; i++) {
					serverSideProtocol.writeInt(i);
				}
				serverSideProtocol.writeEndOfResult();
			}
		};

		// let's read some headers
		final IResponseHandler clientHandler = new TestResponseHandler() {

			int nr = 0;

			@Override
			public boolean handleResult(final RetrievedValue value) {

				assertTrue(value.type.equals(ResponseType.INT));
				try {
					assertEquals(nr, value.getInt());
				} catch (final IOException e) {
					fail(e.getMessage());
				}

				testCounter = nr;
				nr++;

				// keep reading
				return true;
			}
		};

		clientSideProtocol.writeAndHandle("0", clientHandler);
		assertEquals(999, testCounter);
	}

	@Test
	public void testProtocolInts() throws Exception {
		serverHandler = new ITestHandler() {

			@Override
			public void answer(final int msgNr, final RetrievedValue val,
					final Protocol serverSideProtocol) throws IOException {
				if (msgNr > 0 && "END".equals(val.getMessage())) {
					return;
				}

				final Random rnd = new Random();

				for (int i = 0; i < 1000; i++) {
					final int[] values = new int[i];
					for (int k = 0; k < i; k++) {
						values[k] = rnd.nextInt();
					}
					serverSideProtocol.writeInts(values);
				}
				serverSideProtocol.writeEndOfResult();
			}
		};

		// let's read some headers
		final IResponseHandler clientHandler = new TestResponseHandler() {

			int nr = 0;

			@Override
			public boolean handleResult(final RetrievedValue value) {

				assertTrue(value.type.equals(ResponseType.INT_ARRAY));
				assertTrue(value instanceof ChunkedRetrievedValue);
				try {
					assertEquals(nr, value.getInts().length);
				} catch (final IOException e) {
					fail(e.getMessage());
				}

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
					if ("END".equals(val.getMessage())) {
						return;
					}
					headerNames = null;
				}

				serverSideProtocol.writeHeaderNames(headerNames);
				serverSideProtocol.writeEndOfResult();
			}
		};

		final IResponseHandler clientHandler = new TestResponseHandler() {

			@Override
			public void setHeaderNames(final String[] headerNames) {
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
					serverSideProtocol.writeResult((msgNr + "-" + i)
							.getBytes("UTF8"));
				}
				serverSideProtocol.writeEndOfResult();
			}
		};

		final IResponseHandler clientHandler = new TestResponseHandler() {

			@Override
			public boolean handleResult(final RetrievedValue value) {

				final String msg;
				try {
					msg = new String(value.bytes, "UTF8");
				} catch (final UnsupportedEncodingException e) {
					fail(e.getMessage());
					return false;
				}

				assertNotNull(msg);
				assertEquals("0-" + testCounter, msg);

				// do not read the next result
				return false;
			}
		};

		clientSideProtocol.write("0");
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
		clientSideProtocol.write("END");
		serverThread.join();

		// cleanUp
		clientSideProtocol.close();
		clientSideSocket.close();
	}
}
