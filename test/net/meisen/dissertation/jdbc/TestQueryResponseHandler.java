package net.meisen.dissertation.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import net.meisen.dissertation.jdbc.protocol.QueryStatus;
import net.meisen.dissertation.jdbc.protocol.QueryType;
import net.meisen.general.genmisc.types.Files;
import net.meisen.general.genmisc.types.Streams;

import org.junit.Test;

/**
 * Tests the implementation of the {@code QueryResponseHandler}.
 * 
 * @author pmeisen
 * 
 */
public class TestQueryResponseHandler {

	/**
	 * Tests the loading of resources via classpath or from the file-system.
	 * 
	 * @throws IOException
	 *             if the test cannot be run
	 */
	@Test
	public void testResourceResolving() throws IOException {

		/*
		 * Test-setup
		 */
		// create the expected content as string
		final String expected = Streams.readFromStream(getClass()
				.getResourceAsStream(
						"/net/meisen/dissertation/model/testNumberModel.xml"));

		// create a file with the same content
		final File file = new File(System.getProperty("java.io.tmpdir"), UUID
				.randomUUID().toString());
		Streams.copyStreamToFile(
				getClass().getResourceAsStream(
						"/net/meisen/dissertation/model/testNumberModel.xml"),
				file);

		/*
		 * Testing
		 */
		try {
			final QueryResponseHandler qrh = new QueryResponseHandler();
			InputStream is;

			// from classpath
			is = qrh.getResourceStream(QueryResponseHandler.PREFIX_CLASSPATH
					+ ":/net/meisen/dissertation/model/testNumberModel.xml");
			assertNotNull(is);
			assertEquals(expected, Streams.readFromStream(is));

			is = qrh.getResourceStream(QueryResponseHandler.PREFIX_CLASSPATH
					+ ":net/meisen/dissertation/model/testNumberModel.xml");
			assertNotNull(is);
			assertEquals(expected, Streams.readFromStream(is));

			is = qrh.getResourceStream(QueryResponseHandler.PREFIX_CLASSPATH
					+ "://net/meisen/dissertation/model/testNumberModel.xml");
			assertNotNull(is);
			assertEquals(expected, Streams.readFromStream(is));

			// from file
			is = qrh.getResourceStream(Files.getCanonicalPath(file));
			assertNotNull(is);
			assertEquals(expected, Streams.readFromStream(is));

			is = qrh.getResourceStream(QueryResponseHandler.PREFIX_FILE + "://"
					+ Files.getCanonicalPath(file).replace("\\", "/"));
			assertNotNull(is);
			assertEquals(expected, Streams.readFromStream(is));

			is = qrh.getResourceStream(QueryResponseHandler.PREFIX_FILE + "://"
					+ Files.getCanonicalPath(file));
			assertNotNull(is);
			assertEquals(expected, Streams.readFromStream(is));
		} finally {
			assertTrue(file.delete());
		}
	}

	/**
	 * Tests the implementation of
	 * {@code QueryResponseHandler#doHandleQueryType(QueryType)}.
	 */
	@Test
	public void testDoHandleQueryType() {
		final QueryResponseHandler handler = new QueryResponseHandler();

		// check the valid retrieval of identifiers with modify
		handler.setExpectedResultSetType(TidaResultSetType.MODIFY);
		handler.setQueryStatus(QueryStatus.PROCESSANDGETIDS);
		assertEquals(QueryStatus.PROCESSANDGETIDS,
				handler.doHandleQueryType(QueryType.MANIPULATION));
		assertEquals(TidaResultSetType.MODIFY, handler.getResultSetType());

		// check the valid processing with a manipulation
		handler.setExpectedResultSetType(TidaResultSetType.MODIFY);
		handler.setQueryStatus(QueryStatus.PROCESS);
		assertEquals(QueryStatus.PROCESS,
				handler.doHandleQueryType(QueryType.MANIPULATION));
		assertEquals(TidaResultSetType.MODIFY, handler.getResultSetType());

		// check the valid processing with a query
		handler.setExpectedResultSetType(TidaResultSetType.QUERY);
		handler.setQueryStatus(QueryStatus.PROCESS);
		assertEquals(QueryStatus.PROCESS,
				handler.doHandleQueryType(QueryType.QUERY));
		assertEquals(TidaResultSetType.QUERY, handler.getResultSetType());

		// check invalid processing with identifiers with a query
		handler.setExpectedResultSetType(TidaResultSetType.QUERY);
		handler.setQueryStatus(QueryStatus.PROCESSANDGETIDS);
		assertEquals(QueryStatus.CANCEL,
				handler.doHandleQueryType(QueryType.QUERY));
		assertEquals(TidaResultSetType.QUERY, handler.getResultSetType());

		// check default invalidation with query
		handler.setExpectedResultSetType(TidaResultSetType.QUERY);
		handler.setQueryStatus(QueryStatus.CANCEL);
		assertEquals(QueryStatus.CANCEL,
				handler.doHandleQueryType(QueryType.QUERY));
		assertEquals(TidaResultSetType.QUERY, handler.getResultSetType());

		// check default invalidation with manipulation
		handler.setExpectedResultSetType(TidaResultSetType.UNKNOWN);
		handler.setQueryStatus(QueryStatus.CANCEL);
		assertEquals(QueryStatus.CANCEL,
				handler.doHandleQueryType(QueryType.MANIPULATION));
		assertEquals(TidaResultSetType.MODIFY, handler.getResultSetType());

		// check unknown expectation and valid result with manipulation
		handler.setExpectedResultSetType(TidaResultSetType.UNKNOWN);
		handler.setQueryStatus(QueryStatus.PROCESS);
		assertEquals(QueryStatus.PROCESS,
				handler.doHandleQueryType(QueryType.MANIPULATION));
		assertEquals(TidaResultSetType.MODIFY, handler.getResultSetType());

		// check unknown expectation and valid result with manipulation
		handler.setExpectedResultSetType(TidaResultSetType.UNKNOWN);
		handler.setQueryStatus(QueryStatus.PROCESSANDGETIDS);
		assertEquals(QueryStatus.PROCESSANDGETIDS,
				handler.doHandleQueryType(QueryType.MANIPULATION));
		assertEquals(TidaResultSetType.MODIFY, handler.getResultSetType());

		// check unknown expectation and valid result with query
		handler.setExpectedResultSetType(TidaResultSetType.UNKNOWN);
		handler.setQueryStatus(QueryStatus.PROCESS);
		assertEquals(QueryStatus.PROCESS,
				handler.doHandleQueryType(QueryType.QUERY));
		assertEquals(TidaResultSetType.QUERY, handler.getResultSetType());

		// check unknown expectation and invalid result with query
		handler.setExpectedResultSetType(TidaResultSetType.UNKNOWN);
		handler.setQueryStatus(QueryStatus.PROCESSANDGETIDS);
		assertEquals(QueryStatus.CANCEL,
				handler.doHandleQueryType(QueryType.QUERY));
		assertEquals(TidaResultSetType.QUERY, handler.getResultSetType());
	}
}
