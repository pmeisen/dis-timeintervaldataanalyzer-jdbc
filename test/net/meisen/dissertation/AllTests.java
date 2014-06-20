package net.meisen.dissertation;

import net.meisen.dissertation.jdbc.TestQueryResponseHandler;
import net.meisen.dissertation.jdbc.TestTidaConnection;
import net.meisen.dissertation.jdbc.TestTidaDriver;
import net.meisen.dissertation.jdbc.TestTidaResultSet;
import net.meisen.dissertation.jdbc.TestTidaStatement;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * All tests together as a {@link Suite}
 * 
 * @author pmeisen
 * 
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ TestQueryResponseHandler.class, TestProtocol.class,
		TestTidaDriver.class, TestTidaConnection.class,
		TestTidaStatement.class, TestTidaResultSet.class })
public class AllTests {
	// nothing more to do here
}
