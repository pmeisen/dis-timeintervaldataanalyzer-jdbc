package net.meisen.dissertation;

import net.meisen.dissertation.jdbc.TestTidaConnection;
import net.meisen.dissertation.jdbc.TestTidaDriver;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * All tests together as a {@link Suite}
 * 
 * @author pmeisen
 * 
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ TestTidaDriver.class, TestTidaConnection.class })
public class AllTests {
	// nothing more to do here
}
