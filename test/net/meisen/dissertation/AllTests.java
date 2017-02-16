package net.meisen.dissertation;

import net.meisen.dissertation.jdbc.TestObjectArrayResultSet;
import net.meisen.dissertation.jdbc.TestQueryResponseHandler;
import net.meisen.dissertation.jdbc.TestTidaStatement;
import net.meisen.dissertation.jdbc.protocol.TestProtocol;
import net.meisen.dissertation.jdbc.version.TestVersion;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * All tests together as a {@link Suite}
 *
 * @author pmeisen
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({TestVersion.class,
        TestQueryResponseHandler.class,
        TestProtocol.class,
        TestObjectArrayResultSet.class, TestTidaStatement.class})
public class AllTests {
    // nothing more to do here
}
