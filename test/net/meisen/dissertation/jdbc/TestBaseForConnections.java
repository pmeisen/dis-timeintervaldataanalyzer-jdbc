package net.meisen.dissertation.jdbc;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

import net.meisen.dissertation.server.TidaServer;
import net.meisen.general.genmisc.types.Files;

import org.junit.After;
import org.junit.Before;

/**
 * A test base used to start a server.
 * 
 * @author pmeisen
 * 
 */
public abstract class TestBaseForConnections {

	/**
	 * The started server instance.
	 */
	protected TidaServer server;

	/**
	 * Start a test-server for testing purposes.
	 */
	@Before
	public void startServer() {
		final Properties properties = new Properties();

		if (createTemporaryFolder()) {
			Files.deleteOnExitDir(
					new File(System.getProperty("java.io.tmpdir")),
					getTestLocation() + "-.*");

			// create the directory
			assertTrue(getTemporaryFolder().exists()
					|| getTemporaryFolder().mkdirs());
		}

		if (getConfig() != null) {
			properties.setProperty("tida.config.selector", getConfig());

			if (createTemporaryFolder()) {
				properties.setProperty("tida.config.test.location",
						Files.getCanonicalPath(getTemporaryFolder()));
			} else {
				properties.setProperty("tida.config.test.location",
						"!please activate temporary folder creation!");
			}
		}
		properties.setProperty("tida.server.tsql.port", "" + getPort());
		properties.setProperty("tida.server.tsql.enabled", "true");
		properties.setProperty("tida.server.http.enabled", "false");

		server = TidaServer.create(properties);
		server.startAsync();
	}

	/**
	 * CleanUp after the test.
	 */
	@After
	public void shutdownServer() {
				
		if (server != null) {
			server.shutdown(true);
		}

		if (createTemporaryFolder()) {
			Files.deleteOnExitDir(
					new File(System.getProperty("java.io.tmpdir")),
					getTestLocation() + "-.*");
		}
	}

	/**
	 * Defines if a temporary directory should be created for the test.
	 * 
	 * @return {@code true} if a temporary directory should be created,
	 *         otherwise {@code false}
	 */
	public boolean createTemporaryFolder() {
		return true;
	}

	/**
	 * Gets the location of the temporary folder created for the test. The
	 * location is also available using the property
	 * {@code tida.config.test.location}.
	 * 
	 * @return the location of the temporary folder created for the test
	 */
	public File getTemporaryFolder() {
		return new File(System.getProperty("java.io.tmpdir"), getTestLocation()
				+ "-" + UUID.randomUUID().toString());
	}

	/**
	 * A folder which can be used by a test to store configuration information,
	 * e.g. modify the default location of the model-data.
	 * 
	 * @return folder to be used for the test
	 */
	public String getTestLocation() {
		return getClass().getSimpleName();
	}

	/**
	 * Gets the used port of the server.
	 * 
	 * @return the used port of the server
	 */
	public int getPort() {
		return 6666;
	}

	/**
	 * Gets the selector for the configuration of the test.
	 * 
	 * @return the configuration used for the test
	 */
	public String getConfig() {
		return null;
	}

	/**
	 * Gets the JDBC-URL to connect to the server.
	 * 
	 * @return the JDBC-URL to connect to the server
	 */
	public String getJdbc() {
		return "jdbc:tida://localhost:" + getPort();
	}
}
