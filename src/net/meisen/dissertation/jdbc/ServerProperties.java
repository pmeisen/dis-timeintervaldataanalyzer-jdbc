package net.meisen.dissertation.jdbc;

public class ServerProperties {
	private final int port;
	private final String host;

	public ServerProperties(final int port, final String host) {
		this.port = port;
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public String getHost() {
		return host;
	}
	
	
}
