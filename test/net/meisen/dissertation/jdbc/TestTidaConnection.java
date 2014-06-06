package net.meisen.dissertation.jdbc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

import net.meisen.dissertation.server.TidaServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestTidaConnection {
	private TidaServer server;

	@Before
	public void startServer() throws InterruptedException {
		server = TidaServer.create();
		server.startAsync();

		// wait for the server to start
		while (!server.isRunning()) {
			Thread.sleep(50);
		}
	}

	@Test
	public void testConnection() throws IOException {
//		Socket socket = new Socket("localhost", 7001);
		Socket socket = new Socket();
		socket.connect(new InetSocketAddress("localhost", 7001), 1000);
		socket.setSoTimeout(1000);
		
		PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				socket.getInputStream()));

		out.println("SELECT");
		System.out.println("echo: " + in.readLine());
	}

	@After
	public void shutdownServer() {
		server.shutdown();
	}
}
