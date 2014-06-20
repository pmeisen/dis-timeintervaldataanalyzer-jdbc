package net.meisen.dissertation.jdbc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.meisen.dissertation.jdbc.protocol.Protocol;

public class ProtocolManager {
	private final DriverProperties driverProperties;
	private final Map<Protocol, Socket> protocols;
	private final Map<Protocol, BaseConnectionWrapper> owners;
	private final Map<BaseConnectionWrapper, Set<Protocol>> scopes;

	private boolean closed;

	public ProtocolManager(final DriverProperties driverProperties)
			throws SQLException {
		this.driverProperties = driverProperties;

		this.protocols = new HashMap<Protocol, Socket>();
		this.owners = new HashMap<Protocol, BaseConnectionWrapper>();
		this.scopes = new HashMap<BaseConnectionWrapper, Set<Protocol>>();

		this.closed = false;
	}

	public synchronized void closeOnCommit() throws SQLException {
		if (isClosed()) {
			return;
		}

		while (this.owners.size() > 0) {
			final Entry<Protocol, BaseConnectionWrapper> entry = this.owners
					.entrySet().iterator().next();
			final BaseConnectionWrapper owner = entry.getValue();
			if (owner.doCloseOnCommit()) {
				owner.close();
			}
		}
	}

	public synchronized void close() throws SQLException {
		if (isClosed()) {
			return;
		}

		// close all the protocols
		while (protocols.size() > 0) {
			releaseProtocol(protocols.keySet().iterator().next());
		}

		// remove all the scopes, everything is closed from it
		this.scopes.clear();
		this.closed = true;
	}

	public boolean isClosed() {
		return closed;
	}

	public DriverProperties getDriverProperties() {
		return driverProperties;
	}

	protected boolean isClosed(final Protocol protocol) {
		final Socket socket = this.protocols.remove(protocol);

		if (socket.isClosed() || socket.isInputShutdown()
				|| socket.isOutputShutdown()) {

			// mark the socket as closed
			try {
				close();
			} catch (final SQLException e) {
				// ignore
			}
			return true;
		} else {
			return false;
		}
	}

	public synchronized void release(final BaseConnectionWrapper owner)
			throws SQLException {
		if (owner == null) {
			return;
		}

		// cleanup the protocols owned
		Protocol ownedProtocol = null;
		for (final Entry<Protocol, BaseConnectionWrapper> entry : owners
				.entrySet()) {
			if (entry.getValue().equals(owner)) {
				ownedProtocol = entry.getKey();
				break;
			}
		}
		releaseProtocol(ownedProtocol);

		// cleanup the managed protocol
		final Set<Protocol> protocols = this.scopes.remove(owner);
		if (protocols != null) {
			for (final Protocol protocol : protocols) {
				releaseProtocol(protocol);
			}
		}
	}

	protected void releaseProtocol(final Protocol protocol) throws SQLException {
		if (protocol == null) {
			return;
		}

		boolean exception = false;

		// close the protocol first
		try {
			protocol.close();
		} catch (final IOException e) {
			exception = true;
		}

		// if there was a socket bound close it as well
		final Socket socket = this.protocols.remove(protocol);
		if (socket != null) {
			try {
				socket.close();
			} catch (final IOException e) {
				exception = true;
			}
		}

		// get the scope the protocol belongs to and remove it from there
		for (final Entry<BaseConnectionWrapper, Set<Protocol>> entry : this.scopes
				.entrySet()) {
			final Set<Protocol> protocols = entry.getValue();

			// remove the protocol from the set
			if (protocols.remove(protocol)) {

				// if it exists and the list is empty remove it
				if (protocols.size() == 0) {
					this.scopes.remove(protocol);
				}
				break;
			}
		}

		// remove the protocol from the owners
		this.owners.remove(protocol);

		if (exception) {
			throw TidaSqlExceptions.createException(2004);
		}
	}

	public synchronized Protocol getProtocol(final BaseConnectionWrapper owner,
			final BaseConnectionWrapper scope) throws SQLException {
		if (isClosed()) {
			throw TidaSqlExceptions.createException(9004);
		}

		final Socket socket = new Socket();
		try {
			socket.connect(new InetSocketAddress("localhost", 7001),
					driverProperties.getTimeout());
			socket.setSoTimeout(getDriverProperties().getTimeout());
		} catch (final IOException e) {
			throw TidaSqlExceptions.createException(9002, e,
					getDriverProperties().getRawJdbc());
		}

		// add the protocol
		final Protocol protocol;
		try {
			protocol = new Protocol(socket);
		} catch (final IOException e) {
			throw TidaSqlExceptions.createException(9003, e,
					getDriverProperties().getRawJdbc());
		}

		owners.put(protocol, owner);
		protocols.put(protocol, socket);

		// add the scope
		Set<Protocol> protocols = scopes.get(scope);
		if (protocols == null) {
			protocols = new HashSet<Protocol>();
			scopes.put(scope, protocols);
		}
		protocols.add(protocol);

		return protocol;
	}

	/**
	 * Gets the amount of scopes.
	 * 
	 * @return the amount of scopes
	 */
	public synchronized int sizeOfScopes() {
		return scopes.size();
	}

	/**
	 * Gets the amount of protocols associated to the specified {@code scope}.
	 * 
	 * @param scope
	 *            the scope to get the amount of associated protocols of
	 * 
	 * @return the amount of associated protocols to the {@code scope}
	 */
	public synchronized int sizeOfProtocols(final BaseConnectionWrapper scope) {
		final Set<Protocol> protocols = scopes.get(scope);
		if (protocols == null) {
			return 0;
		} else {
			return protocols.size();
		}
	}

	public synchronized boolean isOwner(final BaseConnectionWrapper owner) {
		return owners.containsValue(owner);
	}

	public synchronized int sizeOfOwners() {
		return owners.size();
	}
}
