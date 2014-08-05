package net.meisen.dissertation.jdbc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import net.meisen.dissertation.jdbc.protocol.Protocol;

/**
 * Manager to handle the different {@code Protocol} instances for different
 * owners. The manager assigns each {@code Protocol} created to one specific
 * {@code owner}. The relationship is thereby 1:1, i.e. a {@code Protocol} can
 * only be owned by one owner, and an owner can only own one {@code Protocol}.
 * Additionally, each {@code Protocol} instance is using a specific
 * {@code Socket} to communicate with the server. A {@code Protocol} belongs
 * also to a specific {@code scope}, whereby a {@code scope} can contain several
 * protocols.<br/>
 * The idea behind this mechanism is simple. A {@code scope} groups
 * {@code Protocol} instances. Whenever a scope dies (is closed) all the
 * contained {@code Protocol} instances are closed as well. Additionally, an
 * {@code owner} can be closed, if so the scope of the owning instance is closed
 * as well as the owner itself.
 * 
 * @author pmeisen
 * 
 */
public class ProtocolManager {
	private final DriverProperties driverProperties;
	private final Map<Protocol, Socket> protocols;
	private final Map<Protocol, BaseConnectionWrapper> owners;
	private final Map<BaseConnectionWrapper, Set<Protocol>> scopes;

	private boolean closed;

	/**
	 * Initializes the {@code ProtocolManager} with the specified
	 * {@code DriverProperties}.
	 * 
	 * @param driverProperties
	 *            the properties defining the connection to be established by
	 *            the manager
	 */
	public ProtocolManager(final DriverProperties driverProperties) {
		this.driverProperties = driverProperties;

		this.protocols = new HashMap<Protocol, Socket>();
		this.owners = new HashMap<Protocol, BaseConnectionWrapper>();
		this.scopes = new HashMap<BaseConnectionWrapper, Set<Protocol>>();

		this.closed = false;
	}

	/**
	 * This method closes all the {@code owners} which should be closed on
	 * commit (i.e. {@link BaseConnectionWrapper#doCloseOnCommit()} return
	 * {@code true}).
	 * 
	 * @throws SQLException
	 *             if an error occurs while closing the owner
	 */
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

	/**
	 * Closes the manager and all the protocols managed by the instance.
	 * 
	 * @throws SQLException
	 *             if the closing fails
	 */
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

	/**
	 * Checks if the manager is closed.
	 * 
	 * @return {@code true} if the manager is closed, otherwise {@code false}
	 */
	public boolean isClosed() {
		return closed;
	}

	/**
	 * Gets the {@code DriverProperties} instance used by {@code this}.
	 * 
	 * @return the {@code DriverProperties} instance used by {@code this}
	 */
	public DriverProperties getDriverProperties() {
		return driverProperties;
	}

	/**
	 * Release the specified {@code owner}. Releasing an owner implies to
	 * release the {@code Protocol} instance owned (if one is owned), as well as
	 * all the {@code Protocol} instances which are in the scope of the
	 * {@code owner}.
	 * 
	 * @param owner
	 * @throws SQLException
	 */
	public synchronized void release(final BaseConnectionWrapper owner)
			throws SQLException {
		if (owner == null) {
			return;
		}

		// cleanup the protocol owned
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

	/**
	 * Releases the {@code protocol} by closing it. Additionally the
	 * {@code protocol's} socket is closed and it is removed from the scope it
	 * belongs to. Additionally the scope is removed if it is empty.
	 * 
	 * @param protocol
	 *            the {@code Protocol} instance to be released
	 * 
	 * @throws SQLException
	 *             if releasing the {@code protocol} led to an error
	 */
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
		exception = closeSocket(socket);

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

	/**
	 * Method used to close the specified {@code socket}.
	 * 
	 * @param socket
	 *            the socket to be closed
	 * 
	 * @return {@code true} if the socket was closed without any exception,
	 *         otherwise {@code false}
	 */
	protected boolean closeSocket(final Socket socket) {
		if (socket == null) {
			return true;
		}

		try {
			socket.close();

			return false;
		} catch (final IOException e) {
			return true;
		}
	}

	/**
	 * Creates a new {@code Protocol} for the specified {@code owner} within the
	 * specified {@code scope}.
	 * 
	 * @param owner
	 *            the {@code BaseConnectionWrapper} instance owning the created
	 *            {@code Protocol}
	 * @param scope
	 *            the {@code BaseConnectionWrapper} instance which defines the
	 *            scope
	 * 
	 * @return a created {@code Protocol}
	 * 
	 * @throws SQLException
	 *             if no {@code Protocol} instance could be created
	 */
	public synchronized Protocol createProtocol(
			final BaseConnectionWrapper owner, final BaseConnectionWrapper scope)
			throws SQLException {
		if (isClosed()) {
			throw TidaSqlExceptions.createException(9004);
		}

		final Socket socket = new Socket();
		try {
			socket.connect(
					new InetSocketAddress("localhost", driverProperties
							.getPort()), driverProperties.getTimeout());
		} catch (final IOException e) {
			throw TidaSqlExceptions.createException(9001, e,
					getDriverProperties().getRawJdbc());
		}

		// set the timeout and the linger of the socket
		try {
			socket.setSoTimeout(driverProperties.getTimeout());

			if (driverProperties.disableLinger()) {
				socket.setSoLinger(true, 0);
			} else if (driverProperties.getLingerInSeconds() > 0) {
				socket.setSoLinger(true, driverProperties.getLingerInSeconds());
			}
		} catch (final IOException e) {
			closeSocket(socket);
			throw TidaSqlExceptions.createException(9001, e,
					getDriverProperties().getRawJdbc());
		}

		// add the protocol
		final Protocol protocol;
		try {
			protocol = new Protocol(socket);
		} catch (final IOException e) {
			closeSocket(socket);
			throw TidaSqlExceptions.createException(9003, e,
					getDriverProperties().getRawJdbc());
		}

		// send the credentials to authenticate on the new socket
		try {
			protocol.writeCredential(driverProperties.getUser(),
					driverProperties.getPassword());
		} catch (final IOException e) {
			try {
				protocol.close();
			} catch (final IOException ex) {
				// ignore
			}
			closeSocket(socket);
			throw TidaSqlExceptions.createException(9009, e);
		}

		this.owners.put(protocol, owner);
		this.protocols.put(protocol, socket);

		// add the scope
		Set<Protocol> protocols = scopes.get(scope);
		if (protocols == null) {
			protocols = new HashSet<Protocol>();
			this.scopes.put(scope, protocols);
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

	/**
	 * Checks if the specified {@code owner} is really an owner of any
	 * {@code Protocol}.
	 * 
	 * @param owner
	 *            the instance to be checked
	 * 
	 * @return {@code true} if the {@code owner} owns a {@code Protocol},
	 *         otherwise {@code false}
	 */
	public synchronized boolean isOwner(final BaseConnectionWrapper owner) {
		return owners.containsValue(owner);
	}

	/**
	 * Gets the amount of owners, managed by {@code this}.
	 * 
	 * @return the amount of owners, managed by {@code this}
	 */
	public synchronized int sizeOfOwners() {
		return owners.size();
	}
}
