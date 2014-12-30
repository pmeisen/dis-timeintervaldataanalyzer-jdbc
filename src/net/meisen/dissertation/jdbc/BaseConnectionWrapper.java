package net.meisen.dissertation.jdbc;

import java.io.IOException;
import java.net.SocketException;
import java.sql.SQLException;

import net.meisen.dissertation.jdbc.protocol.IResponseHandler;
import net.meisen.dissertation.jdbc.protocol.Protocol;
import net.meisen.dissertation.jdbc.protocol.WrappedException;

/**
 * A base implementation used for instance which consume (or use) a connection.
 * The base implementation extends the {@code BaseWrapper}. Generally a
 * connection of a parent {@code BaseConnection} is used prior to creating a new
 * one.
 * 
 * @author pmeisen
 * 
 */
public abstract class BaseConnectionWrapper extends BaseWrapper {

	private final ProtocolManager manager;
	private final BaseConnectionWrapper parent;

	private BaseConnectionWrapper blockedBy;
	private boolean closed;
	private Protocol protocol;

	/**
	 * Constructor to create an instance of a using {@code Connection} instance
	 * for the specified {@code DriverProperties}.
	 * 
	 * @param driverProperties
	 *            the properties to base the connections on
	 */
	public BaseConnectionWrapper(final DriverProperties driverProperties) {
		this(new ProtocolManager(driverProperties), null);
	}

	/**
	 * Creates an instance based on the specified {@code parent}. The parent's
	 * connection is used prior to creating an own connection.
	 * 
	 * @param parent
	 *            the parent of {@code this}
	 */
	public BaseConnectionWrapper(final BaseConnectionWrapper parent) {
		this(parent.manager, parent);
	}

	/**
	 * Wrapper constructor to combine both public constructors. This constructor
	 * should only be used internally.
	 * 
	 * @param manager
	 *            the {@code ProtocolManager} of {@code this}
	 * @param parent
	 *            the parent of {@code this}, can be {@code null}
	 */
	protected BaseConnectionWrapper(final ProtocolManager manager,
			final BaseConnectionWrapper parent) {
		this.manager = manager;
		this.parent = parent;

		this.closed = false;
		this.protocol = null;
		this.blockedBy = null;
	}

	/**
	 * Closes {@code this} instance.
	 * 
	 * @throws SQLException
	 *             if the closing fails
	 */
	public void close() throws SQLException {
		if (isClosed()) {
			return;
		}

		release();
		this.closed = true;
	}

	/**
	 * Closes {@code this} and all the {@code Protocols} managed by any instance
	 * of the {@code ProtocolManager} used by the stack of
	 * {@code BaseConnection} users.
	 * 
	 * @throws SQLException
	 *             if the closing of one fails
	 */
	public void closeAll() throws SQLException {
		this.manager.close();
		this.closed = true;
	}

	/**
	 * Checks if the {@code Connection} is closed.
	 * 
	 * @return {@code true} if it's closed, otherwise {@code false}
	 */
	public boolean isClosed() {
		return closed;
	}

	/**
	 * Releases {@code this} instance by releasing {@code this} at the
	 * underlying {@code ProtocolManager} (see
	 * {@link ProtocolManager#release(BaseConnectionWrapper)}) and releasing any
	 * bound protocol.
	 * 
	 * @throws SQLException
	 *             if the release fails
	 */
	protected void release() throws SQLException {

		// release the created protocol
		this.manager.release(this);

		// remove the protocol and release it to be used by others
		this.protocol = null;

		if (parent.isUsedBy(this)) {
			parent.setUser(null);
		}
	}

	/**
	 * Gets the protocol to be used for communication by {@code this}.
	 * 
	 * @return the {@code Protocol} to be used
	 * 
	 * @throws SQLException
	 *             if no protocol can be created or non is available
	 */
	protected Protocol getProtocol() throws SQLException {
		if (isClosed()) {
			throw TidaSqlExceptions.createException(9003, getDriverProperties()
					.getRawJdbc());
		}

		if (this.protocol != null) {
			// do nothing we have a free protocol
		} else if (parent != null && parent.isProtocolAvailable()) {
			parent.setUser(this);
			this.protocol = parent.getProtocol();
		} else if (this.protocol == null) {
			this.protocol = manager.createProtocol(this, getProtocolScope());
		} else if (!isProtocolAvailable()) {
			throw TidaSqlExceptions.createException(9005, getDriverProperties()
					.getRawJdbc());
		}

		return this.protocol;
	}

	/**
	 * Fires the specified {@code sql} query using the specified {@code handler}
	 * to handle the query with.
	 * 
	 * @param sql
	 *            the query to be fired
	 * @param handler
	 *            the handler to handle the query with
	 * 
	 * @return {@code true} if the query was fired and the handler can be used
	 *         to handle further processing, otherwise {@code false}
	 * 
	 * @throws SQLException
	 *             if the query cannot be fired
	 */
	protected boolean fireQuery(final String sql, final IResponseHandler handler)
			throws SQLException {
		try {
			return getProtocol().initializeCommunication(sql, handler);
		} catch (final SocketException e) {

			// close this one and re-query
			close();
			return refireQuery(sql, handler);
		} catch (final IOException e) {
			throw TidaSqlExceptions.createException(9008, sql, e.getMessage());
		} catch (final WrappedException e) {
			throw TidaSqlExceptions.createException(9006, sql, e.getMessage());
		}
	}

	/**
	 * Internally used method to re-fire a query based on a newly created
	 * connection. It might occur that the connection used was time-outed and
	 * therefore one retry should be fired at least.
	 * 
	 * @param sql
	 *            the query to be re-fired
	 * @param handler
	 *            the handler to handle the query with
	 * 
	 * @return {@code true} if the query was fired and the handler can be used
	 *         to handle further processing, otherwise {@code false}
	 * 
	 * @throws SQLException
	 *             if the query cannot be fired
	 */
	private boolean refireQuery(final String sql, final IResponseHandler handler)
			throws SQLException {
		try {
			return getProtocol().initializeCommunication(sql, handler);
		} catch (final IOException e) {
			throw TidaSqlExceptions.createException(9008, sql, e.getMessage());
		}
	}

	/**
	 * Method to further process an already initialized {@code handler}. The
	 * handler should have been initialized using the
	 * {@link #fireQuery(String, IResponseHandler)} method. After calling the
	 * method, the {@code handler} contains the next read record or value after
	 * calling {@code this}. The method should always be called until the
	 * {@link IResponseHandler} handled the complete request, i.e. until the
	 * protocol is ready for the next request. The reaching of the state depends
	 * on the concrete implementation of the {@code IResponseHandler}.
	 * 
	 * @param handler
	 *            the handler used to handle the next response
	 * 
	 * @throws SQLException
	 *             if the response cannot be handled
	 */
	protected void handleResponse(final IResponseHandler handler)
			throws SQLException {
		try {
			getProtocol().handleResponse(handler);
		} catch (final IOException e) {
			throw TidaSqlExceptions.createException(9007, e.getMessage());
		} catch (final WrappedException e) {
			throw TidaSqlExceptions.createException(9007, e.getMessage());
		}
	}

	/**
	 * Gets the amount of currently managed protocols within the {@code scope}
	 * of {@code this}.
	 * 
	 * @return the amount of currently managed protocols within the
	 *         {@code scope} of {@code this}
	 */
	public int sizeOfProtocols() {
		return this.manager.sizeOfProtocols(getProtocolScope());
	}

	/**
	 * Checks if {@code this} connection is currently blocked and used by
	 * {@code user}.
	 * 
	 * @param user
	 *            the user to be checked
	 * 
	 * @return {@code true} if the connection of {@code this} is currently
	 *         blocked by the specified {@code user}
	 */
	public boolean isUsedBy(final BaseConnectionWrapper user) {
		return this.blockedBy == user;
	}

	/**
	 * Checks if the {@code Protocol} of {@code this}, i.e. the connection, is
	 * currently available.
	 * 
	 * @return {@code true} if it is available, otherwise {@code false}
	 */
	public boolean isProtocolAvailable() {
		return this.blockedBy == null;
	}

	/**
	 * Gets the properties used to connect.
	 * 
	 * @return the properties used to connect
	 */
	public DriverProperties getDriverProperties() {
		return manager.getDriverProperties();
	}

	/**
	 * Gets the scope of {@code this}.
	 * 
	 * @return the scope of {@code this}
	 */
	protected abstract BaseConnectionWrapper getProtocolScope();

	/**
	 * Checks if all connections of the scope of {@code this} should be closed
	 * on a commit.
	 * 
	 * @return {@code true} if all connection should be closed, otherwise
	 *         {@code false}
	 */
	protected abstract boolean doCloseOnCommit();

	/**
	 * Sets the user of {@code this}. The user indicates that the
	 * {@code Connection} of {@code this} is occupied by the user currently. The
	 * {@link #getProtocol()} implementation uses the information to decide
	 * which protocol to be used.
	 * 
	 * @param user
	 *            the user blocking the connection
	 */
	protected void setUser(final BaseConnectionWrapper user) {
		this.blockedBy = user;
	}

	/**
	 * Gets the parent {@code BaseConnection}, which might be {@code null} if no
	 * parent exists.
	 * 
	 * @return the parent {@code BaseConnection}, which might be {@code null} if
	 *         no parent exists
	 */
	protected BaseConnectionWrapper getParent() {
		return parent;
	}

	/**
	 * Gets the {@code ProtocolManager} used by {@code this}.
	 * 
	 * @return the {@code ProtocolManager} used by {@code this}
	 */
	protected ProtocolManager getManager() {
		return manager;
	}
}
