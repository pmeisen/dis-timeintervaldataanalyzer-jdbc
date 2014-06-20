package net.meisen.dissertation.jdbc;

import java.io.IOException;
import java.net.SocketException;
import java.sql.SQLException;

import net.meisen.dissertation.jdbc.protocol.IResponseHandler;
import net.meisen.dissertation.jdbc.protocol.Protocol;
import net.meisen.dissertation.jdbc.protocol.WrappedException;

public abstract class BaseConnectionWrapper extends BaseWrapper {

	private final ProtocolManager manager;
	private final BaseConnectionWrapper parent;

	private BaseConnectionWrapper blockedBy;
	private boolean closed;
	private Protocol protocol;

	public BaseConnectionWrapper(final DriverProperties driverProperties)
			throws SQLException {
		this(new ProtocolManager(driverProperties), null);
	}

	public BaseConnectionWrapper(final BaseConnectionWrapper parent) {
		this(parent.manager, parent);
	}

	protected BaseConnectionWrapper(final ProtocolManager manager,
			final BaseConnectionWrapper parent) {
		this.manager = manager;
		this.parent = parent;

		this.closed = false;
		this.protocol = null;
		this.blockedBy = null;
	}

	public void close() throws SQLException {
		if (isClosed()) {
			return;
		}

		release();
		this.closed = true;
	}

	public void closeAll() throws SQLException {
		this.manager.close();
		this.closed = true;
	}

	public boolean isClosed() {
		return closed;
	}

	protected void release() throws SQLException {

		// release the created protocol
		this.manager.release(this);

		// remove the protocol and release it to be used by others
		this.protocol = null;

		if (parent.isUsedBy(this)) {
			parent.setUser(null);
		}
	}

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
			this.protocol = manager.getProtocol(this, getProtocolScope());
		} else if (!isProtocolAvailable()) {
			throw TidaSqlExceptions.createException(9005, getDriverProperties()
					.getRawJdbc());
		}

		return this.protocol;
	}

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

	private boolean refireQuery(final String sql, final IResponseHandler handler)
			throws SQLException {
		try {
			return getProtocol().initializeCommunication(sql, handler);
		} catch (final IOException e) {
			throw TidaSqlExceptions.createException(9008, sql, e.getMessage());
		}
	}

	protected void handleResponse(final IResponseHandler handler)
			throws SQLException {
		try {
			getProtocol().handleResponse(handler);
		} catch (final IOException e) {
			throw TidaSqlExceptions.createException(9008, e.getMessage());
		} catch (final WrappedException e) {
			throw TidaSqlExceptions.createException(9007, e.getMessage());
		}
	}

	public int sizeOfProtocols() {
		return this.manager.sizeOfProtocols(getProtocolScope());
	}

	public boolean isUsedBy(final BaseConnectionWrapper user) {
		return this.blockedBy == user;
	}

	public boolean isProtocolAvailable() {
		return this.blockedBy == null;
	}

	public DriverProperties getDriverProperties() {
		return manager.getDriverProperties();
	}

	protected abstract BaseConnectionWrapper getProtocolScope();

	protected abstract boolean doCloseOnCommit();

	protected void setUser(final BaseConnectionWrapper user) {
		this.blockedBy = user;
	}

	protected BaseConnectionWrapper getParent() {
		return parent;
	}

	protected ProtocolManager getManager() {
		return manager;
	}
}
