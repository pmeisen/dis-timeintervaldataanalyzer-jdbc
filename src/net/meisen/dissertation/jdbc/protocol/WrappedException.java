package net.meisen.dissertation.jdbc.protocol;

/**
 * An exception send from the server, which is wrapped over the communication
 * {@code Protocol}.
 * 
 * @author pmeisen
 * 
 */
public class WrappedException extends RuntimeException {
	private static final long serialVersionUID = -813220420443909821L;

	/**
	 * Constructor which specifies the exception to be wrapped.
	 * 
	 * @param msg
	 *            the exception wrapped by {@code this}
	 */
	public WrappedException(final String msg) {
		super(msg);
	}
}
