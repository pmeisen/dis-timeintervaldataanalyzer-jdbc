package net.meisen.dissertation.jdbc.protocol;

import java.io.InputStream;

/**
 * Handler to handle
 * 
 * @author pmeisen
 * 
 */
public interface IResponseHandler {

	/**
	 * The client must be capable to handle a resource-request of the server.
	 * This method is triggered if such a request is send by the server for the
	 * specified {@code resource}.
	 * 
	 * @param resource
	 *            the identifier used to specify the resource to retrieve
	 * 
	 * @return a {@code InputStream} providing the specified {@code resource} or
	 *         {@code null} if the resource cannot be found
	 */
	public InputStream getResourceStream(final String resource);

	/**
	 * Handles the received {@code result} of the specified {@code type}. It is
	 * not defined for which results the {@code Protocol} must call this
	 * handling method. Generally it can be assumed that the method is at least
	 * used to handle {@link ResponseType#RESOURCE} types.
	 * 
	 * @param type
	 *            the {@code ResponseType} to be handled
	 * @param result
	 *            the value to be handled, which is of the specified
	 *            {@code type}
	 * 
	 * @return {@code true} if the reading should be continued or {@code false}
	 *         if it should be interrupted and programmatically triggered again
	 */
	public boolean handleResult(final ResponseType type, final Object[] result);

	/**
	 * This method is called to inform the {@code ResponseHandler} about the
	 * receiving of an end-of-response reached.
	 */
	public void signalEORReached();

	/**
	 * This method is called whenever the {@code ResponseHandler} should be
	 * reseted, i.e. all selected information should be reset as if the handler
	 * was just constructed.
	 */
	public void resetHandler();

	/**
	 * This method is called to identify if a specific {@code QueryType} can and
	 * should be handled by the {@code ResponseHandler}. If this method returns
	 * {@link QueryStatus#CANCEL} the underlying implementation will cancel any
	 * further processing. If it returns {@code  QueryStatus#PROCESS} or
	 * {@code  QueryStatus#PROCESSANDGETIDS} the query will be further processed.
	 * 
	 * @param queryType
	 *            the type of the query to be checked
	 * 
	 * @return the {@code QueryStatus} defining how to process with the query of
	 *         the specified {@code queryType}
	 */
	public QueryStatus doHandleQueryType(final QueryType queryType);

	/**
	 * Gets the header-types retrieved by the handler, or {@code null} if no
	 * header information was received.
	 * 
	 * @return the header-types
	 */
	public DataType[] getHeader();
}
