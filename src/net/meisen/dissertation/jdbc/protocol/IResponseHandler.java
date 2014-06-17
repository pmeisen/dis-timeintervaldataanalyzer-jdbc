package net.meisen.dissertation.jdbc.protocol;

import java.io.InputStream;

public interface IResponseHandler {
	public InputStream getResourceStream(final String resource);

	public void setHeader(final DataType[] header);

	public void setHeaderNames(final String[] header);

	public boolean handleResult(final ResponseType type, final Object[] result);

	public void signalEORReached();
	
	public void resetHandler();

	public QueryStatus doHandleQueryType(QueryType queryType);

	public DataType[] getHeader();
}
