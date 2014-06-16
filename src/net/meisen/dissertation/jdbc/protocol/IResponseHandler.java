package net.meisen.dissertation.jdbc.protocol;

import java.io.InputStream;

public interface IResponseHandler {
	public InputStream getResourceStream(final String resource);

	public void setHeader(final Class<?>[] header);

	public void setHeaderNames(final String[] header);

	public boolean handleResult(final RetrievedValue value);

	public void signalEORReached();
}
