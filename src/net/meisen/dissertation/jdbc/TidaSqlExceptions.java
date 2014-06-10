package net.meisen.dissertation.jdbc;

import java.sql.ClientInfoStatus;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Collections;

public class TidaSqlExceptions {

	public static SQLClientInfoException createClientInfoException(
			final int number, final String... parameter) {
		final String msg = "[" + number + "] "
				+ createMessage(number, parameter);

		return new SQLClientInfoException(msg,
				Collections.<String, ClientInfoStatus> emptyMap());
	}

	public static SQLException createException(final int number) {
		return createException(number, null, (String[]) null);
	}

	public static SQLException createException(final int number,
			final String... parameter) {
		return createException(number, null, parameter);
	}

	public static SQLException createException(final int number,
			final Exception reason, final String... parameter) {
		final String msg = "[" + number + "] "
				+ createMessage(number, parameter);

		if (reason == null) {
			return new SQLException(msg);
		} else {
			return new SQLException(msg, reason);
		}
	}

	protected static String createMessage(final int number,
			final String... parameter) {

		switch (number) {
		case 1000:
			return "Transaction isolation levels are not supported.";
		case 1001:
			return "Transactions are not supported by the implementation.";
		case 1002:
			return "The connection does not support callable statements.";
		case 1003:
			return "The timeout ('" + parameter[0]
					+ "') value must be larger than or equal to 0.";
		case 1004:
			return "The connection does not support the setting of types.";
		case 1005:
			return "Setting of client properties is not supported by the connection (key: "
					+ parameter[0] + ", value: " + parameter[1] + ").";
		case 1006:
			return "Setting of client properties is not supported by the connection (properties: "
					+ parameter[0] + ").";
		case 1007:
			return "The connection does not support any SQLXML.";
		case 1999:
			return "The connection is already closed.";
		case 2000:
			return "The url does not specify any port, please use: "
					+ Constants.URL_FULL_SYNTAX;
		case 2001:
			return "The url must define a valid host, please use: "
					+ Constants.URL_FULL_SYNTAX;
		case 2002:
			return "The specified port '" + parameter[0]
					+ "' is not a valid number, please use: "
					+ Constants.URL_FULL_SYNTAX;
		case 2003:
			return "Unable to establish a connection using '" + parameter[0]
					+ "'";
		case 2004:
			return "Unable to close the connection";
		case 9000:
			return "The wrapper does not support the type '" + parameter[0]
					+ "'.";
		default:
			return "Unknown exception.";
		}
	}
}