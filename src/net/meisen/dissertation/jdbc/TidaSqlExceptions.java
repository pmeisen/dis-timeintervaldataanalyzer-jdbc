package net.meisen.dissertation.jdbc;

import java.sql.SQLException;

public class TidaSqlExceptions {

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
		case 9000:
			return "The wrapper does not support the type '" + parameter[0]
					+ "'.";
		default:
			return "Unknown exception.";
		}
	}
}