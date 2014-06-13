package net.meisen.dissertation.jdbc;

import java.sql.ClientInfoStatus;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;

public class TidaSqlExceptions {

	public static SQLClientInfoException createClientInfoException(
			final int number, final String... parameter) {
		final String msg = "[" + number + "] "
				+ createMessage(number, parameter);

		return new SQLClientInfoException(msg, null, number,
				Collections.<String, ClientInfoStatus> emptyMap());
	}

	public static SQLFeatureNotSupportedException createNotSupportedException(
			final int number, final String... parameter) {
		return createNotSupportedException(number, null, parameter);
	}

	public static SQLFeatureNotSupportedException createNotSupportedException(
			final int number, final Exception reason, final String... parameter) {
		final String msg = "[" + number + "] "
				+ createMessage(number, parameter);

		if (reason == null) {
			return new SQLFeatureNotSupportedException(msg, null, number);
		} else {
			return new SQLFeatureNotSupportedException(msg, null, number,
					reason);
		}
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
			return new SQLException(msg, null, number);
		} else {
			return new SQLException(msg, null, number, reason);
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
		case 1008:
			return "The connection does not support SQL-Arrays.";
		case 1009:
			return "The connection does not support SQL-Structs.";
		case 1010:
			return "The connection does not support CLOB.";
		case 1011:
			return "The connection does not support BLOB.";
		case 1012:
			return "The connection does not support NLOB.";
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
		case 3999:
			return "The statement is already closed.";
		case 4000:
			return "The fetch-direction cannot be changed to '" + parameter[0]
					+ "'.";
		case 4001:
			return "Changing the fetch-size is not supported, therefore the value '"
					+ parameter[0] + "' is ignored.";
		case 4002:
			return "The value '" + parameter[0]
					+ "' is an invalid value considering the fetch-size.";
		case 4003:
			return "Cannot create the handler class '" + parameter[0] + "'.";
		case 4004:
			return "A query cannot be null.";
		case 4005:
			return "An update cannot be used for queries, please use executeQuery instead.";
		case 4006:
			return "A query cannot be used for updates, please use executeUpdate instead.";
		case 4007:
			return "The methods isFirst, isLast, beforeFirst, afterLast, first, last, getRow, absolute, relative, previous, and refreshRow are not supported for result-sets of TYPE_FORWARD_ONLY.";
		case 4008:
			return "The methods rowUpdated, rowInserted, and rowDeleted are not supported for result-sets of CONCUR_READ_ONLY.";
		case 4009:
			return "Updates are not allowed on a read-only result-set.";
		case 4010:
			return "Inserts are not allowed on a read-only result-set.";
		case 4011:
			return "Deletes are not allowed on a read-only result-set.";
		case 4012:
			return "The methods cancelRowUpdates, moveToInsertRow, and moveToCurrentRow are not allowed on a read-only result-set.";
		case 4013:
			return "The type '" + parameter[0]
					+ "' of a result-set is not supported by the driver.";
		case 4014:
			return "The type '" + parameter[0]
					+ "' is invalid and not specified by the specification.";
		case 4015:
			return "The concurrency '" + parameter[0]
					+ "' of a result-set is not supported by the driver.";
		case 4016:
			return "The concurrency '" + parameter[0]
					+ "' is invalid and not specified by the specification.";
		case 4017:
			return "The holdability '" + parameter[0]
					+ "' is invalid and not specified by the specification.";
		case 4018:
			return "The result-set does not support any SQLXML.";
		case 4019:
			return "The result-set does not support RowIds.";
		case 4999:
			return "The result-set is already closed.";
		case 9000:
			return "The wrapper does not support the type '" + parameter[0]
					+ "'.";
		case 9001:
			return "Unable to establish a connection using '" + parameter[0]
					+ "'.";
		case 9002:
			return "Unable to close the connection.";
		case 9003:
			return "Unable to retrieve a protocol from a closed connection-handler.";
		case 9004:
			return "Unable to retrieve a protocol from a closed manager.";
		case 9005:
			return "The protocol of the current instance is blocked by another instance.";
		case 9006:
			return "The server-side could not handle the query '"
					+ parameter[0] + "': " + parameter[1];
		case 9007:
			return "Unable to retrieve a result, exception: " + parameter[0];
		default:
			return "Unknown exception.";
		}
	}
}