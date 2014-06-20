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
		case 3000:
			return "The fetch-direction cannot be changed to '" + parameter[0]
					+ "'.";
		case 3001:
			return "Changing the fetch-size is not supported, therefore the value '"
					+ parameter[0] + "' is ignored.";
		case 3002:
			return "The value '" + parameter[0]
					+ "' is an invalid value considering the fetch-size.";
		case 3003:
			return "Poolable statements are currently not supported, neither for simple statements, nor for prepared or callable once.";
		case 3004:
			return "The value '"
					+ parameter[0]
					+ "' is an invalid value for the timeout in (milli-)seconds.";
		case 3005:
			return "The execution of '" + parameter[0]
					+ "' exceeded the defined time-out of " + parameter[1]
					+ ".";
		case 3006:
			return "The execution of statement '" + parameter[0]
					+ "' failed because of an interception: " + parameter[1]
					+ ".";
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
			return "An update '"
					+ parameter[0]
					+ "' cannot be used for queries, please use executeQuery instead.";
		case 4006:
			return "The query '"
					+ parameter[0]
					+ "' cannot be used for updates, please use executeUpdate instead.";
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
		case 4020:
			return "The retrieval of specified column-information is not supported, use instead the retrieval of autoGeneratedKeys.";
		case 4021:
			return "The driver does not support the retrieval of results using a query and the retrieval of autoGeneratedKeys.";
		case 4022:
			return "The value at columnIndex '" + parameter[0]
					+ "' cannot be casted to '" + parameter[1] + "'.";
		case 4023:
			return "The result-set does not have any column labeled '"
					+ parameter[0] + "' use one of: " + parameter[1];
		case 4024:
			return "The autoGeneratedKeys '" + parameter[0]
					+ "' is invalid and not specified by the specification.";
		case 4025:
			return "The meta-data weren't receieved correctly. A result was already found.";
		case 4026:
			return "The meta-data weren't receieved correctly. No header information was sent.";
		case 4027:
			return "The result-set of '" + parameter[0]
					+ "' was not accepted from the server or the client.";
		case 4999:
			return "The result-set is already closed.";
		case 5000:
			return "The column-index of the result-set starts with 1 and has only the one column.";
		case 5001:
			return "The result-set can only retrieve results between 0 and "
					+ parameter[1] + ", the value " + parameter[0]
					+ " is therefore invalid.";
		case 5002:
			return "The column-label of the result-set can only be '"
					+ parameter[0] + "'.";
		case 5003:
			return "The setting of a fetch-direction other than ResultSet.FETCH_FORWARD is not supported.";
		case 5004:
			return "The result-set does not support manipulation of any kind.";
		case 5005:
			return "The result-set does not support rowIds.";
		case 5998:
			return "The result-set is closed.";
		case 5999:
			return "A keys result-set does not support any other retrieval than an integer.";
		case 6000:
			return "The amount of columns could not be determined. please make sure that the query is processable on server-side.";
		case 6001:
			return "The result-set is attached to an update-statement and has therefore no columns.";
		case 6002:
			return "The column-index '" + parameter[0] + "' is invalid.";
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
		case 9008:
			return "Cannot retrieve any connection to query '" + parameter[0]
					+ "' to the server: " + parameter[1];
		default:
			return "Unknown exception.";
		}
	}
}