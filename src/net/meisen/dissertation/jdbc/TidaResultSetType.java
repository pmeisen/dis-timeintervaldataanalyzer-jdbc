package net.meisen.dissertation.jdbc;

/**
 * The different result types of a {@code Result} of a query.
 * 
 * @author pmeisen
 * 
 */
public enum TidaResultSetType {
	/**
	 * The result has more data to over. The statement fired is a query, which
	 * asks the server for a set of data. The data, i.e. the result, is send
	 * streamed by the server record by record.
	 */
	QUERY,
	/**
	 * The statement fired was one to modify something within the database. The
	 * result just contains a single integer, which normally identifies the
	 * amount of records modified, but depending on the fired statement the
	 * interpretation may vary.
	 */
	MODIFY,
	/**
	 * The result is unknown and has to be determined by the server. The server
	 * will never use {@code this} as type, instead it will always assign a
	 * valid {@code ResultType} different from {@code UNKNOWN}. The client on
	 * the other hand can use {@code this} to mark the statement's result-type
	 * as unknown, i.e. to not formalize any expectations which might be
	 * obsolete.
	 */
	UNKNOWN;
}
