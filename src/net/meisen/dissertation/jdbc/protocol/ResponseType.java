package net.meisen.dissertation.jdbc.protocol;

/**
 * The type of the response send by the server (typically) or the client.
 * 
 * @author pmeisen
 * 
 */
public enum ResponseType {

	/**
	 * An exception was thrown while retrieving the value on the other side.
	 */
	EXCEPTION((byte) 1),
	/**
	 * The end of response is reached.
	 */
	EOR((byte) 2, false, false),
	/**
	 * A message, i.e. a string, was produced.
	 */
	MESSAGE((byte) 3),
	/**
	 * To further process, a resource has to be send which is specified by a
	 * string.
	 */
	RESOURCE_DEMAND((byte) 4),
	/**
	 * A resource, as byte-stream, is send as answer to a
	 * {@code RESOURCE_DEMAND}.
	 */
	RESOURCE((byte) 5),
	/**
	 * A header is send, i.e. an array of {@code Class} instances represented by
	 * an byte-identifier (see {@link DataType}).
	 */
	HEADER((byte) 6),
	/**
	 * An array of strings is send specifying names for each header.
	 */
	HEADERNAMES((byte) 7, true, true),
	/**
	 * A single integer is send.
	 */
	INT((byte) 8, false, Integer.SIZE / 8),
	/**
	 * An array of integers is send.
	 */
	INT_ARRAY((byte) 9, true, Integer.SIZE / 8),
	/**
	 * A marker specifying that a result is coming next, the result has
	 * additional data, which is not handled by the {@code ResponseType}
	 * directly, therefore it returns {@code false} for {@link #hasData()}.
	 */
	RESULT((byte) 10, false, false),
	/**
	 * A {@code ResponseType} which identifies a cancellation.
	 */
	CANCEL((byte) 11, false, false),
	/**
	 * A {@code ResponseType} which marks the end of meta-data.
	 */
	EOM((byte) 12, false, false);

	private final byte id;
	private final boolean hasData;
	private final boolean chunked;
	private final int fixedSize;

	/**
	 * Constructor to create a {@code ResponseType} representing a
	 * {@code ResponseType} which is defined by bytes.
	 * 
	 * @param id
	 *            the identifier of the {@code ResponseType} to be created
	 */
	private ResponseType(final byte id) {
		this(id, true, false, -1);
	}

	/**
	 * Constructor to create a {@code ResponseType} which having the specified
	 * fixed-size and might be chunked or not (i.e. might be a single value or
	 * an array).
	 * 
	 * @param id
	 *            the identifier of the {@code ResponseType} to be created
	 * @param chunked
	 *            {@code true} if an array of the fixed-sized element is
	 *            expected, otherwise {@code false}
	 * @param fixedSize
	 *            the fixed size of the element
	 */
	private ResponseType(final byte id, final boolean chunked,
			final int fixedSize) {
		this(id, true, chunked, fixedSize);
	}

	/**
	 * COnstructor to create a {@code ResponseType} which might or might not
	 * have data and might or might not be chunked. The created
	 * {@code ResponseType} is not fixed in size.
	 * 
	 * @param id
	 *            the identifier of the {@code ResponseType} to be created
	 * @param hasData
	 *            {@code true} if the {@code ResponseType} has data, otherwise
	 *            {@code false}; later means it just sends an identification
	 *            byte
	 * @param chunked
	 *            {@code true} if an array of the fixed-sized element is
	 *            expected, otherwise {@code false}
	 */
	private ResponseType(final byte id, final boolean hasData,
			final boolean chunked) {
		this(id, hasData, chunked, -1);
	}

	/**
	 * Constructor to specify each an every single value needed by a
	 * {@code ResponseType}.
	 * 
	 * @param id
	 *            the identifier used for the type of the {@code ResponseType}
	 * @param hasData
	 *            {@code true} if the {@code ResponseType} has additional data
	 *            send with it, otherwise {@code false}
	 * @param chunked
	 *            {@code true} if the data is chunked into several byte-arrays,
	 *            i.e. cannot be read just with one byte-array and instead needs
	 *            several retrievals, otherwise {@code false}
	 * @param fixed
	 *            if the size of bytes to be read is pre-defined (by the
	 *            {@code fixedSize})
	 * @param fixedSize
	 */
	private ResponseType(final byte id, final boolean hasData,
			final boolean chunked, final int fixedSize) {
		this.id = id;
		this.hasData = hasData;
		this.chunked = chunked;
		this.fixedSize = fixedSize;
	}

	/**
	 * Gets the byte used to identify the {@code ResponseType}.
	 * 
	 * @return the byte used to identify the {@code ResponseType}
	 */
	public byte getId() {
		return id;
	}

	/**
	 * Resolve the {@code ResponseType} according to the specified {@code id}.
	 * The method returns {@code null} if no {@code ResponseType} for the
	 * specified {@code id} can be found.
	 * 
	 * @param id
	 *            the identifier of the {@code ResponseType} to get
	 * 
	 * @return the {@code ResponseType} for the specified {@code id} or
	 *         {@code null} if unknown
	 */
	public static ResponseType find(final byte id) {
		for (final ResponseType type : ResponseType.values()) {
			if (id == type.getId()) {
				return type;
			}
		}

		return null;
	}

	/**
	 * Checks if the response has further data than just the marker of the type.
	 * 
	 * @return {@code true} if further data has to be read, otherwise
	 *         {@code false}; later is typical for markers
	 */
	public boolean hasData() {
		return hasData;
	}

	/**
	 * Checks if the response is chunked in pieces, i.e. several reads have to
	 * be performed to read the complete response (e.g. when arrays are send).
	 * Chunked data normally sends an amount of chunks, together with a size of
	 * each chunk (e.g. {@code 2 4 xxxx 3 xxx}, means 2 chunks are send, first
	 * chunk is of size 4 with the four data bytes, second byte is of size 3
	 * with the three data bytes).
	 * 
	 * @return {@code true} if the data is chunked, otherwise {@code false}
	 */
	public boolean isChunked() {
		return chunked;
	}

	/**
	 * Checks if the size of the response is fixed and therefore not wrapped
	 * within the response, because it is commonly known.
	 * 
	 * @return {@code true} if the size is fixed, otherwise {@code false}
	 */
	public boolean isFixed() {
		return fixedSize > 0;
	}

	/**
	 * Gets the fixed size of the response if any, otherwise {@code -1} is
	 * returned.
	 * 
	 * @return the fixed size of the response if any, otherwise {@code -1} is
	 *         returned
	 */
	public int getFixedSize() {
		return fixedSize;
	}
}
