package org.neo4j.kernel.impl.transaction.xaframework;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Thrown when reading from a {@link ReadableByteChannel} into a buffer
 * and not enough bytes ({@link ByteBuffer#limit()}) could be read.
 */
public class ReadPastEndException extends Exception
{
    public ReadPastEndException()
    {
        super();
    }
}
