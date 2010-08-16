package org.neo4j.kernel.impl.ha;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;

/**
 * Represents a stream of the data of one or more consecutive transactions. 
 */
public final class TransactionStream
{
    private final Collection<ReadableByteChannel> channels;

    public TransactionStream( Collection<ReadableByteChannel> channels )
    {
        this.channels = channels;
    }

    public Collection<ReadableByteChannel> getChannels()
    {
        return channels;
    }

    public void close() throws IOException
    {
        for ( ReadableByteChannel channel : channels )
        {
            channel.close();
        }
    }
}
