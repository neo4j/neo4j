package org.neo4j.kernel.impl.ha;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;

import org.neo4j.helpers.Pair;

/**
 * Represents a stream of the data of one or more consecutive transactions. 
 */
public final class TransactionStream
{
    private final Collection<Pair<Long, ReadableByteChannel>> channels;

    public TransactionStream( Collection<Pair<Long, ReadableByteChannel>> channels )
    {
        this.channels = channels;
    }

    public Collection<Pair<Long, ReadableByteChannel>> getChannels()
    {
        return channels;
    }

    public void close() throws IOException
    {
        for ( Pair<Long, ReadableByteChannel> channel : channels )
        {
            channel.other().close();
        }
    }
}
