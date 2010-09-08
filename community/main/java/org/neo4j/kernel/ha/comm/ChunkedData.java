package org.neo4j.kernel.ha.comm;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.stream.ChunkedInput;

public abstract class ChunkedData implements ChunkedInput
{
    private ChannelBuffer chunk;

    public boolean hasNextChunk()
    {
        if ( chunk != null ) return true;
        chunk = writeNextChunk();
        if ( chunk == null || !chunk.readable() )
        { // No more data!
            chunk = null;
            close(); // we are done!
            return false;
        }
        else
        {
            return true;
        }
    }

    public final ChannelBuffer nextChunk()
    {
        if ( hasNextChunk() )
        {
            try
            {
                return chunk;
            }
            finally
            {
                chunk = null;
            }
        }
        else
        {
            return null;
        }
    }

    public final boolean isEndOfInput()
    {
        return !hasNextChunk();
    }

    protected abstract ChannelBuffer writeNextChunk();

    public abstract void close();
}
