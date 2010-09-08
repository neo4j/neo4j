package org.neo4j.kernel.ha.comm;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.Response;
import org.neo4j.kernel.ha.TransactionStream;

public class ChunkedResponse extends ChunkedData
{
    private static final int CHUNK_SIZE = 1 << 15;
    private final Iterator<Pair<String, TransactionStream>> stream;
    private DataWriter response;
    private String resource;
    private Iterator<Pair<Long, ReadableByteChannel>> tx;
    private ReadableByteChannel current;
    private final ByteBuffer bytes = ByteBuffer.allocate( CHUNK_SIZE );

    public ChunkedResponse( Response<DataWriter> result )
    {
        this.response = result.response();
        this.stream = result.transactions().getStreams().iterator();
    }

    @Override
    protected ChannelBuffer writeNextChunk()
    {
        if ( true ) throw new Error( "implementation not done!" );
        // FIXME: this is not done!
        if ( response == null ) return null;
        ChannelBuffer buffer = ChannelBuffers.buffer( CHUNK_SIZE );
        while ( tx == null || !tx.hasNext() )
        {
            if ( stream == null || !stream.hasNext() )
            {
                response.write( buffer );
                response = null;
                return buffer;
            }
            else
            {
                Pair<String, TransactionStream> next = stream.next();
                resource = next.first();
                tx = next.other().getChannels().iterator();
            }
        }
        Pair<Long, ReadableByteChannel> next = tx.next();
        CommunicationUtils.writeString( resource, buffer, false );
        current = next.other();
        return buffer;
    }

    @Override
    public void close()
    {
        response = null;
        tx = null;
    }
}
