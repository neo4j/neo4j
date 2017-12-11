package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;

import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.cursor.RawCursor;

public class StoreStreamingProtocol
{
    /**
     * This queues all the file sending operations on the outgoing pipeline, including
     * chunking {@link org.neo4j.causalclustering.catchup.storecopy.FileSender} handlers.
     * <p>
     * Note that we do not block here.
     */
    void stream( ChannelHandlerContext ctx, RawCursor<StoreResource,IOException> resources ) throws IOException
    {
        while ( resources.next() )
        {
            StoreResource resource = resources.get();

            ctx.writeAndFlush( ResponseMessageType.FILE );
            ctx.writeAndFlush( new FileHeader( resource.path(), resource.recordSize() ) );
            ctx.writeAndFlush( new FileSender( resource ) );
        }
    }

    ChannelFuture end( StoreCopyFinishedResponse.Status status, ChannelHandlerContext ctx, long lastCommittedTxBeforeStoreCopy )
    {
        ctx.write( ResponseMessageType.STORE_COPY_FINISHED );
        return ctx.writeAndFlush( new StoreCopyFinishedResponse( status, lastCommittedTxBeforeStoreCopy ) );
    }
}
