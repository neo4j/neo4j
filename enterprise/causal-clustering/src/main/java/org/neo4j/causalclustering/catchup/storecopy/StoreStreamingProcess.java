package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.cursor.RawCursor;
import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;

import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.SUCCESS;

public class StoreStreamingProcess
{
    private final StoreStreamingProtocol protocol;
    private final Supplier<CheckPointer> checkPointerSupplier;
    private final StoreCopyCheckPointMutex mutex;
    private final StoreResourceStreamFactory resourceStreamFactory;

    public StoreStreamingProcess( StoreStreamingProtocol protocol, Supplier<CheckPointer> checkPointerSupplier, StoreCopyCheckPointMutex mutex, StoreResourceStreamFactory resourceStreamFactory )
    {
        this.protocol = protocol;
        this.checkPointerSupplier = checkPointerSupplier;
        this.mutex = mutex;
        this.resourceStreamFactory = resourceStreamFactory;
    }

    void perform( ChannelHandlerContext ctx ) throws IOException
    {
        CheckPointer checkPointer = checkPointerSupplier.get();

        // The checkpoint-lock will be released when the entire store copy has finished.
        Resource checkPointLock = mutex.storeCopy( () -> checkPointer.tryCheckPoint( new SimpleTriggerInfo( "Store copy" ) ) );
        long lastCheckPointedTx = checkPointer.lastCheckPointedTransactionId();

        RawCursor<StoreResource,IOException> resources = resourceStreamFactory.create();
        protocol.stream( ctx, resources );

        ChannelFuture completion = protocol.end( SUCCESS, ctx, lastCheckPointedTx );
        completion.addListener( f -> checkPointLock.close() );
    }

    public void fail( ChannelHandlerContext ctx, StoreCopyFinishedResponse.Status failureCode )
    {
        protocol.end( failureCode, ctx, -1 );
    }
}
