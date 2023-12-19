/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup.tx;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupResult;
import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.CatchupServerProtocol.State;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_INVALID_REQUEST;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_STORE_ID_MISMATCH;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_STORE_UNAVAILABLE;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;

public class TxPullRequestHandler extends SimpleChannelInboundHandler<TxPullRequest>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<StoreId> storeIdSupplier;
    private final BooleanSupplier databaseAvailable;
    private final TransactionIdStore transactionIdStore;
    private final LogicalTransactionStore logicalTransactionStore;
    private final TxPullRequestsMonitor monitor;
    private final Log log;

    public TxPullRequestHandler( CatchupServerProtocol protocol, Supplier<StoreId> storeIdSupplier,
            BooleanSupplier databaseAvailable, Supplier<TransactionIdStore> transactionIdStoreSupplier,
            Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier, Monitors monitors, LogProvider logProvider )
    {
        this.protocol = protocol;
        this.storeIdSupplier = storeIdSupplier;
        this.databaseAvailable = databaseAvailable;
        this.transactionIdStore = transactionIdStoreSupplier.get();
        this.logicalTransactionStore = logicalTransactionStoreSupplier.get();
        this.monitor = monitors.newMonitor( TxPullRequestsMonitor.class );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final TxPullRequest msg ) throws Exception
    {
        monitor.increment();

        if ( msg.previousTxId() <= 0 )
        {
            log.error( "Illegal tx pull request" );
            endInteraction( ctx, E_INVALID_REQUEST, -1 );
            return;
        }

        StoreId localStoreId = storeIdSupplier.get();
        StoreId expectedStoreId = msg.expectedStoreId();

        long firstTxId = msg.previousTxId() + 1;

        /*
         * This is the minimum transaction id we must send to consider our streaming operation successful. The kernel can
         * concurrently prune even future transactions while iterating and the cursor will silently fail on iteration, so
         * we need to add our own protection for this reason and also as a generally important sanity check for the fulfillment
         * of the consistent recovery contract which requires us to stream transactions at least as far as the time when the
         * file copy operation completed.
         */
        long txIdPromise = transactionIdStore.getLastCommittedTransactionId();
        IOCursor<CommittedTransactionRepresentation> txCursor = getCursor( txIdPromise, ctx, firstTxId, localStoreId, expectedStoreId );

        if ( txCursor != null )
        {
            ChunkedTransactionStream txStream = new ChunkedTransactionStream( log, localStoreId, firstTxId, txIdPromise, txCursor, protocol );
            // chunked transaction stream ends the interaction internally and closes the cursor
            ctx.writeAndFlush( txStream ).addListener( f ->
            {
                if ( log.isDebugEnabled() || !f.isSuccess() )
                {
                    String message = format( "Streamed transactions [%d--%d] to %s", firstTxId, txStream.lastTxId(), ctx.channel().remoteAddress() );
                    if ( f.isSuccess() )
                    {
                        log.debug( message );
                    }
                    else
                    {
                        log.warn( message, f.cause() );
                    }
                }
            } );
        }
    }

    private IOCursor<CommittedTransactionRepresentation> getCursor( long txIdPromise, ChannelHandlerContext ctx, long firstTxId,
            StoreId localStoreId, StoreId expectedStoreId ) throws IOException
    {
        if ( localStoreId == null || !localStoreId.equals( expectedStoreId ) )
        {
            log.info( "Failed to serve TxPullRequest for tx %d and storeId %s because that storeId is different " +
                    "from this machine with %s", firstTxId, expectedStoreId, localStoreId );
            endInteraction( ctx, E_STORE_ID_MISMATCH, txIdPromise );
            return null;
        }
        else if ( !databaseAvailable.getAsBoolean() )
        {
            log.info( "Failed to serve TxPullRequest for tx %d because the local database is unavailable.", firstTxId );
            endInteraction( ctx, E_STORE_UNAVAILABLE, txIdPromise );
            return null;
        }
        else if ( txIdPromise < firstTxId )
        {
            endInteraction( ctx, SUCCESS_END_OF_STREAM, txIdPromise );
            return null;
        }

        try
        {
            return logicalTransactionStore.getTransactions( firstTxId );
        }
        catch ( NoSuchTransactionException e )
        {
            log.info( "Failed to serve TxPullRequest for tx %d because the transaction does not exist.", firstTxId );
            endInteraction( ctx, E_TRANSACTION_PRUNED, txIdPromise );
            return null;
        }
    }

    private void endInteraction( ChannelHandlerContext ctx, CatchupResult status, long lastCommittedTransactionId )
    {
        ctx.write( ResponseMessageType.TX_STREAM_FINISHED );
        ctx.writeAndFlush( new TxStreamFinishedResponse( status, lastCommittedTransactionId ) );
        protocol.expect( State.MESSAGE_TYPE );
    }
}
