/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
import org.neo4j.causalclustering.messaging.EventHandler;
import org.neo4j.causalclustering.messaging.EventHandlerProvider;
import org.neo4j.causalclustering.messaging.EventId;
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.causalclustering.catchup.CatchupResult.E_INVALID_REQUEST;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_STORE_ID_MISMATCH;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_STORE_UNAVAILABLE;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.Begin;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.End;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.Error;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.Info;
import static org.neo4j.causalclustering.messaging.EventHandler.EventState.Warn;
import static org.neo4j.causalclustering.messaging.EventHandler.Param.param;

public class TxPullRequestHandler extends SimpleChannelInboundHandler<TxPullRequest>
{
    private final CatchupServerProtocol protocol;
    private final Supplier<StoreId> storeIdSupplier;
    private final BooleanSupplier databaseAvailable;
    private final TransactionIdStore transactionIdStore;
    private final LogicalTransactionStore logicalTransactionStore;
    private final TxPullRequestsMonitor monitor;
    private final EventHandlerProvider eventHandlerProvider;

    public TxPullRequestHandler( CatchupServerProtocol protocol, Supplier<StoreId> storeIdSupplier,
            BooleanSupplier databaseAvailable, Supplier<TransactionIdStore> transactionIdStoreSupplier,
            Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier, Monitors monitors, EventHandlerProvider eventHandlerProvider )
    {
        this.protocol = protocol;
        this.storeIdSupplier = storeIdSupplier;
        this.databaseAvailable = databaseAvailable;
        this.transactionIdStore = transactionIdStoreSupplier.get();
        this.logicalTransactionStore = logicalTransactionStoreSupplier.get();
        this.monitor = monitors.newMonitor( TxPullRequestsMonitor.class );
        this.eventHandlerProvider = eventHandlerProvider;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final TxPullRequest msg ) throws Exception
    {
        monitor.increment();
        EventHandler eventHandler = eventHandlerProvider.eventHandler( EventId.from( msg.messageId() ) );
        eventHandler.on( Begin, param( "Request", msg ) );
        CatchupResult responseStatus = E_INVALID_REQUEST;
        long lastCommittedTransactionId = -1;

        try
        {
            if ( msg.previousTxId() <= 0 )
            {
                eventHandler.on( Error, "Illegal tx pull request", param( "Previous tx id", msg.previousTxId() ) );
                responseStatus = E_INVALID_REQUEST;
            }
            else
            {
                StoreId localStoreId = storeIdSupplier.get();
                long firstTxId = msg.previousTxId() + 1;
                lastCommittedTransactionId = getLastCommittedTransactionId();

                if ( localStoreId == null || !localStoreId.equals( msg.expectedStoreId() ) )
                {
                    eventHandler.on( Error, "Store id mismatch", param( "Local storeId", localStoreId ), param( "Requested storeId", msg.expectedStoreId() ) );
                    responseStatus = E_STORE_ID_MISMATCH;
                }
                else if ( !databaseAvailable.getAsBoolean() )
                {
                    eventHandler.on( Error, "The local database is unavailable" );
                    responseStatus = E_STORE_UNAVAILABLE;
                }
                else if ( lastCommittedTransactionId < firstTxId )
                {
                    responseStatus = SUCCESS_END_OF_STREAM;
                }
                else
                {
                    IOCursor<CommittedTransactionRepresentation> txCursor = getCursor( firstTxId );
                    if ( txCursor == null )
                    {
                        eventHandler.on( Info, "Transaction does not exits. Consider a less aggressive log pruning strategy" );
                        responseStatus = E_TRANSACTION_PRUNED;
                    }
                    else
                    {
                        ChunkedTransactionStream txStream = new ChunkedTransactionStream( localStoreId, firstTxId, txCursor, protocol );
                        // chunked transaction stream ends the interaction internally and closes the cursor
                        ctx.writeAndFlush( txStream ).addListener( f ->
                        {
                            if ( !f.isSuccess() )
                            {
                                eventHandler.on( Warn, "Failed streaming transactions", f.cause(), param( "FromId", firstTxId ),
                                        param( "ToId", txStream.lastTxId() ), param( "Address", ctx.channel().remoteAddress() ) );
                            }
                        } );
                        responseStatus = SUCCESS_END_OF_STREAM;
                    }
                }
            }
        }
        finally
        {
            eventHandler.on( End, param( "Response status", responseStatus ) );
            endInteraction( ctx, responseStatus, lastCommittedTransactionId );
        }
    }

    private IOCursor<CommittedTransactionRepresentation> getCursor( long firstTxId ) throws IOException
    {
        try
        {
            return logicalTransactionStore.getTransactions( firstTxId );
        }
        catch ( NoSuchTransactionException e )
        {
            return null;
        }
    }

    private long getLastCommittedTransactionId()
    {
        return transactionIdStore.getLastCommittedTransactionId();
    }

    private void endInteraction( ChannelHandlerContext ctx, CatchupResult status, long lastCommittedTransactionId )
    {
        ctx.write( ResponseMessageType.TX_STREAM_FINISHED );
        ctx.writeAndFlush( new TxStreamFinishedResponse( status, lastCommittedTransactionId ) );
        protocol.expect( State.MESSAGE_TYPE );
    }
}
