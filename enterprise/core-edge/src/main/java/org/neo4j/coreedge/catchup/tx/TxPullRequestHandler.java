/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.catchup.tx;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.CatchupServerProtocol;
import org.neo4j.coreedge.catchup.CatchupServerProtocol.State;
import org.neo4j.coreedge.catchup.ResponseMessageType;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.cursor.IOCursor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class TxPullRequestHandler extends SimpleChannelInboundHandler<TxPullRequest>
{
    private final CatchupServerProtocol protocol;
    private final StoreId storeId;
    private final TransactionIdStore transactionIdStore;
    private final LogicalTransactionStore logicalTransactionStore;
    private final TxPullRequestsMonitor monitor;
    private final Log log;

    public TxPullRequestHandler( CatchupServerProtocol protocol,
                                 Supplier<StoreId> storeIdSupplier,
                                 Supplier<TransactionIdStore> transactionIdStoreSupplier,
                                 Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier,
                                 Monitors monitors, LogProvider logProvider )
    {
        this.protocol = protocol;
        this.storeId = storeIdSupplier.get();
        this.transactionIdStore = transactionIdStoreSupplier.get();
        this.logicalTransactionStore = logicalTransactionStoreSupplier.get();
        this.monitor = monitors.newMonitor( TxPullRequestsMonitor.class );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final TxPullRequest msg ) throws Exception
    {
        long startTxId = Math.max( msg.txId(), TransactionIdStore.BASE_TX_ID );
        long endTxId = startTxId;
        boolean success = true;

        if ( !this.storeId.equals( msg.storeId() ) )
        {
            success = false;
            log.info( "Failed to serve TxPullRequest for tx %d and storeId %s because that storeId is different " +
                            "from this machine with %s",
                    endTxId, msg.storeId(), this.storeId );
        }

        else if ( transactionIdStore.getLastCommittedTransactionId() > startTxId )
        {
            try ( IOCursor<CommittedTransactionRepresentation> cursor =
                          logicalTransactionStore.getTransactions( startTxId + 1 ) )
            {
                while ( cursor.next() )
                {
                    ctx.write( ResponseMessageType.TX );
                    CommittedTransactionRepresentation tx = cursor.get();
                    endTxId = tx.getCommitEntry().getTxId();
                    ctx.write( new TxPullResponse( storeId, tx ) );
                }
                ctx.flush();
            }
            catch ( NoSuchTransactionException e )
            {
                success = false;
                log.info( "Failed to serve TxPullRequest for tx %d because the transaction does not exist.", endTxId );
            }
        }
        ctx.write( ResponseMessageType.TX_STREAM_FINISHED );
        ctx.write( new TxStreamFinishedResponse( endTxId, success ) );
        ctx.flush();

        monitor.increment();
        protocol.expect( State.MESSAGE_TYPE );
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
    {
        cause.printStackTrace();
        ctx.close();
    }
}
