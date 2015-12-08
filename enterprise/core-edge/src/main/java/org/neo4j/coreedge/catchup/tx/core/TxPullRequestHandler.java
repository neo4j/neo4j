/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.catchup.tx.core;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.Supplier;

import org.neo4j.coreedge.catchup.CatchupServerProtocol;
import org.neo4j.coreedge.catchup.ResponseMessageType;
import org.neo4j.coreedge.catchup.tx.edge.TxPullRequest;
import org.neo4j.coreedge.catchup.tx.edge.TxPullResponse;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.IOCursor;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;

import static org.neo4j.coreedge.catchup.CatchupServerProtocol.NextMessage;

public class TxPullRequestHandler extends SimpleChannelInboundHandler<TxPullRequest>
{
    private final CatchupServerProtocol protocol;
    private final StoreId storeId;
    private final TransactionIdStore transactionIdStore;
    private final LogicalTransactionStore logicalTransactionStore;

    public TxPullRequestHandler( CatchupServerProtocol protocol,
                                 Supplier<StoreId> storeIdSupplier,
                                 Supplier<TransactionIdStore> transactionIdStoreSupplier,
                                 Supplier<LogicalTransactionStore> logicalTransactionStoreSupplier )
    {
        this.protocol = protocol;
        this.storeId = storeIdSupplier.get();
        this.transactionIdStore = transactionIdStoreSupplier.get();
        this.logicalTransactionStore = logicalTransactionStoreSupplier.get();
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, final TxPullRequest msg ) throws Exception
    {
        long startTxId = msg.txId();
        long endTxId = startTxId;

        if ( transactionIdStore.getLastCommittedTransactionId() > startTxId )
        {
            IOCursor<CommittedTransactionRepresentation> cursor =
                    logicalTransactionStore.getTransactions( startTxId + 1 );

            while ( cursor.next() )
            {
                ctx.write( ResponseMessageType.TX );
                CommittedTransactionRepresentation tx = cursor.get();
                endTxId = tx.getCommitEntry().getTxId();
                ctx.write( new TxPullResponse( storeId, tx ) );
            }
            ctx.flush();
        }

        ctx.write( ResponseMessageType.TX_STREAM_FINISHED );
        ctx.write( new TxStreamFinishedResponse( endTxId ) );
        ctx.flush();

        protocol.expect( NextMessage.MESSAGE_TYPE );
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
    {
        cause.printStackTrace();
        ctx.close();
    }
}
