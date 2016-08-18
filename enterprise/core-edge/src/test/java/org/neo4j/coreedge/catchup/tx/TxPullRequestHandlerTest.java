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
import org.junit.Test;

import org.neo4j.coreedge.catchup.CatchupServerProtocol;
import org.neo4j.coreedge.catchup.ResponseMessageType;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Commands;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.transaction.command.Commands.createNode;
import static org.neo4j.kernel.impl.util.Cursors.cursor;
import static org.neo4j.kernel.impl.util.Cursors.io;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class TxPullRequestHandlerTest
{
    @Test
    public void shouldRespondWithStreamOfTransactions() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );

        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        when( logicalTransactionStore.getTransactions( 13L ) ).thenReturn( io( cursor(
                tx( 13 ),
                tx( 14 ),
                tx( 15 )
        ) ) );

        TxPullRequestHandler txPullRequestHandler = new TxPullRequestHandler( new CatchupServerProtocol(),
                () -> storeId, () -> transactionIdStore, () -> logicalTransactionStore,
                new Monitors(), NullLogProvider.getInstance() );
        ChannelHandlerContext context = mock( ChannelHandlerContext.class );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 12, storeId ) );

        // then
        verify( context, times( 3 ) ).write( ResponseMessageType.TX );
        verify( context ).write( new TxPullResponse( storeId, tx( 13 ) ) );
        verify( context ).write( new TxPullResponse( storeId, tx( 14 ) ) );
        verify( context ).write( new TxPullResponse( storeId, tx( 15 ) ) );

        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).write( new TxStreamFinishedResponse( 15, true ) );
    }

    @Test
    public void shouldRespondWithoutTransactionsIfTheyDoNotExist() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );

        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        when( logicalTransactionStore.getTransactions( 13L ) ).thenThrow( new NoSuchTransactionException( 13 ) );

        AssertableLogProvider logProvider = new AssertableLogProvider();
        TxPullRequestHandler txPullRequestHandler = new TxPullRequestHandler( new CatchupServerProtocol(),
                () -> storeId, () -> transactionIdStore, () -> logicalTransactionStore,
                new Monitors(), logProvider );
        ChannelHandlerContext context = mock( ChannelHandlerContext.class );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 12, storeId ) );

        // then
        verify( context, never() ).write( ResponseMessageType.TX );
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).write( new TxStreamFinishedResponse( 12, false ) );
        logProvider.assertAtLeastOnce( inLog( TxPullRequestHandler.class )
                .info( "Failed to serve TxPullRequest for tx %d because the transaction does not exist.", 12L ) );
    }

    @Test
    public void shouldNotStreamTxEntriesIfStoreIdMismatches() throws Exception
    {
        // given
        StoreId serverStoreId = new StoreId( 1, 2, 3, 4 );
        StoreId clientStoreId = new StoreId( 5, 6, 7, 8 );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );

        AssertableLogProvider logProvider = new AssertableLogProvider();
        TxPullRequestHandler txPullRequestHandler = new TxPullRequestHandler( new CatchupServerProtocol(),
                () -> serverStoreId, () -> transactionIdStore, () -> logicalTransactionStore,
                new Monitors(), logProvider );
        ChannelHandlerContext context = mock( ChannelHandlerContext.class );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 1, clientStoreId ) );

        // then
        verify( context, never() ).write( ResponseMessageType.TX );
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).write( new TxStreamFinishedResponse( 1, false ) );
        logProvider.assertAtLeastOnce( inLog( TxPullRequestHandler.class )
                .info( "Failed to serve TxPullRequest for tx %d and storeId %s because that storeId is different " +
                        "from this machine with %s", 1L, clientStoreId, serverStoreId ) );
    }

    private static CommittedTransactionRepresentation tx( int id )
    {
        return new CommittedTransactionRepresentation(
                new LogEntryStart( id, id, id, id - 1, new byte[]{}, LogPosition.UNSPECIFIED ),
                Commands.transactionRepresentation( createNode( 0 ) ),
                new OnePhaseCommit( id, id ) );
    }
}
