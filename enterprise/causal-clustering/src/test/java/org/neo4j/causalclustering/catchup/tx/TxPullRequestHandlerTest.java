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
package org.neo4j.causalclustering.catchup.tx;

import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.identity.StoreId;
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
import static org.neo4j.causalclustering.catchup.CatchupResult.E_STORE_ID_MISMATCH;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_STORE_UNAVAILABLE;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_BATCH;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.neo4j.kernel.impl.transaction.command.Commands.createNode;
import static org.neo4j.kernel.impl.util.Cursors.cursor;
import static org.neo4j.kernel.impl.util.Cursors.txCursor;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class TxPullRequestHandlerTest
{
    private static final int BATCH_SIZE = 3;
    private final ChannelHandlerContext context = mock( ChannelHandlerContext.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    public void shouldRespondWithCompleteStreamOfTransactions() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );

        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        when( logicalTransactionStore.getTransactions( 14L ) ).thenReturn( txCursor( cursor( tx( 14 ), tx( 15 ) ) ) );

        TxPullRequestHandler txPullRequestHandler =
                new TxPullRequestHandler( new CatchupServerProtocol(), () -> storeId, () -> true,
                        () -> transactionIdStore, () -> logicalTransactionStore, BATCH_SIZE, new Monitors(),
                        NullLogProvider.getInstance() );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 13, storeId ) );

        // then
        verify( context, times( 2 ) ).write( ResponseMessageType.TX );
        verify( context ).write( new TxPullResponse( storeId, tx( 14 ) ) );
        verify( context ).write( new TxPullResponse( storeId, tx( 15 ) ) );

        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).write( new TxStreamFinishedResponse( SUCCESS_END_OF_STREAM ) );
    }

    @Test
    public void shouldRespondWithBatchOfTransactions() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );

        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        when( logicalTransactionStore.getTransactions( 14L ) ).thenReturn( txCursor(
                cursor( tx( 14 ), tx( 15 ), tx( 16 ), tx( 17 ) ) ) );

        TxPullRequestHandler txPullRequestHandler =
                new TxPullRequestHandler( new CatchupServerProtocol(), () -> storeId, () -> true,
                        () -> transactionIdStore, () -> logicalTransactionStore, BATCH_SIZE, new Monitors(),
                        NullLogProvider.getInstance() );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 13, storeId ) );

        // then
        verify( context, times( 3 ) ).write( ResponseMessageType.TX );
        verify( context ).write( new TxPullResponse( storeId, tx( 14 ) ) );
        verify( context ).write( new TxPullResponse( storeId, tx( 15 ) ) );
        verify( context ).write( new TxPullResponse( storeId, tx( 16 ) ) );

        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).write( new TxStreamFinishedResponse( SUCCESS_END_OF_BATCH ) );
    }

    @Test
    public void shouldRespondWithEndOfStreamIfThereAreNoTransactions() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 14L );

        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );

        TxPullRequestHandler txPullRequestHandler =
                new TxPullRequestHandler( new CatchupServerProtocol(), () -> storeId, () -> true,
                        () -> transactionIdStore, () -> logicalTransactionStore, BATCH_SIZE, new Monitors(),
                        NullLogProvider.getInstance() );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 14, storeId ) );

        // then
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).write( new TxStreamFinishedResponse( SUCCESS_END_OF_STREAM ) );
    }

    @Test
    public void shouldRespondWithoutTransactionsIfTheyDoNotExist() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );

        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
        when( logicalTransactionStore.getTransactions( 14L ) ).thenThrow( new NoSuchTransactionException( 14 ) );

        TxPullRequestHandler txPullRequestHandler =
                new TxPullRequestHandler( new CatchupServerProtocol(), () -> storeId, () -> true,
                        () -> transactionIdStore, () -> logicalTransactionStore, BATCH_SIZE, new Monitors(), logProvider );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 13, storeId ) );

        // then
        verify( context, never() ).write( ResponseMessageType.TX );
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).write( new TxStreamFinishedResponse( E_TRANSACTION_PRUNED ) );
        logProvider.assertAtLeastOnce( inLog( TxPullRequestHandler.class )
                .info( "Failed to serve TxPullRequest for tx %d because the transaction does not exist.", 14L ) );
    }

    @Test
    public void shouldNotStreamTxEntriesIfStoreIdMismatches() throws Exception
    {
        // given
        StoreId serverStoreId = new StoreId( 1, 2, 3, 4 );
        StoreId clientStoreId = new StoreId( 5, 6, 7, 8 );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );

        TxPullRequestHandler txPullRequestHandler =
                new TxPullRequestHandler( new CatchupServerProtocol(), () -> serverStoreId, () -> true,
                        () -> transactionIdStore, () -> logicalTransactionStore, BATCH_SIZE, new Monitors(), logProvider );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 1, clientStoreId ) );

        // then
        verify( context, never() ).write( ResponseMessageType.TX );
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).write( new TxStreamFinishedResponse( E_STORE_ID_MISMATCH ) );
        logProvider.assertAtLeastOnce( inLog( TxPullRequestHandler.class )
                .info( "Failed to serve TxPullRequest for tx %d and storeId %s because that storeId is different " +
                        "from this machine with %s", 2L, clientStoreId, serverStoreId ) );
    }

    @Test
    public void shouldNotStreamTxsAndReportErrorIfTheLocalDatabaseIsNotAvailable() throws Exception
    {
        // given
        StoreId storeId = new StoreId( 1, 2, 3, 4 );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );

        TxPullRequestHandler txPullRequestHandler =
                new TxPullRequestHandler( new CatchupServerProtocol(), () -> storeId, () -> false,
                        () -> transactionIdStore, () -> logicalTransactionStore, BATCH_SIZE, new Monitors(), logProvider );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 1, storeId ) );

        // then
        verify( context, never() ).write( ResponseMessageType.TX );
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).write( new TxStreamFinishedResponse( E_STORE_UNAVAILABLE ) );
        logProvider.assertAtLeastOnce( inLog( TxPullRequestHandler.class )
                .info( "Failed to serve TxPullRequest for tx %d because the local database is unavailable.", 2L ) );
    }

    private static CommittedTransactionRepresentation tx( int id )
    {
        return new CommittedTransactionRepresentation(
                new LogEntryStart( id, id, id, id - 1, new byte[]{}, LogPosition.UNSPECIFIED ),
                Commands.transactionRepresentation( createNode( 0 ) ), new OnePhaseCommit( id, id ) );
    }
}
