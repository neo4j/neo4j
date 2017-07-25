/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.IOException;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Commands;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.OnePhaseCommit;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_STORE_ID_MISMATCH;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_STORE_UNAVAILABLE;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.neo4j.kernel.impl.api.state.StubCursors.cursor;
import static org.neo4j.kernel.impl.transaction.command.Commands.createNode;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class TxPullRequestHandlerTest
{
    private final ChannelHandlerContext context = mock( ChannelHandlerContext.class );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    private StoreId storeId = new StoreId( 1, 2 );
    private LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
    private TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );

    private TxPullRequestHandler txPullRequestHandler = new TxPullRequestHandler( new CatchupServerProtocol(), () -> storeId, () -> true,
            () -> transactionIdStore, () -> logicalTransactionStore, new Monitors(), logProvider );

    @Test
    public void shouldRespondWithCompleteStreamOfTransactions() throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );
        when( logicalTransactionStore.getTransactions( 14L ) ).thenReturn( txCursor( cursor( tx( 14 ), tx( 15 ) ) ) );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 13, storeId ) );

        // then
        verify( context ).writeAndFlush( isA( ChunkedTransactionStream.class ) );
    }

    @Test
    public void shouldRespondWithEndOfStreamIfThereAreNoTransactions() throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 14L );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 14, storeId ) );

        // then
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( SUCCESS_END_OF_STREAM, 14L ) );
    }

    @Test
    public void shouldRespondWithoutTransactionsIfTheyDoNotExist() throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );
        when( logicalTransactionStore.getTransactions( 14L ) ).thenThrow( new NoSuchTransactionException( 14 ) );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 13, storeId ) );

        // then
        verify( context, never() ).write( isA( ChunkedTransactionStream.class ) );
        verify( context, never() ).writeAndFlush( isA( ChunkedTransactionStream.class ) );

        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_TRANSACTION_PRUNED, 15L ) );
        logProvider.assertAtLeastOnce( inLog( TxPullRequestHandler.class )
                .info( "Failed to serve TxPullRequest for tx %d because the transaction does not exist.", 14L ) );
    }

    @Test
    public void shouldNotStreamTxEntriesIfStoreIdMismatches() throws Exception
    {
        // given
        StoreId serverStoreId = new StoreId( 1, 2 );
        StoreId clientStoreId = new StoreId( 5, 6 );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );
        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );

        TxPullRequestHandler txPullRequestHandler =
                new TxPullRequestHandler( new CatchupServerProtocol(), () -> serverStoreId, () -> true,
                        () -> transactionIdStore, () -> logicalTransactionStore, new Monitors(), logProvider );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 1, clientStoreId ) );

        // then
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_STORE_ID_MISMATCH, 15L ) );
        logProvider.assertAtLeastOnce( inLog( TxPullRequestHandler.class )
                .info( "Failed to serve TxPullRequest for tx %d and storeId %s because that storeId is different " +
                        "from this machine with %s", 2L, clientStoreId, serverStoreId ) );
    }

    @Test
    public void shouldNotStreamTxsAndReportErrorIfTheLocalDatabaseIsNotAvailable() throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );

        TxPullRequestHandler txPullRequestHandler =
                new TxPullRequestHandler( new CatchupServerProtocol(), () -> storeId, () -> false,
                        () -> transactionIdStore, () -> logicalTransactionStore, new Monitors(), logProvider );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 1, storeId ) );

        // then
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_STORE_UNAVAILABLE, 15L ) );
        logProvider.assertAtLeastOnce( inLog( TxPullRequestHandler.class )
                .info( "Failed to serve TxPullRequest for tx %d because the local database is unavailable.", 2L ) );
    }

    private static CommittedTransactionRepresentation tx( int id )
    {
        return new CommittedTransactionRepresentation(
                new LogEntryStart( id, id, id, id - 1, new byte[]{}, LogPosition.UNSPECIFIED ),
                Commands.transactionRepresentation( createNode( 0 ) ), new OnePhaseCommit( id, id ) );
    }

    private static TransactionCursor txCursor( Cursor<CommittedTransactionRepresentation> cursor )
    {
        return new TransactionCursor()
        {
            @Override
            public LogPosition position()
            {
                throw new UnsupportedOperationException(
                        "LogPosition does not apply when moving a generic cursor over a list of transactions" );
            }

            @Override
            public boolean next() throws IOException
            {
                return cursor.next();
            }

            @Override
            public void close() throws IOException
            {
                cursor.close();
            }

            @Override
            public CommittedTransactionRepresentation get()
            {
                return cursor.get();
            }
        };
    }
}
