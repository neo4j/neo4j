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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.EventHandler;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Commands;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.monitoring.Monitors;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_STORE_ID_MISMATCH;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_STORE_UNAVAILABLE;
import static org.neo4j.causalclustering.catchup.CatchupResult.E_TRANSACTION_PRUNED;
import static org.neo4j.causalclustering.catchup.CatchupResult.SUCCESS_END_OF_STREAM;
import static org.neo4j.causalclustering.messaging.EventHandler.Param.param;
import static org.neo4j.kernel.impl.api.state.StubCursors.cursor;
import static org.neo4j.kernel.impl.transaction.command.Commands.createNode;

public class TxPullRequestHandlerTest
{
    private final ChannelHandlerContext context = mock( ChannelHandlerContext.class );

    private StoreId storeId = new StoreId( 1, 2, 3, 4 );
    private LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );
    private TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
    private AssertableEventHandler eventHandler = new AssertableEventHandler();

    private TxPullRequestHandler txPullRequestHandler = new TxPullRequestHandler( new CatchupServerProtocol(), () -> storeId, () -> true,
            () -> transactionIdStore, () -> logicalTransactionStore, new Monitors(), id -> eventHandler );

    private final String messageId = "id";

    @Before
    public void resetEventHandler()
    {
        eventHandler = new AssertableEventHandler();
    }

    @Test
    public void shouldRespondWithCompleteStreamOfTransactions() throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );
        when( logicalTransactionStore.getTransactions( 14L ) ).thenReturn( txCursor( cursor( tx( 14 ), tx( 15 ) ) ) );
        ChannelFuture channelFuture = mock( ChannelFuture.class );
        when( context.writeAndFlush( any() ) ).thenReturn( channelFuture );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 13, storeId, messageId ) );

        // then
        verify( context ).writeAndFlush( isA( ChunkedTransactionStream.class ) );
    }

    @Test
    public void shouldRespondWithEndOfStreamIfThereAreNoTransactions() throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 14L );

        // when
        txPullRequestHandler.channelRead0( context, new TxPullRequest( 14, storeId, messageId ) );

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
        TxPullRequest request = new TxPullRequest( 13, storeId, messageId );

        // and
        eventHandler.setAssertMode();
        eventHandler.on( EventHandler.EventState.Begin, param( "Request", request ) );
        eventHandler.on( EventHandler.EventState.Info, "Transaction does not exits. Consider a less aggressive log pruning strategy" );
        eventHandler.on( EventHandler.EventState.End, param( "Response status", E_TRANSACTION_PRUNED ) );
        eventHandler.setVerifyingMode();

        // when
        txPullRequestHandler.channelRead0( context, request );

        // then
        verify( context, never() ).write( isA( ChunkedTransactionStream.class ) );
        verify( context, never() ).writeAndFlush( isA( ChunkedTransactionStream.class ) );

        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_TRANSACTION_PRUNED, 15L ) );
        eventHandler.assertAllFound();
    }

    @Test
    public void shouldNotStreamTxEntriesIfStoreIdMismatches() throws Exception
    {
        // given
        StoreId serverStoreId = new StoreId( 1, 2, 3, 4 );
        StoreId clientStoreId = new StoreId( 5, 6, 7, 8 );

        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );
        LogicalTransactionStore logicalTransactionStore = mock( LogicalTransactionStore.class );

        TxPullRequestHandler txPullRequestHandler =
                new TxPullRequestHandler( new CatchupServerProtocol(), () -> serverStoreId, () -> true, () -> transactionIdStore, () -> logicalTransactionStore,
                        new Monitors(), id -> eventHandler );
        TxPullRequest request = new TxPullRequest( 1, clientStoreId, messageId );

        // and
        eventHandler.setAssertMode();
        eventHandler.on( EventHandler.EventState.Begin, param( "Request", request ) );
        eventHandler.on( EventHandler.EventState.Error, "Store id mismatch", param( "Local storeId", serverStoreId ),
                param( "Requested storeId", clientStoreId ) );
        eventHandler.on( EventHandler.EventState.End, param( "Response status", E_STORE_ID_MISMATCH ) );
        eventHandler.setVerifyingMode();

        // when
        txPullRequestHandler.channelRead0( context, request );

        // then
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_STORE_ID_MISMATCH, 15L ) );
        eventHandler.assertAllFound();
    }

    @Test
    public void shouldNotStreamTxsAndReportErrorIfTheLocalDatabaseIsNotAvailable() throws Exception
    {
        // given
        when( transactionIdStore.getLastCommittedTransactionId() ).thenReturn( 15L );

        TxPullRequestHandler txPullRequestHandler =
                new TxPullRequestHandler( new CatchupServerProtocol(), () -> storeId, () -> false, () -> transactionIdStore, () -> logicalTransactionStore,
                        new Monitors(), id -> eventHandler );
        TxPullRequest request = new TxPullRequest( 1, storeId, messageId );

        // and
        eventHandler.setAssertMode();
        eventHandler.on( EventHandler.EventState.Begin, param( "Request", request ) );
        eventHandler.on( EventHandler.EventState.Error, "The local database is unavailable" );
        eventHandler.on( EventHandler.EventState.End, param( "Response status", E_STORE_UNAVAILABLE ) );
        eventHandler.setVerifyingMode();

        // when
        txPullRequestHandler.channelRead0( context, request );

        // then
        verify( context ).write( ResponseMessageType.TX_STREAM_FINISHED );
        verify( context ).writeAndFlush( new TxStreamFinishedResponse( E_STORE_UNAVAILABLE, 15L ) );
        eventHandler.assertAllFound();
    }

    private static CommittedTransactionRepresentation tx( int id )
    {
        return new CommittedTransactionRepresentation(
                new LogEntryStart( id, id, id, id - 1, new byte[]{}, LogPosition.UNSPECIFIED ),
                Commands.transactionRepresentation( createNode( 0 ) ), new LogEntryCommit( id, id ) );
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
            public boolean next()
            {
                return cursor.next();
            }

            @Override
            public void close()
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
