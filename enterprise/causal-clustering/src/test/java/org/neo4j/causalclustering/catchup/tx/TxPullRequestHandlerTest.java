/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Objects;

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
        when( context.writeAndFlush( any() ) ).thenReturn( mock( ChannelFuture.class ) );

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

    class AssertableEventHandler implements EventHandler
    {
        private final LinkedList<EventContext> exectedEventContext = new LinkedList<>();
        private final LinkedList<EventContext> givenNonMatchingEventContext = new LinkedList<>();
        private boolean assertMode = true;

        void setAssertMode()
        {
            assertMode = true;
        }

        void setVerifyingMode()
        {
            assertMode = false;
        }

        @Override
        public void on( EventState eventState, String message, Throwable throwable, Param... params )
        {
            EventContext eventContext = new EventContext( eventState, message, throwable, params );
            if ( assertMode )
            {
                exectedEventContext.add( eventContext );
            }
            else
            {
                if ( !exectedEventContext.remove( eventContext ) )
                {
                    givenNonMatchingEventContext.add( eventContext );
                }
            }
        }

        void assertAllFound()
        {
            assertTrue( "Still contains asserted event: " + Arrays.toString( exectedEventContext.toArray( new EventContext[0] ) ) + ". Got: " +
                    Arrays.toString( givenNonMatchingEventContext.toArray( new EventContext[0] ) ), exectedEventContext.isEmpty() );
        }

        class EventContext
        {
            private final EventState eventState;
            private final String message;
            private final Throwable throwable;
            private final Param[] params;

            public EventContext( EventState eventState, String message, Throwable throwable, Param[] params )
            {

                this.eventState = eventState;
                this.message = message;
                this.throwable = throwable;
                this.params = params;
            }

            @Override
            public boolean equals( Object o )
            {
                if ( this == o )
                {
                    return true;
                }
                if ( o == null || getClass() != o.getClass() )
                {
                    return false;
                }
                EventContext that = (EventContext) o;
                boolean arrayqual = Arrays.equals( params, that.params );
                boolean stateEqual = eventState == that.eventState;
                boolean messageEqual = Objects.equals( message, that.message );
                boolean throwableEquals = Objects.equals( throwable, that.throwable );
                return stateEqual && messageEqual && throwableEquals && arrayqual;
            }

            @Override
            public int hashCode()
            {

                int result = Objects.hash( eventState, message, throwable );
                result = 31 * result + Arrays.hashCode( params );
                return result;
            }

            @Override
            public String toString()
            {
                return "EventContext{" + "eventState=" + eventState + ", message='" + message + '\'' + ", throwable=" + throwable + ", params=" +
                        Arrays.toString( params ) + '}';
            }
        }
    }
}
