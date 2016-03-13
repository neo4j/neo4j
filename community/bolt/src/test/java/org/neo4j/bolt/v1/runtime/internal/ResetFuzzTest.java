/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.runtime.internal;

import org.junit.After;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.security.auth.BasicAuthenticationResult;
import org.neo4j.bolt.v1.messaging.MessageHandler;
import org.neo4j.bolt.v1.messaging.message.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.message.Message;
import org.neo4j.bolt.v1.messaging.message.PullAllMessage;
import org.neo4j.bolt.v1.messaging.message.ResetMessage;
import org.neo4j.bolt.v1.messaging.message.RunMessage;
import org.neo4j.bolt.v1.messaging.msgprocess.TransportBridge;
import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.integration.RecordingCallback;
import org.neo4j.bolt.v1.runtime.internal.concurrent.ThreadedSessions;
import org.neo4j.bolt.v1.runtime.spi.RecordStream;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.recorded;
import static org.neo4j.bolt.v1.runtime.integration.SessionMatchers.success;
import static org.neo4j.bolt.v1.runtime.internal.SessionStateMachine.State.IDLE;
import static org.neo4j.helpers.collection.MapUtil.map;

public class ResetFuzzTest
{
    // Because RESET has a "call ahead" mechanism where it will interrupt
    // the session before RESET arrives in order to purge any statements
    // ahead in the message queue, we use this test to convince ourselves
    // there is no code path where RESET causes a session to not go back
    // to a good state.

    private final int seed = new Random().nextInt();

    private final Random rand = new Random( seed );
    private final LifeSupport life = new LifeSupport();
    /** We track the number of un-closed transactions, and fail if we ever leak one */
    private final AtomicLong liveTransactions = new AtomicLong();
    private final Neo4jJobScheduler scheduler = life.add(new Neo4jJobScheduler());
    private final SessionStateMachine ssm = new SessionStateMachine( new FuzzStubSPI() );
    private final ThreadedSessions sessions =
            new ThreadedSessions( ( enc, descriptor ) -> ssm, scheduler, NullLogService.getInstance() );

    private final List<Message> messages = asList(
        new RunMessage( "test", map() ),
        new DiscardAllMessage(),
        new PullAllMessage(),
        new ResetMessage()
    );

    private final List<Message> sent = new LinkedList<>();

    @Test
    public void shouldAlwaysReturnToIdleAfterReset() throws Throwable
    {
        // given
        life.start();
        Session session = sessions.newSession( "<test>" );
        session.init( "Test/0.0.0", map(), null, Session.Callback.NO_OP );

        TransportBridge bridge = new TransportBridge(
                NullLog.getInstance(), session, new MessageHandler.Adapter<>(), ( () -> {} ) );

        // 5 seconds lead to testing ~1M permutations on my 5-year old MBP, so
        // 2 seconds seemed like a sensible balance, testing 300K permutations or
        // so per run without taking up too much test time. Simply bump this to
        // run a longer test. This was green at 300 second runs when this is written.
        long deadline = System.currentTimeMillis() + 2 * 1000;

        // when
        while( System.currentTimeMillis() < deadline )
        {
            dispatchRandomMessages( bridge );
            assertSessionWorks( session );
        }
    }

    private void assertSessionWorks( Session session )
    {
        RecordingCallback recorder = new RecordingCallback();
        session.reset( null, recorder );

        try
        {
            assertThat( recorder, recorded( success() ) );
            assertThat( ssm.state(), equalTo( IDLE ) );
            assertThat( liveTransactions.get(), equalTo( 0L ));
        }
        catch( AssertionError e )
        {
            throw new AssertionError( String.format( "Expected session to return to good state after RESET, but " +
                                                     "assertion failed: %s.%n" +
                                                     "Seed: %s%n" +
                                                     "Messages sent:%n" +
                                                     "%s",
                    e.getMessage(), seed, Iterables.toString( sent, "\n" ) ), e );
        }
    }

    private void dispatchRandomMessages( MessageHandler session )
    {
        for ( int i = 0; i < 50; i++ )
        {
            Message message = messages.get( rand.nextInt( messages.size() ) );
            sent.add( message );
            message.dispatch( session );
        }
    }

    @After
    public void cleanup()
    {
        life.shutdown();
    }

    /**
     * We can't use mockito to create this, because it stores all invocations,
     * so we run out of RAM in like five seconds.
     */
    private class FuzzStubSPI implements SessionStateMachine.SPI
    {
        @Override
        public String connectionDescriptor()
        {
            return "<test>";
        }

        @Override
        public void reportError( Neo4jError err )
        {

        }

        @Override
        public void reportError( String message, Throwable cause )
        {

        }

        @Override
        public KernelTransaction beginTransaction( KernelTransaction.Type type, AccessMode mode )
        {
            liveTransactions.incrementAndGet();
            return new CloseTrackingKernelTransaction();
        }

        @Override
        public void bindTransactionToCurrentThread( KernelTransaction tx )
        {

        }

        @Override
        public void unbindTransactionFromCurrentThread()
        {

        }

        @Override
        public RecordStream run( SessionStateMachine ctx, String statement, Map<String,Object> params )
                throws KernelException
        {
            return RecordStream.EMPTY;
        }

        @Override
        public AuthenticationResult authenticate( Map<String,Object> authToken ) throws AuthenticationException
        {
            return new BasicAuthenticationResult( AccessMode.Static.FULL, false );
        }

        @Override
        public void udcRegisterClient( String clientName )
        {

        }

        @Override
        public Statement currentStatement()
        {
            return null;
        }

    }

    /**
     * Used to track begin/close of transactions, ensuring we never leak
     * a transaction.
     */
    private class CloseTrackingKernelTransaction implements KernelTransaction
    {
        @Override
        public Statement acquireStatement()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void success()
        {

        }

        @Override
        public void failure()
        {

        }

        @Override
        public void close() throws TransactionFailureException
        {
            liveTransactions.decrementAndGet();
        }

        @Override
        public boolean isOpen()
        {
            return false;
        }

        @Override
        public AccessMode mode()
        {
            return null;
        }

        @Override
        public boolean shouldBeTerminated()
        {
            return false;
        }

        @Override
        public void markForTermination()
        {

        }

        @Override
        public void registerCloseListener( CloseListener listener )
        {

        }

        @Override
        public Type transactionType()
        {
            return null;
        }

        @Override
        public Revertable restrict( AccessMode read )
        {
            return null;
        }
    }
}
