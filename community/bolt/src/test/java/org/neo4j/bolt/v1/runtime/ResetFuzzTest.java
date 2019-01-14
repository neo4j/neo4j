/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.v1.runtime;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.BoltConnectionFactory;
import org.neo4j.bolt.runtime.BoltSchedulerProvider;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.BoltStateMachineSPI;
import org.neo4j.bolt.runtime.CachedThreadPoolExecutorFactory;
import org.neo4j.bolt.runtime.DefaultBoltConnectionFactory;
import org.neo4j.bolt.runtime.ExecutorBoltSchedulerProvider;
import org.neo4j.bolt.runtime.Neo4jError;
import org.neo4j.bolt.runtime.TransactionStateMachineSPI;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.testing.BoltResponseRecorder;
import org.neo4j.bolt.testing.BoltTestUtil;
import org.neo4j.bolt.testing.RecordedBoltResponse;
import org.neo4j.bolt.transport.TransportThrottleGroup;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.messaging.request.RunMessage;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.SUCCESS;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class ResetFuzzTest
{
    private static final String CONNECTOR = "bolt";
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
    private final Monitors monitors = new Monitors();
    private final JobScheduler scheduler = life.add( createScheduler() );
    private final Config config = createConfig();
    private final BoltSchedulerProvider boltSchedulerProvider = life.add(
            new ExecutorBoltSchedulerProvider( config, new CachedThreadPoolExecutorFactory( NullLog.getInstance() ), scheduler,
                    NullLogService.getInstance() ) );
    private final Clock clock = Clock.systemUTC();
    private final BoltStateMachine machine = new BoltStateMachineV1( new FuzzStubSPI(), BoltTestUtil.newTestBoltChannel(), clock );
    private final BoltConnectionFactory connectionFactory =
            new DefaultBoltConnectionFactory( boltSchedulerProvider, TransportThrottleGroup.NO_THROTTLE,
                    config, NullLogService.getInstance(), clock, monitors );
    private BoltChannel boltChannel;

    private final List<List<RequestMessage>> sequences = asList(
            asList( new RunMessage( "test", EMPTY_MAP ), DiscardAllMessage.INSTANCE ),
            asList( new RunMessage( "test", EMPTY_MAP ), PullAllMessage.INSTANCE ),
            singletonList( new RunMessage( "test", EMPTY_MAP ) )
    );

    private final List<RequestMessage> sent = new LinkedList<>();

    @Before
    public void setup()
    {
        boltChannel = mock( BoltChannel.class, RETURNS_MOCKS );
        when( boltChannel.id() ).thenReturn( UUID.randomUUID().toString() );
        when( boltChannel.connector() ).thenReturn( CONNECTOR );
    }

    @Test
    public void shouldAlwaysReturnToReadyAfterReset() throws Throwable
    {
        // given
        life.start();
        BoltConnection boltConnection = connectionFactory.newConnection( boltChannel, machine );
        boltConnection.enqueue( machine -> machine.process( new InitMessage( "ResetFuzzTest/0.0", emptyMap() ), nullResponseHandler() ) );

        // Test random combinations of messages within a small budget of testing time.
        long deadline = System.currentTimeMillis() + 2 * 1000;

        // when
        while ( System.currentTimeMillis() < deadline )
        {
            dispatchRandomSequenceOfMessages( boltConnection );
            assertSchedulerWorks( boltConnection );
        }
    }

    private void assertSchedulerWorks( BoltConnection connection ) throws InterruptedException
    {
        BoltResponseRecorder recorder = new BoltResponseRecorder();
        connection.enqueue( machine -> machine.process( ResetMessage.INSTANCE, recorder ) );

        try
        {
            RecordedBoltResponse response = recorder.nextResponse();
            assertThat( response.message(), equalTo( SUCCESS ) );
            assertThat( ((BoltStateMachineV1) machine).state(), instanceOf( ReadyState.class ) );
            assertThat( liveTransactions.get(), equalTo( 0L ) );
        }
        catch ( AssertionError e )
        {
            throw new AssertionError( String.format( "Expected session to return to good state after RESET, but " +
                                                     "assertion failed: %s.%n" +
                                                     "Seed: %s%n" +
                                                     "Messages sent:%n" +
                                                     "%s",
                    e.getMessage(), seed, Iterables.toString( sent, "\n" ) ), e );
        }
    }

    private void dispatchRandomSequenceOfMessages( BoltConnection connection )
    {
        List<RequestMessage> sequence = sequences.get( rand.nextInt( sequences.size() ) );
        for ( RequestMessage message: sequence )
        {
            sent.add( message );
            connection.enqueue( stateMachine -> stateMachine.process( message, nullResponseHandler() ) );
        }
    }

    @After
    public void cleanup()
    {
        life.shutdown();
    }

    private static Config createConfig()
    {
        Map<String, String> configProps = new HashMap<>();

        configProps.put( new BoltConnector( CONNECTOR ).enabled.name(), "TRUE" );
        configProps.put( new BoltConnector( CONNECTOR ).listen_address.name(), "localhost:0" );
        configProps.put( new BoltConnector( CONNECTOR ).type.name(), BoltConnector.ConnectorType.BOLT.name() );
        configProps.put( new BoltConnector( CONNECTOR ).thread_pool_min_size.name(), "5" );
        configProps.put( new BoltConnector( CONNECTOR ).thread_pool_max_size.name(), "10" );

        return Config.fromSettings( configProps ).build();
    }

    /**
     * We can't use mockito to create this, because it stores all invocations,
     * so we run out of RAM in like five seconds.
     */
    private class FuzzStubSPI implements BoltStateMachineSPI
    {

        @Override
        public TransactionStateMachineSPI transactionSpi()
        {
            return null;
        }

        @Override
        public void reportError( Neo4jError err )
        {
            // do nothing
        }

        @Override
        public AuthenticationResult authenticate( Map<String,Object> authToken )
        {
            return AuthenticationResult.AUTH_DISABLED;
        }

        @Override
        public void udcRegisterClient( String clientName )
        {
            // do nothing
        }

        @Override
        public String version()
        {
            return "<test-version>";
        }
    }
}
