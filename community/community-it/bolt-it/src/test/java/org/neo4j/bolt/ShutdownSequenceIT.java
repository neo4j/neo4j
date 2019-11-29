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
package org.neo4j.bolt;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.bolt.runtime.scheduling.ExecutorBoltScheduler;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.rule.OtherThreadRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.time.Clocks;
import org.neo4j.time.Stopwatch;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.fail;
import static org.neo4j.bolt.testing.MessageMatchers.msgFailure;
import static org.neo4j.bolt.testing.MessageMatchers.msgRecord;
import static org.neo4j.bolt.testing.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.testing.StreamMatchers.eqRecord;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.values.storable.Values.stringValue;

public class ShutdownSequenceIT
{
    private static final String PREFIX = RandomStringUtils.randomAlphabetic( 1000 );
    private static final Duration THREAD_POOL_SHUTDOWN_WAIT_TIME = Duration.ofSeconds( 10 );

    private final AssertableLogProvider internalLogProvider = new AssertableLogProvider();
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();
    private final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(), fsRule, getSettingsFunction() );
    private final TransportTestUtil util = new TransportTestUtil();
    private HostnamePort address;
    private Semaphore procedureLatch;

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( SuppressOutput.suppressAll() ).around( fsRule ).around( server );

    @Rule
    public OtherThreadRule<Void> otherThread = new OtherThreadRule<>( 1, MINUTES );

    @Before
    public void setup() throws Exception
    {
        address = server.lookupDefaultConnector();
        procedureLatch = new Semaphore( 0 );

        var procedures = ((GraphDatabaseAPI) server.graphDatabaseService()).getDependencyResolver().resolveDependency( GlobalProcedures.class );
        procedures.registerComponent( Semaphore.class, context -> procedureLatch, true );
        procedures.registerProcedure( TestProcedures.class );
    }

    @After
    public void tearDown()
    {
        userLogProvider.print( System.out );
        internalLogProvider.print( System.out );
    }

    @Test
    public void shutdownShouldResultInFailureMessageForTransactionAwareConnections() throws Exception
    {
        var connection = connectAndAuthenticate();

        // Ask for streaming to start
        connection.send( util.defaultRunAutoCommitTx( "CALL test.stream.nodes()" ) );

        // Wait for the procedure to get stuck
        if ( !procedureLatch.tryAcquire( 1, 1, MINUTES ) )
        {
            fail( "Unable to acquire semaphore in a reasonable duration" );
        }

        // Shutdown the server
        server.getManagementService().shutdown();

        // Expect the connection to have the following interactions
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
        assertThat( connection, util.eventuallyReceives( msgRecord( eqRecord( equalTo( stringValue( "0" ) ) ) ) ) );
        assertThat( connection, util.eventuallyReceives( msgFailure() ) );
        assertThat( connection, eventuallyDisconnects() );
        internalLogProvider.assertAtLeastOnce( inLog( ExecutorBoltScheduler.class ).debug( "Thread pool shut down" ) );
    }

    @Test
    public void shutdownShouldCloseIdleConnections() throws Exception
    {
        // Create an idle connection
        var connection = connectAndAuthenticate();

        // Shutdown the server
        server.getManagementService().shutdown();

        // Expect the connection to be silently closed.
        assertThat( connection, eventuallyDisconnects() );
        internalLogProvider.assertAtLeastOnce( inLog( ExecutorBoltScheduler.class ).debug( "Thread pool shut down" ) );
    }

    @Test
    public void shutdownShouldWaitForNonTransactionAwareConnections() throws Exception
    {
        var count = 5_000L;
        var semaphore = new Semaphore( 1 );
        var connection = connectAndAuthenticate();

        // This calls a procedure that generates a stream of strings with 10ms pauses between each item
        // Ask for streaming to start
        connection.send( util.defaultRunAutoCommitTx( "CALL test.stream.strings($limit, 10)", ValueUtils.asMapValue( MapUtil.map( "limit", count ) ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        // Consume records in another thread
        semaphore.acquire();
        var streamFuture = otherThread.execute( w ->
        {
            var value = 0;
            for ( var i = 0; i < count; i++ )
            {
                assertThat( connection,
                        util.eventuallyReceives(
                                msgRecord( eqRecord( equalTo( stringValue( String.valueOf( String.format( "%s-%d", PREFIX, value ) ) ) ) ) ) ) );
                value += 2;

                if ( i == 500 )
                {
                    semaphore.release();
                }
            }
            assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
            return 0;
        } );

        // Wait for the streaming to progress
        if ( !semaphore.tryAcquire( 1, 30, SECONDS ) )
        {
            fail( "Unable to acquire semaphore in a reasonable duration" );
        }

        // Initiate the shutdown
        server.getManagementService().shutdown();

        // Expect the connection to be terminated but the thread pool shutdown to time out
        assertThat( connection, eventuallyDisconnects() );
        internalLogProvider.assertAtLeastOnce( inLog( ExecutorBoltScheduler.class ).warn(
                "Waited %s for the thread pool to shutdown cleanly, but timed out waiting for existing work to finish cleanly",
                THREAD_POOL_SHUTDOWN_WAIT_TIME ) );

        // Also the streaming thread should have been failed
        try
        {
            otherThread.get().awaitFuture( streamFuture );
            fail( "streaming thread should have been failed" );
        }
        catch ( ExecutionException ex )
        {
            assertThat( getRootCause( ex ), isA( IOException.class ) );
        }
    }

    private TransportConnection connectAndAuthenticate() throws Exception
    {
        var connection = new SocketConnection();

        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );

        connection.send( util.defaultAuth() );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        return connection;
    }

    private TestDatabaseManagementServiceBuilder getTestGraphDatabaseFactory()
    {
        TestDatabaseManagementServiceBuilder factory = new TestDatabaseManagementServiceBuilder();
        factory.setInternalLogProvider( internalLogProvider );
        factory.setUserLogProvider( userLogProvider );
        return factory;
    }

    private static Consumer<Map<Setting<?>,Object>> getSettingsFunction()
    {
        return settings ->
        {
            settings.put( BoltConnector.encryption_level, OPTIONAL );
            settings.put( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) );
            settings.put( BoltConnector.thread_pool_min_size, 0 );
            settings.put( BoltConnector.thread_pool_max_size, 2 );
            settings.put( BoltConnector.thread_pool_shutdown_wait_time, THREAD_POOL_SHUTDOWN_WAIT_TIME );
        };
    }

    public static class TestProcedures
    {
        @Context
        public GraphDatabaseService db;

        @Context
        public Semaphore procedureLatch;

        @Procedure( name = "test.stream.strings", mode = READ )
        public Stream<Output> streamStrings( @Name( value = "limit", defaultValue = "0" ) long limit,
                @Name( value = "delay", defaultValue = "100" ) long delay )
        {
            final var value = new AtomicLong( 0 );
            var stream = Stream.generate( () ->
            {
                if ( delay > 0 )
                {
                    try
                    {
                        Thread.sleep( delay );
                    }
                    catch ( InterruptedException exc )
                    {
                        Thread.currentThread().interrupt();
                    }
                }

                return value.getAndAdd( 2 );
            } );

            if ( limit > 0 )
            {
                stream = stream.limit( limit );
            }

            return stream.map( i -> new Output( String.format( "%s-%d", PREFIX, i ) ) );
        }

        @Procedure( name = "test.stream.nodes", mode = READ )
        public Stream<Output> streamNodes()
        {
            final var value = new MutableInt( 0 );

            return Stream.generate( () ->
            {
                int i = value.getAndIncrement();

                if ( i == 1 )
                {
                    try ( var tx = db.beginTx() )
                    {
                        procedureLatch.release();
                        Stopwatch stopwatch = Clocks.nanoClock().startStopWatch();
                        while ( !stopwatch.hasTimedOut( 1, MINUTES ) )
                        {
                            try
                            {
                                tx.getNodeById( 0 );
                            }
                            catch ( NotFoundException ignore )
                            {
                                // We don't expect there to be any
                            }
                            parkNanos( MILLISECONDS.toNanos( 10 ) );
                        }
                        fail( "Transaction was never terminated" );
                    }
                }

                return new Output( String.valueOf( i ) );
            } );
        }

        public static class Output
        {
            public String out;

            Output( String value )
            {
                this.out = value;
            }
        }
    }
}

