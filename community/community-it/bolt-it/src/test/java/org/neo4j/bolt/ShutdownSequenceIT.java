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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.bolt.runtime.ExecutorBoltScheduler;
import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory;
import org.neo4j.bolt.v4.messaging.PullMessage;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.fail;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.eqRecord;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;
import static org.neo4j.values.storable.Values.stringValue;

public class ShutdownSequenceIT
{
    private static final String PREFIX = RandomStringUtils.randomAlphabetic( 1000 );
    private static final String USER_AGENT = "TestClient/4.0";

    private AssertableLogProvider internalLogProvider = new AssertableLogProvider();
    private AssertableLogProvider userLogProvider = new AssertableLogProvider();
    private EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(), fsRule, getSettingsFunction() );
    private TransportTestUtil util =
            new TransportTestUtil( BoltProtocolV4ComponentFactory.newNeo4jPack(), BoltProtocolV4ComponentFactory.newMessageEncoder() );
    private HostnamePort address;

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( SuppressOutput.suppressAll() ).around( fsRule ).around( server );

    @Rule
    public OtherThreadRule<Void> otherThread = new OtherThreadRule<>( 1, MINUTES );

    @Before
    public void setup() throws Exception
    {
        address = server.lookupDefaultConnector();

        var procedures = ((GraphDatabaseAPI) server.graphDatabaseService()).getDependencyResolver().resolveDependency( GlobalProcedures.class );
        procedures.registerProcedure( TestProcedures.class );
    }

    @After
    public void tearDown() throws Exception
    {
        userLogProvider.print( System.out );
        internalLogProvider.print( System.out );
    }

    @Test
    public void shutdownShouldResultInFailureMessageForTransactionAwareConnections() throws Exception
    {
        var connection = connectAndAuthenticate();

        // This calls a procedure that creates 1000 nodes with 50ms pauses between each node
        connection.send( util.chunk( new RunMessage( "CALL test.stream.nodes(1000, 50)" ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        // Ask for streaming to start
        connection.send( util.chunk( new PullMessage( ValueUtils.asMapValue( MapUtil.map( "n", -1L ) ) ) ) );

        // Shutdown the server
        server.getManagementService().shutdown();

        // Expect the connection to be delivered a FAILURE message, we don't place a defined status & message expectation
        // for the failure as it could be different
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
        connection.send( util.chunk( new RunMessage( "CALL test.stream.strings($limit, 10)", ValueUtils.asMapValue( MapUtil.map( "limit", count ) ) ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        // Ask for streaming to start
        connection.send( util.chunk( new PullMessage( ValueUtils.asMapValue( MapUtil.map( "n", -1L ) ) ) ) );

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
                Duration.ofSeconds( 10 ) ) );

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

        connection.connect( address ).send( util.acceptedVersions( 4, 0, 0, 0 ) );
        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 4} ) );

        connection.send( util.chunk( new HelloMessage( map( "user_agent", USER_AGENT ) ) ) );
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

    private Consumer<Map<Setting<?>,Object>> getSettingsFunction()
    {
        return settings ->
        {
            settings.put( BoltConnector.encryption_level, OPTIONAL );
            settings.put( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) );
            settings.put( BoltConnector.thread_pool_min_size, 0 );
            settings.put( BoltConnector.thread_pool_max_size, 2 );
            settings.put( BoltConnector.thread_pool_shutdown_wait_time, Duration.ofSeconds( 10 ) );
        };
    }

    public static class TestProcedures
    {
        @Context
        public GraphDatabaseService db;

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

        @Procedure( name = "test.stream.nodes", mode = WRITE )
        public Stream<Output> streamNodes( @Name( value = "limit", defaultValue = "5000" ) long limit,
                @Name( value = "delay", defaultValue = "50" ) long delay )
        {
            AtomicLong counter = new AtomicLong( 0 );
            Output[] props = new Output[(int) limit];

            try ( var txc = db.beginTx() )
            {
                for ( int i = 0; i < limit; i++ )
                {
                    var id = String.format( "%s:%d", PREFIX, counter.getAndAdd( 2 ) );
                    var node = db.createNode( Label.label( "StreamedNode" ) );
                    node.setProperty( "id", id );
                    props[i] = new Output( id );

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
                }

                txc.commit();
            }

            return Arrays.stream( props );
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

