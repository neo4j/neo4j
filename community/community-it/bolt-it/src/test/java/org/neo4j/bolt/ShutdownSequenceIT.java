/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.bolt.runtime.scheduling.ExecutorBoltScheduler;
import org.neo4j.bolt.testing.TransportTestUtil;
import org.neo4j.bolt.testing.client.SocketConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.SpiedAssertableLogProvider;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;

import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doAnswer;
import static org.neo4j.bolt.testing.MessageConditions.either;
import static org.neo4j.bolt.testing.MessageConditions.msgFailure;
import static org.neo4j.bolt.testing.MessageConditions.msgRecord;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;
import static org.neo4j.bolt.testing.StreamConditions.eqRecord;
import static org.neo4j.bolt.testing.TransportTestUtil.eventuallyDisconnects;
import static org.neo4j.configuration.connectors.BoltConnector.EncryptionLevel.OPTIONAL;
import static org.neo4j.logging.AssertableLogProvider.Level.DEBUG;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.values.storable.Values.stringValue;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@ExtendWith( {SuppressOutputExtension.class, OtherThreadExtension.class} )
@ResourceLock( Resources.SYSTEM_OUT )
public class ShutdownSequenceIT
{
    private static final Duration THREAD_POOL_SHUTDOWN_WAIT_TIME = Duration.ofSeconds( 10 );

    private final AssertableLogProvider internalLogProvider =
            new SpiedAssertableLogProvider( ExecutorBoltScheduler.class );
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();
    @Inject
    private Neo4jWithSocket server;
    private final TransportTestUtil util = new TransportTestUtil();
    private HostnamePort address;
    private CountDownLatch txStarted;
    private CountDownLatch boltWorkerThreadPoolShuttingDown;

    @BeforeEach
    public void setup( TestInfo testInfo ) throws Exception
    {
        server.setGraphDatabaseFactory( getTestGraphDatabaseFactory() );
        server.setConfigure( getSettingsFunction() );
        server.init( testInfo );
        address = server.lookupDefaultConnector();
        txStarted = new CountDownLatch( 1 );
        boltWorkerThreadPoolShuttingDown = new CountDownLatch( 1 );

        var procedures = ((GraphDatabaseAPI) server.graphDatabaseService()).getDependencyResolver().resolveDependency( GlobalProcedures.class );
        procedures.registerComponent( Pair.class, context -> Pair.of( txStarted, boltWorkerThreadPoolShuttingDown ), true );
        procedures.registerProcedure( TestProcedures.class );
    }

    @AfterEach
    public void tearDown()
    {
        userLogProvider.print( System.out );
        internalLogProvider.print( System.out );
    }

    @Test
    public void shutdownShouldResultInFailureMessageForTransactionAwareConnections() throws Exception
    {
        var connection = connectAndAuthenticate();

        connection.send( util.defaultRunAutoCommitTx( "CALL test.stream.nodes()" ) );

        // Wait for a transaction to start on the server side
        assertTrue( txStarted.await( 1, MINUTES ) );

        // Register a callback when the bolt worker thread pool is shut down.
        var schedulerLog = internalLogProvider.getLog( ExecutorBoltScheduler.class );
        doAnswer( invocation -> {
            invocation.callRealMethod();
            boltWorkerThreadPoolShuttingDown.countDown();
            return null;
        } ).when( schedulerLog ).debug( "Shutting down thread pool" );

        // Shutdown the server
        server.getManagementService().shutdown();

        // Expect the connection to have the following interactions
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                either( msgFailure( Status.Transaction.Terminated, "The transaction has been terminated" ),
                        msgFailure( Status.General.UnknownError, "The transaction has been terminated" ) ) ) );
        assertThat( connection ).satisfies( eventuallyDisconnects() );
        assertThat( internalLogProvider ).forClass( ExecutorBoltScheduler.class )
                .forLevel( DEBUG ).containsMessages( "Thread pool shut down" );
    }

    @Test
    public void shutdownShouldCloseIdleConnections() throws Exception
    {
        // Create an idle connection
        var connection = connectAndAuthenticate();

        // Shutdown the server
        server.getManagementService().shutdown();

        // Expect the connection to be silently closed.
        assertThat( connection ).satisfies( eventuallyDisconnects() );
        assertThat( internalLogProvider ).forClass( ExecutorBoltScheduler.class )
                .forLevel( DEBUG ).containsMessages( "Thread pool shut down" );
    }

    @Test
    public void shutdownShouldWaitForNonTransactionAwareConnections() throws Exception
    {
        var connection = connectAndAuthenticate();

        connection.send( util.defaultRunAutoCommitTx( "CALL test.stream.strings()" ) );

        // Wait for a transaction to start on the server side
        assertTrue( txStarted.await( 1, MINUTES ) );

        // Register a callback when the bolt worker thread pool is shut down.
        var schedulerLog = internalLogProvider.getLog( ExecutorBoltScheduler.class );
        doAnswer( invocation -> {
            invocation.callRealMethod();
            boltWorkerThreadPoolShuttingDown.countDown();
            return null;
        } ).when( schedulerLog ).debug( "Shutting down thread pool" );

        // Initiate the shutdown
        server.getManagementService().shutdown();

        // Expect the connection to have the following interactions
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );
        Condition<AnyValue> equalRecord = new Condition<>( record -> record.equals( stringValue( "0" ) ), "Equal record" );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgRecord( eqRecord( equalRecord ) ) ) );
        assertThat( connection ).satisfies( util.eventuallyReceives(
                either( msgFailure( Status.Transaction.Terminated, "The transaction has been terminated." ),
                        msgFailure( Status.General.UnknownError, "The transaction has been terminated" ) ) ) );
        assertThat( connection ).satisfies( eventuallyDisconnects() );
        assertThat( internalLogProvider ).forClass( ExecutorBoltScheduler.class )
                .forLevel( DEBUG ).containsMessages( "Thread pool shut down" );
    }

    private TransportConnection connectAndAuthenticate() throws Exception
    {
        var connection = new SocketConnection();

        connection.connect( address ).send( util.defaultAcceptedVersions() );
        assertThat( connection ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );

        connection.send( util.defaultAuth() );
        assertThat( connection ).satisfies( util.eventuallyReceives( msgSuccess() ) );

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
        public Pair<CountDownLatch, CountDownLatch> pair;

        @Context
        public Transaction tx;

        @Procedure( name = "test.stream.strings", mode = READ )
        public Stream<Output> streamStrings()
        {
            pair.first().countDown();
            try
            {
                assertTrue( pair.other().await( 1, MINUTES ) );
            }
            catch ( InterruptedException e )
            {
                fail( "Interrupted while waiting for bolt worker threads shut down." );
            }
            // I shall be able to stream this value back.
            // But this procedure tx shall not be able to commit/rollback as dbms is already shutting down.
            return Stream.of( new Output( valueOf( 0 ) ) );
        }

        @Procedure( name = "test.stream.nodes", mode = READ )
        public Stream<Output> streamNodes()
        {
            pair.first().countDown();
            try
            {
                assertTrue( pair.other().await( 1, MINUTES ) );
            }
            catch ( InterruptedException e )
            {
                fail( "Interrupted while waiting for bolt worker threads shut down." );
            }

            // I shall fail to access node id
            tx.getNodeById( 0 );
            return Stream.of( new Output( valueOf( 0 ) ) );
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

