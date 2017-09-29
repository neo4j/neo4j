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
package org.neo4j.bolt;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

import java.time.Clock;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.bolt.v1.runtime.BoltFactory;
import org.neo4j.bolt.v1.runtime.MonitoredWorkerFactory.SessionMonitor;
import org.neo4j.bolt.v1.runtime.WorkerFactory;
import org.neo4j.bolt.v1.transport.BoltMessagingProtocolV1Handler;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.LogMatcher;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.Connector.ConnectorType.BOLT;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.boltConnector;
import static org.neo4j.helpers.collection.Iterators.count;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class BoltFailuresIT
{
    private static final int TEST_TIMEOUT_SECONDS = 120;
    private static final int PREDICATE_AWAIT_TIMEOUT_SECONDS = TEST_TIMEOUT_SECONDS / 2;

    private final TestDirectory dir = TestDirectory.testDirectory();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( Timeout.seconds( TEST_TIMEOUT_SECONDS ) ).around( dir );

    private GraphDatabaseService db;
    private Driver driver;

    @After
    public void shutdownDb()
    {
        if ( db != null )
        {
            db.shutdown();
        }
        IOUtils.closeAllSilently( driver );
    }

    @Test
    public void throwsWhenWorkerCreationFails()
    {
        WorkerFactory workerFactory = mock( WorkerFactory.class );
        when( workerFactory.newWorker( anyObject() ) ).thenThrow( new IllegalStateException( "Oh!" ) );

        BoltKernelExtension extension = new BoltKernelExtensionWithWorkerFactory( workerFactory );

        db = startDbWithBolt( new GraphDatabaseFactoryWithCustomBoltKernelExtension( extension ) );

        try
        {
            // attempt to create a driver when server is unavailable
            driver = createDriver();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        }
    }

    @Test
    public void throwsWhenMonitoredWorkerCreationFails()
    {
        ThrowingSessionMonitor sessionMonitor = new ThrowingSessionMonitor();
        sessionMonitor.throwInSessionStarted();
        Monitors monitors = newMonitorsSpy( sessionMonitor );

        db = startDbWithBolt( new GraphDatabaseFactory().setMonitors( monitors ) );
        try
        {
            // attempt to create a driver when server is unavailable
            driver = createDriver();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        }
    }

    @Test
    public void throwsWhenInitMessageReceiveFails()
    {
        throwsWhenInitMessageFails( ThrowingSessionMonitor::throwInMessageReceived, false );
    }

    @Test
    public void throwsWhenInitMessageProcessingFailsToStart()
    {
        throwsWhenInitMessageFails( ThrowingSessionMonitor::throwInProcessingStarted, false );
    }

    @Test
    public void throwsWhenInitMessageProcessingFailsToComplete()
    {
        throwsWhenInitMessageFails( ThrowingSessionMonitor::throwInProcessingDone, true );
    }

    @Test
    public void throwsWhenRunMessageReceiveFails()
    {
        throwsWhenRunMessageFails( ThrowingSessionMonitor::throwInMessageReceived );
    }

    @Test
    public void throwsWhenRunMessageProcessingFailsToStart()
    {
        throwsWhenRunMessageFails( ThrowingSessionMonitor::throwInProcessingStarted );
    }

    @Test
    public void throwsWhenRunMessageProcessingFailsToComplete()
    {
        throwsWhenRunMessageFails( ThrowingSessionMonitor::throwInProcessingDone );
    }

    @Test
    public void boltServerLogsRealErrorWhenDriverIsClosedWithRunningTransactions() throws Exception
    {
        AssertableLogProvider internalLogProvider = new AssertableLogProvider();
        db = startTestDb( internalLogProvider );

        // create a dummy node
        db.execute( "CREATE (:Node)" ).close();

        // lock that dummy node to make all subsequent writes wait on an exclusive lock
        org.neo4j.graphdb.Transaction tx = db.beginTx();
        Node node = single( db.findNodes( label( "Node" ) ) );
        tx.acquireWriteLock( node );

        Driver driver = createDriver();

        // try to execute write query for the same node through the driver
        Future<?> writeThroughDriverFuture = updateAllNodesAsync( driver );
        // make sure this query is executing and visible in query listing
        awaitNumberOfActiveQueriesToBe( 1 );

        // close driver while it has ongoing transaction, it should get terminated
        driver.close();

        // driver transaction should fail
        expectFailure( writeThroughDriverFuture );

        // make sure there are no active queries
        awaitNumberOfActiveQueriesToBe( 0 );

        // verify that closing of the driver resulted in transaction termination on the server and correct log message
        awaitLogToContainMessage( internalLogProvider, inLog( BoltMessagingProtocolV1Handler.class ).warn(
                startsWith( "Unable to send error back to the client" ),
                instanceOf( TransactionTerminatedException.class ) ) );
    }

    private void throwsWhenInitMessageFails( Consumer<ThrowingSessionMonitor> monitorSetup,
            boolean shouldBeAbleToBeginTransaction )
    {
        ThrowingSessionMonitor sessionMonitor = new ThrowingSessionMonitor();
        monitorSetup.accept( sessionMonitor );
        Monitors monitors = newMonitorsSpy( sessionMonitor );

        db = startTestDb( monitors );

        try
        {
            driver = GraphDatabase.driver( "bolt://localhost", Config.build().withoutEncryption().toConfig() );
            if ( shouldBeAbleToBeginTransaction )
            {
                try ( Session session = driver.session();
                      Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE ()" ).consume();
                }
            }
            else
            {
                fail( "Exception expected" );
            }
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        }
    }

    private void throwsWhenRunMessageFails( Consumer<ThrowingSessionMonitor> monitorSetup )
    {
        ThrowingSessionMonitor sessionMonitor = new ThrowingSessionMonitor();
        Monitors monitors = newMonitorsSpy( sessionMonitor );

        db = startTestDb( monitors );
        driver = createDriver();

        // open a session and start a transaction, this will force driver to obtain
        // a network connection and bind it to the transaction
        Session session = driver.session();
        Transaction tx = session.beginTransaction();

        // at this point driver holds a valid initialize connection
        // setup monitor to throw before running the query to make processing of the RUN message fail
        monitorSetup.accept( sessionMonitor );
        tx.run( "CREATE ()" );
        try
        {
            tx.close();
            session.close();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( ServiceUnavailableException.class ) );
        }
    }

    private GraphDatabaseService startTestDb( Monitors monitors )
    {
        return startDbWithBolt( newDbFactory().setMonitors( monitors ) );
    }

    private GraphDatabaseService startTestDb( LogProvider internalLogProvider )
    {
        return startDbWithBolt( newDbFactory().setInternalLogProvider( internalLogProvider ) );
    }

    private GraphDatabaseService startDbWithBolt( GraphDatabaseFactory dbFactory )
    {
        return dbFactory.newEmbeddedDatabaseBuilder( dir.graphDbDir() )
                .setConfig( boltConnector( "0" ).type, BOLT.name() )
                .setConfig( boltConnector( "0" ).enabled, TRUE )
                .setConfig( GraphDatabaseSettings.auth_enabled, FALSE )
                .newGraphDatabase();
    }

    private void awaitNumberOfActiveQueriesToBe( int value ) throws TimeoutException
    {
        await( () ->
        {
            Result listQueriesResult = db.execute( "CALL dbms.listQueries()" );
            return count( listQueriesResult ) == value + 1; // procedure call itself is also listed
        } );
    }

    private void awaitLogToContainMessage( AssertableLogProvider logProvider, LogMatcher matcher )
            throws TimeoutException
    {
        try
        {
            await( () -> logProvider.containsMatchingLogCall( matcher ) );
        }
        catch ( TimeoutException e )
        {
            System.err.println( "Expected log call did not happen. Full log:" );
            System.err.println( logProvider.serialize() );
            throw e;
        }
    }

    private Future<?> updateAllNodesAsync( Driver driver )
    {
        return runAsync( () ->
        {
            try ( Session session = driver.session() )
            {
                session.run( "MATCH (n) SET n.prop = 42" ).consume();
            }
        } );
    }

    private static void expectFailure( Future<?> future ) throws TimeoutException, InterruptedException
    {
        try
        {
            future.get( 1, MINUTES );
            fail( "Exception expected" );
        }
        catch ( ExecutionException e )
        {
            // expected
            e.printStackTrace();
        }
    }

    private static TestEnterpriseGraphDatabaseFactory newDbFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }

    private static Driver createDriver()
    {
        return GraphDatabase.driver( "bolt://localhost", Config.build().withoutEncryption().toConfig() );
    }

    private static Monitors newMonitorsSpy( ThrowingSessionMonitor sessionMonitor )
    {
        Monitors monitors = spy( new Monitors() );
        // it is not allowed to throw exceptions from monitors
        // make the given sessionMonitor be returned as is, without any proxying
        when( monitors.newMonitor( SessionMonitor.class ) ).thenReturn( sessionMonitor );
        when( monitors.hasListeners( SessionMonitor.class ) ).thenReturn( true );
        return monitors;
    }

    private static void await( Supplier<Boolean> condition ) throws TimeoutException
    {
        Predicates.await( condition, PREDICATE_AWAIT_TIMEOUT_SECONDS, SECONDS );
    }

    private static class BoltKernelExtensionWithWorkerFactory extends BoltKernelExtension
    {
        final WorkerFactory workerFactory;

        BoltKernelExtensionWithWorkerFactory( WorkerFactory workerFactory )
        {
            this.workerFactory = workerFactory;
        }

        @Override
        protected WorkerFactory createWorkerFactory( BoltFactory boltFactory, JobScheduler scheduler,
                Dependencies dependencies, LogService logService, Clock clock )
        {
            return workerFactory;
        }
    }

    private static class ThrowingSessionMonitor implements SessionMonitor
    {
        volatile boolean throwInSessionStarted;
        volatile boolean throwInMessageReceived;
        volatile boolean throwInProcessingStarted;
        volatile boolean throwInProcessingDone;

        @Override
        public void sessionStarted()
        {
            throwIfNeeded( throwInSessionStarted );
        }

        @Override
        public void messageReceived()
        {
            throwIfNeeded( throwInMessageReceived );
        }

        @Override
        public void processingStarted( long queueTime )
        {
            throwIfNeeded( throwInProcessingStarted );
        }

        @Override
        public void processingDone( long processingTime )
        {
            throwIfNeeded( throwInProcessingDone );
        }

        void throwInSessionStarted()
        {
            throwInSessionStarted = true;
        }

        void throwInMessageReceived()
        {
            throwInMessageReceived = true;
        }

        void throwInProcessingStarted()
        {
            throwInProcessingStarted = true;
        }

        void throwInProcessingDone()
        {
            throwInProcessingDone = true;
        }

        void throwIfNeeded( boolean shouldThrow )
        {
            if ( shouldThrow )
            {
                throw new RuntimeException();
            }
        }
    }
}
