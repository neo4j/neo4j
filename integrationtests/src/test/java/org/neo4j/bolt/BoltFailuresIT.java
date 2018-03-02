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
package org.neo4j.bolt;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

import java.time.Clock;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.runtime.BoltFactory;
import org.neo4j.bolt.v1.runtime.MonitoredWorkerFactory.SessionMonitor;
import org.neo4j.bolt.v1.runtime.WorkerFactory;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.TRUE;

public class BoltFailuresIT
{
    private static final int TEST_TIMEOUT_SECONDS = 120;

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
        when( workerFactory.newWorker( any(), any() ) ).thenThrow( new IllegalStateException( "Oh!" ) );

        BoltKernelExtension extension = new BoltKernelExtensionWithWorkerFactory( workerFactory );

        int port = PortAuthority.allocatePort();

        db = startDbWithBolt( new GraphDatabaseFactoryWithCustomBoltKernelExtension( extension ), port );

        try
        {
            // attempt to create a driver when server is unavailable
            driver = createDriver( port );
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

        int port = PortAuthority.allocatePort();

        db = startDbWithBolt( new GraphDatabaseFactory().setMonitors( monitors ), port );
        try
        {
            // attempt to create a driver when server is unavailable
            driver = createDriver( port );
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

    private void throwsWhenInitMessageFails( Consumer<ThrowingSessionMonitor> monitorSetup,
            boolean shouldBeAbleToBeginTransaction )
    {
        ThrowingSessionMonitor sessionMonitor = new ThrowingSessionMonitor();
        monitorSetup.accept( sessionMonitor );
        Monitors monitors = newMonitorsSpy( sessionMonitor );

        int port = PortAuthority.allocatePort();

        db = startTestDb( monitors, port );

        try
        {
            driver = GraphDatabase.driver( "bolt://localhost:" + port, Config.build().withoutEncryption().toConfig() );
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

        int port = PortAuthority.allocatePort();

        db = startTestDb( monitors, port );
        driver = createDriver( port );

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

    private GraphDatabaseService startTestDb( Monitors monitors, int port )
    {
        return startDbWithBolt( newDbFactory().setMonitors( monitors ), port );
    }

    private GraphDatabaseService startTestDb( LogProvider internalLogProvider, int port )
    {
        return startDbWithBolt( newDbFactory().setInternalLogProvider( internalLogProvider ), port );
    }

    private GraphDatabaseService startDbWithBolt( GraphDatabaseFactory dbFactory, int port )
    {
        return dbFactory.newEmbeddedDatabaseBuilder( dir.graphDbDir() )
                .setConfig( new BoltConnector( "0" ).type, BOLT.name() )
                .setConfig( new BoltConnector( "0" ).enabled, TRUE )
                .setConfig( new BoltConnector( "0" ).listen_address, "localhost:" + port )
                .setConfig( GraphDatabaseSettings.auth_enabled, FALSE )
                .setConfig( OnlineBackupSettings.online_backup_enabled, FALSE )
                .newGraphDatabase();
    }

    private static TestEnterpriseGraphDatabaseFactory newDbFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }

    private static Driver createDriver( int port )
    {
        return GraphDatabase.driver( "bolt://localhost:" + port, Config.build().withoutEncryption().toConfig() );
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
