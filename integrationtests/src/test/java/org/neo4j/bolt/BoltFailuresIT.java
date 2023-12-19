/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.bolt;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

import java.time.Clock;
import java.util.function.Consumer;

import org.neo4j.bolt.runtime.BoltConnectionMetricsMonitor;
import org.neo4j.bolt.v1.runtime.BoltFactory;
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
    public void throwsWhenMonitoredWorkerCreationFails()
    {
        ThrowingSessionMonitor sessionMonitor = new ThrowingSessionMonitor();
        sessionMonitor.throwInConnectionOpened();
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
        throwsWhenInitMessageFails( ThrowingSessionMonitor::throwInMessageProcessingStarted, false );
    }

    @Test
    public void throwsWhenInitMessageProcessingFailsToComplete()
    {
        throwsWhenInitMessageFails( ThrowingSessionMonitor::throwInMessageProcessingCompleted, true );
    }

    @Test
    public void throwsWhenRunMessageReceiveFails()
    {
        throwsWhenRunMessageFails( ThrowingSessionMonitor::throwInMessageReceived );
    }

    @Test
    public void throwsWhenRunMessageProcessingFailsToStart()
    {
        throwsWhenRunMessageFails( ThrowingSessionMonitor::throwInMessageProcessingStarted );
    }

    @Test
    public void throwsWhenRunMessageProcessingFailsToComplete()
    {
        throwsWhenRunMessageFails( ThrowingSessionMonitor::throwInMessageProcessingCompleted );
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
        when( monitors.newMonitor( BoltConnectionMetricsMonitor.class ) ).thenReturn( sessionMonitor );
        when( monitors.hasListeners( BoltConnectionMetricsMonitor.class ) ).thenReturn( true );
        return monitors;
    }

    private static class ThrowingSessionMonitor implements BoltConnectionMetricsMonitor
    {
        volatile boolean throwInConnectionOpened;
        volatile boolean throwInMessageReceived;
        volatile boolean throwInMessageProcessingStarted;
        volatile boolean throwInMessageProcessingCompleted;

        @Override
        public void connectionOpened()
        {
            throwIfNeeded( throwInConnectionOpened );
        }

        @Override
        public void connectionActivated()
        {

        }

        @Override
        public void connectionWaiting()
        {

        }

        @Override
        public void messageReceived()
        {
            throwIfNeeded( throwInMessageReceived );
        }

        @Override
        public void messageProcessingStarted( long queueTime )
        {
            throwIfNeeded( throwInMessageProcessingStarted );
        }

        @Override
        public void messageProcessingCompleted( long processingTime )
        {
            throwIfNeeded( throwInMessageProcessingCompleted );
        }

        @Override
        public void messageProcessingFailed()
        {

        }

        @Override
        public void connectionClosed()
        {

        }

        void throwInConnectionOpened()
        {
            throwInConnectionOpened = true;
        }

        void throwInMessageReceived()
        {
            throwInMessageReceived = true;
        }

        void throwInMessageProcessingStarted()
        {
            throwInMessageProcessingStarted = true;
        }

        void throwInMessageProcessingCompleted()
        {
            throwInMessageProcessingCompleted = true;
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
