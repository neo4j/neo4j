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
package org.neo4j.bolt;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

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
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.Connector.ConnectorType.BOLT;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.TRUE;

public class BoltFailuresIT
{
    private static final int TEST_TIMEOUT = 20_000;

    @Rule
    public final TestDirectory dir = TestDirectory.testDirectory();

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

    @Test( timeout = TEST_TIMEOUT )
    public void throwsWhenWorkerCreationFails()
    {
        WorkerFactory workerFactory = mock( WorkerFactory.class );
        when( workerFactory.newWorker( anyObject(), anyObject(), any() ) ).thenThrow( new IllegalStateException( "Oh!" ) );

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

    @Test( timeout = TEST_TIMEOUT )
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

    @Test( timeout = TEST_TIMEOUT )
    public void throwsWhenInitMessageReceiveFails()
    {
        throwsWhenInitMessageFails( ThrowingSessionMonitor::throwInMessageReceived, false );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void throwsWhenInitMessageProcessingFailsToStart()
    {
        throwsWhenInitMessageFails( ThrowingSessionMonitor::throwInProcessingStarted, false );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void throwsWhenInitMessageProcessingFailsToComplete()
    {
        throwsWhenInitMessageFails( ThrowingSessionMonitor::throwInProcessingDone, true );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void throwsWhenRunMessageReceiveFails()
    {
        throwsWhenRunMessageFails( ThrowingSessionMonitor::throwInMessageReceived );
    }

    @Test( timeout = TEST_TIMEOUT )
    public void throwsWhenRunMessageProcessingFailsToStart()
    {
        throwsWhenRunMessageFails( ThrowingSessionMonitor::throwInProcessingStarted );
    }

    @Test( timeout = TEST_TIMEOUT )
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

    private GraphDatabaseService startDbWithBolt( GraphDatabaseFactory dbFactory )
    {
        return dbFactory.newEmbeddedDatabaseBuilder( dir.graphDbDir() )
                .setConfig( new BoltConnector( "0" ).type, BOLT.name() )
                .setConfig( new BoltConnector( "0" ).enabled, TRUE )
                .setConfig( GraphDatabaseSettings.auth_enabled, FALSE )
                .newGraphDatabase();
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
