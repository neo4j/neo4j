/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package recovery;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.Barrier;
import org.neo4j.test.Race;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.fail;

public class RecoveryCleanupIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
    private Barrier.Control recoveryCompleteBarrier;
    private GraphDatabaseService db;
    private File storeDir;
    private ExecutorService executor = Executors.newFixedThreadPool( 2 );
    private Label label = Label.label( "label" );

    @Before
    public void setup()
    {
        storeDir = testDirectory.graphDbDir();

        Monitors monitors = new Monitors();
        recoveryCompleteBarrier = new Barrier.Control();
        LabelScanStore.Monitor recoveryBarrierMonitor = new RecoveryBarrierMonitor( recoveryCompleteBarrier );
        monitors.addMonitorListener( recoveryBarrierMonitor );
        factory.setMonitors( monitors );
    }

    @After
    public void tearDown() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination( 10, TimeUnit.SECONDS );
    }

    @Test
    public void recoveryCleanupShouldBlockCheckpoint() throws Throwable
    {
        // GIVEN
        AtomicReference<Throwable> error = new AtomicReference<>();
        try
        {
            db = startDatabase();

            DatabaseHealth databaseHealth = databaseHealth( db );
            someData( db );
            checkpoint( db );
            someData( db );
            databaseHealth.panic( new Throwable( "Trigger recovery on next startup" ) );
            db.shutdown();
            db = null;

            // WHEN
            db = startDatabase();
            recoveryCompleteBarrier.awaitUninterruptibly(); // Ensure we are mid recovery cleanup

            // THEN
            Future<?> checkpointFuture = executor.submit( () -> reportError( () -> checkpoint( db ), error ) );
            shouldWait( checkpointFuture );
            recoveryCompleteBarrier.release();
            checkpointFuture.get();

            db.shutdown();
        }
        finally
        {
            Throwable throwable = error.get();
            if ( throwable != null )
            {
                throw throwable;
            }
        }
    }

    private void reportError( Race.ThrowingRunnable checkpoint, AtomicReference<Throwable> error )
    {
        try
        {
            checkpoint.run();
        }
        catch ( Throwable t )
        {
            error.compareAndSet( null, t );
        }
    }

    private void checkpoint( GraphDatabaseService db ) throws IOException
    {
        CheckPointer checkPointer = checkPointer( db );
        checkPointer.forceCheckPoint( new SimpleTriggerInfo( "test" ) );
    }

    private void someData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label );
            tx.success();
        }
    }

    private void shouldWait( Future<?> future )throws InterruptedException, ExecutionException
    {
        try
        {
            future.get( 200L, TimeUnit.MILLISECONDS );
            fail( "Expected timeout" );
        }
        catch ( TimeoutException e )
        {
            // good
        }
    }

    private GraphDatabaseService startDatabase()
    {
        return factory.newEmbeddedDatabase( storeDir );
    }

    private DatabaseHealth databaseHealth( GraphDatabaseService db )
    {
        return dependencyResolver( db ).resolveDependency( DatabaseHealth.class );
    }

    private CheckPointer checkPointer( GraphDatabaseService db )
    {
        DependencyResolver dependencyResolver = dependencyResolver( db );
        return dependencyResolver.resolveDependency( NeoStoreDataSource.class ).getDependencyResolver()
                .resolveDependency( CheckPointer.class );
    }

    private DependencyResolver dependencyResolver( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver();
    }

    private class RecoveryBarrierMonitor extends LabelScanStore.Monitor.Adaptor
    {
        private final Barrier.Control barrier;

        RecoveryBarrierMonitor( Barrier.Control barrier )
        {
            this.barrier = barrier;
        }

        @Override
        public void recoveryCompleted( Map<String,Object> data )
        {
            barrier.reached();
        }
    }
}
