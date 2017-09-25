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

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
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
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.Barrier;
import org.neo4j.test.Race;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.fail;

public class RecoveryCleanupIT
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private GraphDatabaseService db;
    private File storeDir;
    private final TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
    private final ExecutorService executor = Executors.newFixedThreadPool( 2 );
    private final Label label = Label.label( "label" );
    private final String propKey = "propKey";
    private Map<Setting,String> testSpecificConfig = new HashMap<>();

    @Before
    public void setup()
    {
        storeDir = testDirectory.graphDbDir();
        testSpecificConfig.clear();
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
            dirtyDatabase();

            // WHEN
            Barrier.Control recoveryCompleteBarrier = new Barrier.Control();
            LabelScanStore.Monitor recoveryBarrierMonitor = new RecoveryBarrierMonitor( recoveryCompleteBarrier );
            setMonitor( recoveryBarrierMonitor );
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

    @Test
    public void scanStoreMustLogCrashPointerCleanupDuringRecovery() throws Exception
    {
        // given
        dirtyDatabase();

        // when
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        factory.setUserLogProvider( logProvider );
        factory.setInternalLogProvider( logProvider );
        startDatabase().shutdown();

        // then
        logProvider.assertContainsLogCallContaining( "Scan store recovery completed" );
    }

    @Test
    public void nativeIndexMustLogCrashPointerCleanupDuringRecovery() throws Exception
    {
        // given
        setTestConfig( GraphDatabaseSettings.enable_native_schema_index, "true" );
        dirtyDatabase();

        // when
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        factory.setUserLogProvider( logProvider );
        factory.setInternalLogProvider( logProvider );
        startDatabase().shutdown();

        // then
        logProvider.assertContainsMessageMatching( Matchers.stringContainsInOrder( Iterables.asIterable(
                "Schema index recovery completed",
                "cleaned crashed pointers",
                "pages visited",
                "Time spent" ) ) );
    }

    private void dirtyDatabase() throws IOException
    {
        db = startDatabase();

        DatabaseHealth databaseHealth = databaseHealth( db );
        index( db );
        someData( db );
        checkpoint( db );
        someData( db );
        databaseHealth.panic( new Throwable( "Trigger recovery on next startup" ) );
        db.shutdown();
        db = null;
    }

    private void setTestConfig( Setting<Boolean> setting, String value )
    {
        testSpecificConfig.put( setting, value );
    }

    private void setMonitor( Object monitor )
    {
        Monitors monitors = new Monitors();
        monitors.addMonitorListener( monitor );
        factory.setMonitors( monitors );
    }

    private void index( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( propKey ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
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
            db.createNode( label ).setProperty( propKey, 1 );
            db.createNode( label ).setProperty( propKey, "string" );
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
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( storeDir );
        testSpecificConfig.forEach( builder::setConfig );
        return builder.newGraphDatabase();
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
