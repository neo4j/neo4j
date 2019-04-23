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
package recovery;

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Health;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.Barrier;
import org.neo4j.test.Race;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.Values;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;

@ExtendWith( TestDirectoryExtension.class )
class RecoveryCleanupIT
{
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseService db;
    private TestDatabaseManagementServiceBuilder factory;
    private final ExecutorService executor = Executors.newFixedThreadPool( 2 );
    private final Label label = Label.label( "label" );
    private final String propKey = "propKey";
    private Map<Setting,String> testSpecificConfig = new HashMap<>();
    private DatabaseManagementService managementService;

    @BeforeEach
    void setup()
    {
        testSpecificConfig.clear();
        factory = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() );
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination( 10, TimeUnit.SECONDS );
    }

    @Test
    void recoveryCleanupShouldBlockCheckpoint()
    {
        assertTimeoutPreemptively( ofSeconds( 1000 ), () ->
        {
            // GIVEN
            try
            {
                dirtyDatabase();

                // WHEN
                Barrier.Control recoveryCompleteBarrier = new Barrier.Control();
                LabelScanStore.Monitor recoveryBarrierMonitor = new RecoveryBarrierMonitor( recoveryCompleteBarrier );
                setMonitor( recoveryBarrierMonitor );
                Future<GraphDatabaseService> startDatabaseFuture = executor.submit( () -> db = startDatabase() );
                recoveryCompleteBarrier.awaitUninterruptibly(); // Ensure we are mid recovery cleanup

                // THEN
                shouldWait( startDatabaseFuture );
                recoveryCompleteBarrier.release();
                startDatabaseFuture.get();
            }
            finally
            {
                if ( db != null )
                {
                    managementService.shutdown();
                }
            }
        } );
    }

    @Test
    void scanStoreMustLogCrashPointerCleanupDuringRecovery() throws Exception
    {
        // given
        dirtyDatabase();

        // when
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        factory.setUserLogProvider( logProvider );
        factory.setInternalLogProvider( logProvider );
        startDatabase();
        managementService.shutdown();

        // then
        logProvider.assertContainsLogCallContaining( "Label index cleanup job registered" );
        logProvider.assertContainsLogCallContaining( "Label index cleanup job started" );
        logProvider.assertContainsMessageMatching( Matchers.stringContainsInOrder( Iterables.asIterable(
                "Label index cleanup job finished",
                "Number of pages visited",
                "Number of cleaned crashed pointers",
                "Time spent" ) ) );
        logProvider.assertContainsLogCallContaining( "Label index cleanup job closed" );
    }

    @Test
    void nativeIndexFusion30MustLogCrashPointerCleanupDuringRecovery() throws Exception
    {
        nativeIndexMustLogCrashPointerCleanupDuringRecovery( GraphDatabaseSettings.SchemaIndex.NATIVE30, "native-btree-1.0" );
    }

    @Test
    void nativeIndexBTreeMustLogCrashPointerCleanupDuringRecovery() throws Exception
    {
        nativeIndexMustLogCrashPointerCleanupDuringRecovery( GraphDatabaseSettings.SchemaIndex.NATIVE_BTREE10, "index" );
    }

    private void nativeIndexMustLogCrashPointerCleanupDuringRecovery( GraphDatabaseSettings.SchemaIndex setting, String... subTypes ) throws Exception
    {
        // given
        setTestConfig( GraphDatabaseSettings.default_schema_provider, setting.providerName() );
        dirtyDatabase();

        // when
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        factory.setInternalLogProvider( logProvider );
        startDatabase();
        managementService.shutdown();

        // then
        List<Matcher<String>> matchers = new ArrayList<>();
        for ( String subType : subTypes )
        {
            matchers.add( indexRecoveryLogMatcher( "Schema index cleanup job registered", subType ) );
            matchers.add( indexRecoveryLogMatcher( "Schema index cleanup job started", subType ) );
            matchers.add( indexRecoveryFinishedLogMatcher( subType ) );
            matchers.add( indexRecoveryLogMatcher( "Schema index cleanup job closed", subType ) );
        }
        matchers.forEach( logProvider::assertContainsExactlyOneMessageMatching );
    }

    private static Matcher<String> indexRecoveryLogMatcher( String logMessage, String subIndexProviderKey )
    {
        return Matchers.stringContainsInOrder( Iterables.asIterable(
                logMessage,
                "descriptor",
                "indexFile=",
                File.separator + subIndexProviderKey ) );
    }

    private static Matcher<String> indexRecoveryFinishedLogMatcher( String subIndexProviderKey )
    {

        return Matchers.stringContainsInOrder( Iterables.asIterable(
                "Schema index cleanup job finished",
                "descriptor",
                "indexFile=",
                File.separator + subIndexProviderKey,
                "Number of pages visited",
                "Number of cleaned crashed pointers",
                "Time spent" )
        );
    }

    private void dirtyDatabase() throws IOException
    {
        db = startDatabase();

        Health databaseHealth = databaseHealth( db );
        index( db );
        someData( db );
        checkpoint( db );
        someData( db );
        databaseHealth.panic( new Throwable( "Trigger recovery on next startup" ) );
        managementService.shutdown();
        db = null;
    }

    private void setTestConfig( Setting<?> setting, String value )
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

    private static void reportError( Race.ThrowingRunnable checkpoint, AtomicReference<Throwable> error )
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

    private static void checkpoint( GraphDatabaseService db ) throws IOException
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
            db.createNode( label ).setProperty( propKey, Values.pointValue( Cartesian, 0.5, 0.5 ) );
            db.createNode( label ).setProperty( propKey, LocalTime.of( 0, 0 ) );
            tx.success();
        }
    }

    private static void shouldWait( Future<?> future )
    {
        assertThrows(TimeoutException.class, () -> future.get( 200L, TimeUnit.MILLISECONDS ));
    }

    private GraphDatabaseService startDatabase()
    {
        testSpecificConfig.forEach( factory::setConfig );
        managementService = factory.build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static Health databaseHealth( GraphDatabaseService db )
    {
        return dependencyResolver( db ).resolveDependency( DatabaseHealth.class );
    }

    private static CheckPointer checkPointer( GraphDatabaseService db )
    {
        DependencyResolver dependencyResolver = dependencyResolver( db );
        return dependencyResolver.resolveDependency( Database.class ).getDependencyResolver()
                .resolveDependency( CheckPointer.class );
    }

    private static DependencyResolver dependencyResolver( GraphDatabaseService db )
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver();
    }

    private static class RecoveryBarrierMonitor extends LabelScanStore.Monitor.Adaptor
    {
        private final Barrier.Control barrier;

        RecoveryBarrierMonitor( Barrier.Control barrier )
        {
            this.barrier = barrier;
        }

        @Override
        public void recoveryCleanupFinished( long numberOfPagesVisited, long numberOfTreeNodes, long numberOfCleanedCrashPointers, long durationMillis )
        {
            barrier.reached();
        }
    }
}
