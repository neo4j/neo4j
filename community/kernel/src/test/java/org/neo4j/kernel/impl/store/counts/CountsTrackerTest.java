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
package org.neo4j.kernel.impl.store.counts;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import org.neo4j.function.IOFunction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.context.TransactionVersionContextSupplier;
import org.neo4j.kernel.impl.store.CountsOracle;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.kvstore.DataInitializer;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.kernel.impl.store.kvstore.RotationTimeoutException;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.test.Barrier;
import org.neo4j.test.rule.Resources;
import org.neo4j.test.rule.concurrent.ThreadingRule;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.time.SystemNanoClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.function.Predicates.all;
import static org.neo4j.kernel.impl.util.DebugUtil.classNameContains;
import static org.neo4j.kernel.impl.util.DebugUtil.methodIs;
import static org.neo4j.kernel.impl.util.DebugUtil.stackTraceContains;
import static org.neo4j.test.rule.Resources.InitialLifecycle.STARTED;
import static org.neo4j.test.rule.Resources.TestPath.FILE_IN_EXISTING_DIRECTORY;

public class CountsTrackerTest
{
    @Rule
    public final Resources resourceManager = new Resources( FILE_IN_EXISTING_DIRECTORY );
    @Rule
    public final ThreadingRule threading = new ThreadingRule();

    @Test
    public void shouldBeAbleToStartAndStopTheStore()
    {
        // given
        resourceManager.managed( newTracker() );

        // when
        resourceManager.lifeStarts();
        resourceManager.lifeShutsDown();
    }

    @Test
    @Resources.Life( STARTED )
    public void shouldBeAbleToWriteDataToCountsTracker() throws Exception
    {
        // given
        CountsTracker tracker = resourceManager.managed( newTracker() );
        long indexId = 0;
        CountsOracle oracle = new CountsOracle();
        {
            CountsOracle.Node a = oracle.node( 1 );
            CountsOracle.Node b = oracle.node( 1 );
            oracle.relationship( a, 1, b );
            oracle.indexSampling( indexId, 2, 2 );
            oracle.indexUpdatesAndSize( indexId, 10, 2 );
        }

        // when
        oracle.update( tracker, 2 );

        // then
        oracle.verify( tracker );

        // when
        tracker.rotate( 2 );

        // then
        oracle.verify( tracker );

        // when
        try ( CountsAccessor.IndexStatsUpdater updater = tracker.updateIndexCounts() )
        {
            updater.incrementIndexUpdates( indexId, 2 );
        }

        // then
        oracle.indexUpdatesAndSize( indexId, 12, 2 );
        oracle.verify( tracker );

        // when
        tracker.rotate( 2 );

        // then
        oracle.verify( tracker );
    }

    @Test
    public void shouldStoreCounts() throws Exception
    {
        // given
        CountsOracle oracle = someData();

        // when
        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker tracker = life.add( newTracker() );
            oracle.update( tracker, 2 );
            tracker.rotate( 2 );
        }

        // then
        try ( Lifespan life = new Lifespan() )
        {
            oracle.verify( life.add( newTracker() ) );
        }
    }

    @Test
    public void shouldUpdateCountsOnExistingStore() throws Exception
    {
        // given
        CountsOracle oracle = someData();
        int firstTx = 2;
        int secondTx = 3;
        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker tracker = life.add( newTracker() );
            oracle.update( tracker, firstTx );
            tracker.rotate( firstTx );

            oracle.verify( tracker );

            // when
            CountsOracle delta = new CountsOracle();
            {
                CountsOracle.Node n1 = delta.node( 1 );
                CountsOracle.Node n2 = delta.node( 1, 4 );  // Label 4 has not been used before...
                delta.relationship( n1, 1, n2 );
                delta.relationship( n2, 2, n1 ); // relationshipType 2 has not been used before...
            }
            delta.update( tracker, secondTx );
            delta.update( oracle );

            // then
            oracle.verify( tracker );

            // when
            tracker.rotate( secondTx );
        }

        // then
        try ( Lifespan life = new Lifespan() )
        {
            oracle.verify( life.add( newTracker() ) );
        }
    }

    @Test
    public void detectInMemoryDirtyVersionRead()
    {
        int labelId = 1;
        long lastClosedTransactionId = 11L;
        long writeTransactionId = 22L;
        TransactionVersionContextSupplier versionContextSupplier = new TransactionVersionContextSupplier();
        versionContextSupplier.init( () -> lastClosedTransactionId );
        VersionContext versionContext = versionContextSupplier.getVersionContext();

        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker tracker = life.add( newTracker( versionContextSupplier ) );
            try ( CountsAccessor.Updater updater = tracker.apply( writeTransactionId ).get() )
            {
                updater.incrementNodeCount( labelId, 1 );
            }

            versionContext.initRead();
            tracker.nodeCount( labelId, Registers.newDoubleLongRegister() );
            assertTrue( versionContext.isDirty() );
        }
    }

    @Test
    public void allowNonDirtyInMemoryDirtyVersionRead()
    {
        int labelId = 1;
        long lastClosedTransactionId = 15L;
        long writeTransactionId = 13L;
        TransactionVersionContextSupplier versionContextSupplier = new TransactionVersionContextSupplier();
        versionContextSupplier.init( () -> lastClosedTransactionId );
        VersionContext versionContext = versionContextSupplier.getVersionContext();

        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker tracker = life.add( newTracker( versionContextSupplier ) );
            try ( CountsAccessor.Updater updater = tracker.apply( writeTransactionId ).get() )
            {
                updater.incrementNodeCount( labelId, 1 );
            }

            versionContext.initRead();
            tracker.nodeCount( labelId, Registers.newDoubleLongRegister() );
            assertFalse( versionContext.isDirty() );
        }
    }

    @Test
    public void shouldBeAbleToReadUpToDateValueWhileAnotherThreadIsPerformingRotation() throws Exception
    {
        // given
        CountsOracle oracle = someData();
        final int firstTransaction = 2;
        int secondTransaction = 3;
        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker tracker = life.add( newTracker() );
            oracle.update( tracker, firstTransaction );
            tracker.rotate( firstTransaction );
        }

        // when
        final CountsOracle delta = new CountsOracle();
        {
            CountsOracle.Node n1 = delta.node( 1 );
            CountsOracle.Node n2 = delta.node( 1, 4 );  // Label 4 has not been used before...
            delta.relationship( n1, 1, n2 );
            delta.relationship( n2, 2, n1 ); // relationshipType 2 has not been used before...
        }
        delta.update( oracle );

        try ( Lifespan life = new Lifespan() )
        {
            final Barrier.Control barrier = new Barrier.Control();
            CountsTracker tracker = life.add( new CountsTracker(
                    resourceManager.logProvider(), resourceManager.fileSystem(), resourceManager.pageCache(),
                    Config.defaults(), resourceManager.testPath(), EmptyVersionContextSupplier.EMPTY )
            {
                @Override
                protected boolean include( CountsKey countsKey, ReadableBuffer value )
                {
                    barrier.reached();
                    return super.include( countsKey, value );
                }
            } );
            Future<Void> task = threading.execute( t ->
            {
                try
                {
                    delta.update( t, secondTransaction );
                    t.rotate( secondTransaction );
                }
                catch ( IOException e )
                {
                    throw new AssertionError( e );
                }
                return null;
            }, tracker );

            // then
            barrier.await();
            oracle.verify( tracker );
            barrier.release();
            task.get();
            oracle.verify( tracker );
        }
    }

    @Test
    public void shouldOrderStoreByTxIdInHeaderThenMinorVersion()
    {
        // given
        FileVersion version = new FileVersion( 16, 5 );

        // then
        assertTrue( CountsTracker.compare( version, new FileVersion( 5, 5 ) ) > 0 );
        assertTrue( CountsTracker.compare( version, new FileVersion( 16, 5 ) ) == 0 );
        assertTrue( CountsTracker.compare( version, new FileVersion( 30, 1 ) ) < 0 );
        assertTrue( CountsTracker.compare( version, new FileVersion( 16, 1 ) ) > 0 );
        assertTrue( CountsTracker.compare( version, new FileVersion( 16, 7 ) ) < 0 );
    }

    @Test
    @Resources.Life( STARTED )
    public void shouldNotRotateIfNoDataChanges() throws Exception
    {
        // given
        CountsTracker tracker = resourceManager.managed( newTracker() );
        File before = tracker.currentFile();

        // when
        tracker.rotate( tracker.txId() );

        // then
        assertSame( "not rotated", before, tracker.currentFile() );
    }

    @Test
    @Resources.Life( STARTED )
    public void shouldRotateOnDataChangesEvenIfTransactionIsUnchanged() throws Exception
    {
        // given
        CountsTracker tracker = resourceManager.managed( newTracker() );
        File before = tracker.currentFile();
        try ( CountsAccessor.IndexStatsUpdater updater = tracker.updateIndexCounts() )
        {
            updater.incrementIndexUpdates( 7, 100 );
        }

        // when
        tracker.rotate( tracker.txId() );

        // then
        assertNotEquals( "rotated", before, tracker.currentFile() );
    }

    @Test
    @Resources.Life( STARTED )
    public void shouldSupportTransactionsAppliedOutOfOrderOnRotation() throws Exception
    {
        // given
        final CountsTracker tracker = resourceManager.managed( newTracker() );
        try ( CountsAccessor.Updater tx = tracker.apply( 2 ).get() )
        {
            tx.incrementNodeCount( 1, 1 );
        }
        try ( CountsAccessor.Updater tx = tracker.apply( 4 ).get() )
        {
            tx.incrementNodeCount( 1, 1 );
        }

        // when
        Future<Long> rotated = threading.executeAndAwait( new Rotation( 2 ), tracker, thread ->
        {
            switch ( thread.getState() )
            {
            case BLOCKED:
            case WAITING:
            case TIMED_WAITING:
            case TERMINATED:
                return true;
            default:
                return false;
            }
        }, 10, SECONDS );
        try ( CountsAccessor.Updater tx = tracker.apply( 5 ).get() )
        {
            tx.incrementNodeCount( 1, 1 );
        }
        try ( CountsAccessor.Updater tx = tracker.apply( 3 ).get() )
        {
            tx.incrementNodeCount( 1, 1 );
        }

        // then
        assertEquals( "rotated transaction", 4, rotated.get().longValue() );
        assertEquals( "stored transaction", 4, tracker.txId() );

        // the value in memory
        assertEquals( "count", 4, tracker.nodeCount( 1, Registers.newDoubleLongRegister() ).readSecond() );

        // the value in the store
        CountsVisitor visitor = mock( CountsVisitor.class );
        tracker.visitFile( tracker.currentFile(), visitor );
        verify( visitor ).visitNodeCount( 1, 3 );
        verifyNoMoreInteractions( visitor );

        assertEquals( "final rotation", 5, tracker.rotate( 5 ) );
    }

    @Test
    @Resources.Life( STARTED )
    public void shouldNotEndUpInBrokenStateAfterRotationFailure() throws Exception
    {
        // GIVEN
        FakeClock clock = Clocks.fakeClock();
        CallTrackingClock callTrackingClock = new CallTrackingClock( clock );
        CountsTracker tracker = resourceManager.managed( newTracker( callTrackingClock, EmptyVersionContextSupplier.EMPTY ) );
        int labelId = 1;
        try ( CountsAccessor.Updater tx = tracker.apply( 2 ).get() )
        {
            tx.incrementNodeCount( labelId, 1 ); // now at 1
        }

        // WHEN
        Predicate<Thread> arrived = thread ->
            stackTraceContains( thread, all( classNameContains( "Rotation" ), methodIs( "rotate" ) ) );
        Future<Object> rotation = threading.executeAndAwait( t -> t.rotate( 4 ), tracker, arrived, 1, SECONDS );
        try ( CountsAccessor.Updater tx = tracker.apply( 3 ).get() )
        {
            tx.incrementNodeCount( labelId, 1 ); // now at 2
        }
        while ( callTrackingClock.callsToNanos() == 0 )
        {
            Thread.sleep( 10 );
        }
        clock.forward( Config.defaults().get( GraphDatabaseSettings.counts_store_rotation_timeout ).toMillis() * 2, MILLISECONDS );
        try
        {
            rotation.get();
            fail( "Should've failed rotation due to timeout" );
        }
        catch ( ExecutionException e )
        {
            // good
            assertTrue( e.getCause() instanceof RotationTimeoutException );
        }

        // THEN
        Register.DoubleLongRegister register = Registers.newDoubleLongRegister();
        tracker.get( CountsKeyFactory.nodeKey( labelId ), register );
        assertEquals( 2, register.readSecond() );

        // and WHEN later attempting rotation again
        try ( CountsAccessor.Updater tx = tracker.apply( 4 ).get() )
        {
            tx.incrementNodeCount( labelId, 1 ); // now at 3
        }
        tracker.rotate( 4 );

        // THEN
        tracker.get( CountsKeyFactory.nodeKey( labelId ), register );
        assertEquals( 3, register.readSecond() );
    }

    private CountsTracker newTracker()
    {
        return newTracker( Clocks.nanoClock(), EmptyVersionContextSupplier.EMPTY );
    }

    private CountsTracker newTracker( VersionContextSupplier versionContextSupplier )
    {
        return newTracker( Clocks.nanoClock(), versionContextSupplier );
    }

    private CountsTracker newTracker( SystemNanoClock clock, VersionContextSupplier versionContextSupplier )
    {
        return new CountsTracker( resourceManager.logProvider(), resourceManager.fileSystem(),
                resourceManager.pageCache(), Config.defaults(), resourceManager.testPath(), clock,
                versionContextSupplier )
                .setInitializer( new DataInitializer<CountsAccessor.Updater>()
                {
                    @Override
                    public void initialize( CountsAccessor.Updater updater )
                    {
                    }

                    @Override
                    public long initialVersion()
                    {
                        return FileVersion.INITIAL_TX_ID;
                    }
                } );
    }

    private CountsOracle someData()
    {
        CountsOracle oracle = new CountsOracle();
        CountsOracle.Node n0 = oracle.node( 0, 1 );
        CountsOracle.Node n1 = oracle.node( 0, 3 );
        CountsOracle.Node n2 = oracle.node( 2, 3 );
        CountsOracle.Node n3 = oracle.node( 2 );
        oracle.relationship( n0, 1, n2 );
        oracle.relationship( n1, 1, n3 );
        oracle.relationship( n1, 1, n2 );
        oracle.relationship( n0, 1, n3 );
        long indexId = 2;
        oracle.indexUpdatesAndSize( indexId, 0L, 50L );
        oracle.indexSampling( indexId, 25L, 50L );
        return oracle;
    }

    private static class Rotation implements IOFunction<CountsTracker, Long>
    {
        private final long txId;

        Rotation( long txId )
        {
            this.txId = txId;
        }

        @Override
        public Long apply( CountsTracker tracker ) throws IOException
        {
            return tracker.rotate( txId );
        }
    }
}
