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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.store.CountsOracle;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.kvstore.DataInitializer;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.kernel.impl.store.kvstore.RotationTimeoutException;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;
import org.neo4j.test.Barrier;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.test.extension.actors.Actors;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;
import org.neo4j.time.SystemNanoClock;

import static java.lang.Thread.State.BLOCKED;
import static java.lang.Thread.State.TERMINATED;
import static java.lang.Thread.State.TIMED_WAITING;
import static java.lang.Thread.State.WAITING;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@Actors
@PageCacheExtension
@ExtendWith( {LifeExtension.class} )
class CountsTrackerTest
{
    private LogProvider logProvider = NullLogProvider.getInstance();

    @Inject
    LifeSupport life;
    @Inject
    Actor threading;
    @Inject
    PageCache pageCache;
    @Inject
    FileSystemAbstraction fs;
    @Inject
    TestDirectory testDir;

    @Test
    void shouldBeAbleToStartAndStopTheStore()
    {
        // given
        life.add( newTracker() );

        // when
        life.start();
        life.shutdown();
    }

    @Test
    void shouldBeAbleToWriteDataToCountsTracker() throws Exception
    {
        // given
        life.start();
        CountsTracker tracker = life.add( newTracker() );
        CountsOracle oracle = new CountsOracle();
        {
            CountsOracle.Node a = oracle.node( 1 );
            CountsOracle.Node b = oracle.node( 1 );
            oracle.relationship( a, 1, b );
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
        tracker.rotate( 2 );

        // then
        oracle.verify( tracker );
    }

    @Test
    void shouldStoreCounts() throws Exception
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
    void shouldUpdateCountsOnExistingStore() throws Exception
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
    void detectInMemoryDirtyVersionRead()
    {
        int labelId = 1;
        long lastClosedTransactionId = 11L;
        long writeTransactionId = 22L;
        TxVersionContextSupplier versionContextSupplier = new TxVersionContextSupplier();
        versionContextSupplier.init( () -> lastClosedTransactionId );
        VersionContext versionContext = versionContextSupplier.getVersionContext();

        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker tracker = life.add( newTracker( versionContextSupplier ) );
            try ( CountsAccessor.Updater updater = get( writeTransactionId, tracker ) )
            {
                updater.incrementNodeCount( labelId, 1 );
            }

            versionContext.initRead();
            tracker.nodeCount( labelId, Registers.newDoubleLongRegister() );
            assertTrue( versionContext.isDirty() );
        }
    }

    @Test
    void allowNonDirtyInMemoryDirtyVersionRead()
    {
        int labelId = 1;
        long lastClosedTransactionId = 15L;
        long writeTransactionId = 13L;
        TxVersionContextSupplier versionContextSupplier = new TxVersionContextSupplier();
        versionContextSupplier.init( () -> lastClosedTransactionId );
        VersionContext versionContext = versionContextSupplier.getVersionContext();

        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker tracker = life.add( newTracker( versionContextSupplier ) );
            try ( CountsAccessor.Updater updater = get( writeTransactionId, tracker ) )
            {
                updater.incrementNodeCount( labelId, 1 );
            }

            versionContext.initRead();
            tracker.nodeCount( labelId, Registers.newDoubleLongRegister() );
            assertFalse( versionContext.isDirty() );
        }
    }

    @Test
    void shouldBeAbleToReadUpToDateValueWhileAnotherThreadIsPerformingRotation() throws Exception
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
                    logProvider, fs, pageCache, Config.defaults(), testDir.databaseLayout(), EmptyVersionContextSupplier.EMPTY )
            {
                @Override
                protected boolean include( CountsKey countsKey, ReadableBuffer value )
                {
                    barrier.reached();
                    return super.include( countsKey, value );
                }
            } );
            Future<Void> task = threading.submit( () ->
            {
                delta.update( tracker, secondTransaction );
                tracker.rotate( secondTransaction );
                return null;
            } );

            // then
            barrier.await();
            oracle.verify( tracker );
            barrier.release();
            task.get();
            oracle.verify( tracker );
        }
    }

    @Test
    void shouldOrderStoreByTxIdInHeaderThenMinorVersion()
    {
        // given
        FileVersion version = new FileVersion( 16, 5 );

        // then
        assertTrue( CountsTracker.compare( version, new FileVersion( 5, 5 ) ) > 0 );
        assertEquals( 0, CountsTracker.compare( version, new FileVersion( 16, 5 ) ) );
        assertTrue( CountsTracker.compare( version, new FileVersion( 30, 1 ) ) < 0 );
        assertTrue( CountsTracker.compare( version, new FileVersion( 16, 1 ) ) > 0 );
        assertTrue( CountsTracker.compare( version, new FileVersion( 16, 7 ) ) < 0 );
    }

    @Test
    void shouldNotRotateIfNoDataChanges() throws Exception
    {
        // given
        life.start();
        CountsTracker tracker = life.add( newTracker() );
        File before = tracker.currentFile();

        // when
        tracker.rotate( tracker.txId() );

        // then
        assertSame( before, tracker.currentFile(), "not rotated" );
    }

    @Test
    void shouldSupportTransactionsAppliedOutOfOrderOnRotation() throws Exception
    {
        // given
        life.start();
        final CountsTracker tracker = life.add( newTracker() );
        try ( CountsAccessor.Updater tx = get( 2, tracker ) )
        {
            tx.incrementNodeCount( 1, 1 );
        }
        try ( CountsAccessor.Updater tx = get( 4, tracker ) )
        {
            tx.incrementNodeCount( 1, 1 );
        }

        // when
        Future<Long> rotated = threading.submit( () -> tracker.rotate( 2 ) );
        threading.untilThreadState( BLOCKED, WAITING, TIMED_WAITING, TERMINATED );
        try ( CountsAccessor.Updater tx = get( 5, tracker ) )
        {
            tx.incrementNodeCount( 1, 1 );
        }
        try ( CountsAccessor.Updater tx = get( 3, tracker ) )
        {
            tx.incrementNodeCount( 1, 1 );
        }

        // then
        assertEquals( 4, rotated.get().longValue(), "rotated transaction" );
        assertEquals( 4, tracker.txId(), "stored transaction" );

        // the value in memory
        assertEquals( 4, tracker.nodeCount( 1, Registers.newDoubleLongRegister() ).readSecond(), "count" );

        // the value in the store
        CountsVisitor visitor = mock( CountsVisitor.class );
        tracker.visitFile( tracker.currentFile(), visitor );
        verify( visitor ).visitNodeCount( 1, 3 );
        verifyNoMoreInteractions( visitor );

        assertEquals( 5, tracker.rotate( 5 ), "final rotation" );
    }

    @Test
    void shouldNotEndUpInBrokenStateAfterRotationFailure() throws Exception
    {
        // GIVEN
        life.start();
        FakeClock clock = Clocks.fakeClock();
        CallTrackingClock callTrackingClock = new CallTrackingClock( clock );
        CountsTracker tracker = life.add( newTracker( callTrackingClock, EmptyVersionContextSupplier.EMPTY ) );
        int labelId = 1;
        try ( CountsAccessor.Updater tx = get( 2, tracker ) )
        {
            tx.incrementNodeCount( labelId, 1 ); // now at 1
        }

        // WHEN
        Future<Object> rotation = threading.submit( () -> tracker.rotate( 4 ) );
        threading.untilWaitingIn( "rotate" );
        try ( CountsAccessor.Updater tx = get( 3, tracker ) )
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
        try ( CountsAccessor.Updater tx = get( 4, tracker ) )
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
        return new CountsTracker( logProvider, fs,
                pageCache, Config.defaults(), testDir.databaseLayout(), clock, versionContextSupplier )
                .setInitializer( DataInitializer.empty() );
    }

    private CountsAccessor.Updater get( long writeTransactionId, CountsTracker tracker )
    {
        Optional<CountsAccessor.Updater> updater = tracker.apply( writeTransactionId );
        assertTrue( updater.isPresent() );
        return updater.get();
    }

    private static CountsOracle someData()
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
        return oracle;
    }
}
