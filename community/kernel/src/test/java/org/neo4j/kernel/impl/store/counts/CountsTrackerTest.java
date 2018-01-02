/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;

import org.neo4j.function.Function;
import org.neo4j.function.IOFunction;
import org.neo4j.function.Predicate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.CountsOracle;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.DataInitializer;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.kernel.impl.store.kvstore.Resources;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.register.Registers;
import org.neo4j.test.Barrier;
import org.neo4j.test.ThreadingRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.impl.store.kvstore.Resources.InitialLifecycle.STARTED;
import static org.neo4j.kernel.impl.store.kvstore.Resources.TestPath.FILE_IN_EXISTING_DIRECTORY;

public class CountsTrackerTest
{
    @Rule
    public final Resources resourceManager = new Resources( FILE_IN_EXISTING_DIRECTORY );
    @Rule
    public final ThreadingRule threading = new ThreadingRule();

    @Test
    public void shouldBeAbleToStartAndStopTheStore() throws Exception
    {
        // given
        resourceManager.managed( newTracker() );

        // when
        resourceManager.lifeStarts();
        resourceManager.lifeShutsDown();
    }

    @Test
    @Resources.Life(STARTED)
    public void shouldBeAbleToWriteDataToCountsTracker() throws Exception
    {
        // given
        CountsTracker tracker = resourceManager.managed( newTracker() );
        CountsOracle oracle = new CountsOracle();
        {
            CountsOracle.Node a = oracle.node( 1 );
            CountsOracle.Node b = oracle.node( 1 );
            oracle.relationship( a, 1, b );
            oracle.indexSampling( 1, 1, 2, 2 );
            oracle.indexUpdatesAndSize( 1, 1, 10, 2 );
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
            updater.incrementIndexUpdates( 1, 1, 2 );
        }

        // then
        oracle.indexUpdatesAndSize( 1, 1, 12, 2 );
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
        int firstTx = 2, secondTx = 3;
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
    public void shouldBeAbleToReadUpToDateValueWhileAnotherThreadIsPerformingRotation() throws Exception
    {
        // given
        CountsOracle oracle = someData();
        final int firstTransaction = 2, secondTransaction = 3;
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
                    new Config(), resourceManager.testPath() )
            {
                @Override
                protected boolean include( CountsKey countsKey, ReadableBuffer value )
                {
                    barrier.reached();
                    return super.include( countsKey, value );
                }
            } );
            Future<Void> task = threading.execute( new Function<CountsTracker, Void>()
            {
                @Override
                public Void apply( CountsTracker tracker )
                {
                    try
                    {
                        delta.update( tracker, secondTransaction );
                        tracker.rotate( secondTransaction );
                    }
                    catch ( IOException e )
                    {
                        throw new AssertionError( e );
                    }
                    return null;
                }
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
    public void shouldOrderStoreByTxIdInHeaderThenMinorVersion() throws Exception
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
    @Resources.Life(STARTED)
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
    @Resources.Life(STARTED)
    public void shouldRotateOnDataChangesEvenIfTransactionIsUnchanged() throws Exception
    {
        // given
        CountsTracker tracker = resourceManager.managed( newTracker() );
        File before = tracker.currentFile();
        try ( CountsAccessor.IndexStatsUpdater updater = tracker.updateIndexCounts() )
        {
            updater.incrementIndexUpdates( 7, 8, 100 );
        }

        // when
        tracker.rotate( tracker.txId() );

        // then
        assertNotEquals( "rotated", before, tracker.currentFile() );
    }

    @Test
    @Resources.Life(STARTED)
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
        Future<Long> rotated = threading.executeAndAwait( new Rotation( 2 ), tracker, new Predicate<Thread>()
        {
            @Override
            public boolean test( Thread thread )
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

    private CountsTracker newTracker()
    {
        return new CountsTracker( resourceManager.logProvider(), resourceManager.fileSystem(),
                resourceManager.pageCache(), new Config(), resourceManager.testPath() )
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
        oracle.indexUpdatesAndSize( 1, 2, 0l, 50l );
        oracle.indexSampling( 1, 2, 25l, 50l );
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
