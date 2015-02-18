/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.Function;
import org.neo4j.kernel.impl.store.CountsOracle;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.kernel.impl.store.kvstore.Resources;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.test.Barrier;
import org.neo4j.test.ThreadingRule;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.impl.store.kvstore.Resources.InitialLifecycle.STARTED;
import static org.neo4j.kernel.impl.store.kvstore.Resources.TestPath.FILE_IN_EXISTING_DIRECTORY;

public class CountsTrackerTest
{
    public final @Rule Resources the = new Resources( FILE_IN_EXISTING_DIRECTORY );
    public final @Rule ThreadingRule threading = new ThreadingRule();

    @Test
    public void shouldBeAbleToStartAndStopTheStore() throws Exception
    {
        // given
        the.managed( newTracker() );

        // when
        the.lifeStarts();
        the.lifeShutsDown();
    }

    @Test
    @Resources.Life(STARTED)
    public void shouldBeAbleToWriteDataToCountsTracker() throws Exception
    {
        // given
        CountsTracker tracker = the.managed( newTracker() );
        CountsOracle oracle = new CountsOracle();
        {
            CountsOracle.Node a = oracle.node( 1 );
            CountsOracle.Node b = oracle.node( 1 );
            oracle.relationship( a, 1, b );
            oracle.indexSampling( 1, 1, 2, 2 );
            oracle.indexUpdatesAndSize( 1, 1, 10, 2 );
        }

        // when
        oracle.update( tracker );

        // then
        oracle.verify( tracker );

        // when
        tracker.rotate( 17 );

        // then
        oracle.verify( tracker );

        // when
        tracker.incrementIndexUpdates( 1, 1, 2 );

        // then
        oracle.indexUpdatesAndSize( 1, 1, 12, 2 );
        oracle.verify( tracker );

        // when
        tracker.rotate( 18 );

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
            oracle.update( tracker );
            tracker.rotate( 1 );
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
        int txId = 2;
        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker tracker = life.add( newTracker() );
            oracle.update( tracker );
            tracker.rotate( 1 );

            oracle.verify( tracker );

            // when
            CountsOracle delta = new CountsOracle();
            {
                CountsOracle.Node n1 = delta.node( 1 );
                CountsOracle.Node n2 = delta.node( 1, 4 );  // Label 4 has not been used before...
                delta.relationship( n1, 1, n2 );
                delta.relationship( n2, 2, n1 ); // relationshipType 2 has not been used before...
            }
            delta.update( tracker );
            delta.update( oracle );

            // then
            oracle.verify( tracker );

            // when
            tracker.rotate( txId );
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
        int newTxId = 2;
        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker tracker = life.add( newTracker() );
            oracle.update( tracker );
            tracker.rotate( newTxId );
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

        final Barrier.Control barrier = new Barrier.Control();
        try ( Lifespan life = new Lifespan() )
        {
            CountsTracker tracker = life.add( new CountsTracker(
                    the.logger(), the.fileSystem(), the.pageCache(), the.testPath() )
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
                        delta.update( tracker );
                        tracker.rotate( 2 );
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
        CountsTracker tracker = the.managed( newTracker() );
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
        CountsTracker tracker = the.managed( newTracker() );
        File before = tracker.currentFile();
        tracker.incrementIndexUpdates( 7, 8, 100 );

        // when
        tracker.rotate( tracker.txId() );

        // then
        assertNotEquals( "rotated", before, tracker.currentFile() );
    }

    private CountsTracker newTracker()
    {
        return new CountsTracker( the.logger(), the.fileSystem(), the.pageCache(), the.testPath() );
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
}