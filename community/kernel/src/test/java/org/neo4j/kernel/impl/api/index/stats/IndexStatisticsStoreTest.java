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
package org.neo4j.kernel.impl.api.index.stats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.Function;

import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.test.Race.throwing;

@EphemeralPageCacheExtension
class IndexStatisticsStoreTest
{
    private LifeSupport lifeSupport = new LifeSupport();

    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;

    private IndexStatisticsStore store;

    @BeforeEach
    void start()
    {
        store = openStore();
        lifeSupport.start();
    }

    @AfterEach
    void stop()
    {
        lifeSupport.shutdown();
    }

    private IndexStatisticsStore openStore()
    {
        return lifeSupport.add(
                new IndexStatisticsStore( pageCache, testDirectory.file( "stats" ), immediate(), false ) );
    }

    @Test
    void shouldReplaceIndexSample()
    {
        // given
        long indexId = 4;

        // when/then
        replaceAndVerifySample( indexId, new IndexSample( 456, 123, 456, 3 ) );
        replaceAndVerifySample( indexId, new IndexSample( 555, 444, 550, 0 ) );
    }

    @Test
    void shouldIncrementIndexUpdates()
    {
        // given
        long indexId = 4;
        IndexSample initialSample = new IndexSample( 456, 5, 200, 123 );
        store.replaceStats( indexId, initialSample );

        // when
        int addedUpdates = 5;
        store.incrementIndexUpdates( indexId, addedUpdates );

        // then
        assertEquals( new IndexSample( initialSample.indexSize(), initialSample.uniqueValues(), initialSample.sampleSize(),
                initialSample.updates() + addedUpdates ), store.indexSample( indexId ) );
    }

    @Test
    void shouldStoreDataOnCheckpoint() throws IOException
    {
        // given
        long indexId1 = 1;
        long indexId2 = 2;
        IndexSample sample1 = new IndexSample( 500, 100, 200, 25 );
        IndexSample sample2 = new IndexSample( 501, 101, 201, 26 );
        store.replaceStats( indexId1, sample1 );
        store.replaceStats( indexId2, sample2 );

        // when
        restartStore();

        // then
        assertEquals( sample1, store.indexSample( indexId1 ) );
        assertEquals( sample2, store.indexSample( indexId2 ) );
    }

    private void restartStore() throws IOException
    {
        store.checkpoint( IOLimiter.UNLIMITED );
        lifeSupport.shutdown();
        lifeSupport = new LifeSupport();
        store = openStore();
        lifeSupport.start();
    }

    @Test
    void shouldAllowMultipleThreadsIncrementIndexUpdates() throws Throwable
    {
        // given
        long indexId = 5;
        Race race = new Race();
        int contestants = 20;
        int delta = 3;
        store.replaceStats( indexId, new IndexSample( 0, 0, 0 ) );
        race.addContestants( contestants, () -> store.incrementIndexUpdates( indexId, delta ), 1 );

        // when
        race.go();

        // then
        assertEquals( new IndexSample( 0, 0, 0, contestants * delta ), store.indexSample( indexId ) );
    }

    @Test
    void shouldHandleConcurrentUpdatesWithCheckpointing() throws Throwable
    {
        // given
        Race race = new Race();
        AtomicBoolean checkpointDone = new AtomicBoolean();
        int contestantsPerIndex = 5;
        int indexes = 3;
        int delta = 5;
        AtomicIntegerArray expected = new AtomicIntegerArray( indexes );
        race.addContestant( throwing( () ->
        {
            for ( int i = 0; i < 20; i++ )
            {
                Thread.sleep( 5 );
                store.checkpoint( IOLimiter.UNLIMITED );
            }
            checkpointDone.set( true );
        } ) );
        for ( int i = 0; i < indexes; i++ )
        {
            int indexId = i;
            store.replaceStats( indexId, new IndexSample( 0, 0, 0 ) );
            race.addContestants( contestantsPerIndex, () ->
            {
                while ( !checkpointDone.get() )
                {
                    store.incrementIndexUpdates( indexId, delta );
                    expected.addAndGet( indexId, delta );
                }
            } );
        }

        // when
        race.go();

        // then
        for ( int i = 0; i < indexes; i++ )
        {
            assertEquals( new IndexSample( 0, 0, 0, expected.get( i ) ), store.indexSample( i ) );
        }
        restartStore();
        for ( int i = 0; i < indexes; i++ )
        {
            assertEquals( new IndexSample( 0, 0, 0, expected.get( i ) ), store.indexSample( i ) );
        }
    }

    @Test
    void shouldNotStartWithoutFileIfReadOnly()
    {
        final IndexStatisticsStore indexStatisticsStore = new IndexStatisticsStore( pageCache, testDirectory.file( "non-existing" ), immediate(), true );
        final Exception e = assertThrows( Exception.class, indexStatisticsStore::init );
        assertTrue( Exceptions.contains( e, t -> t instanceof NoSuchFileException ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof TreeFileNotFoundException ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof IllegalStateException ) );
    }

    @Test
    void shouldNotReplaceStatsIfReadOnly() throws IOException
    {
        assertOperationThrowInReadOnlyMode( iss -> () -> iss.replaceStats( 1, new IndexSample( 1, 1, 1 ) ) );
    }

    @Test
    void shouldNotRemoveIndexIfReadOnly() throws IOException
    {
        assertOperationThrowInReadOnlyMode( iss -> () -> iss.removeIndex( 1 ) );
    }

    @Test
    void shouldNotIncrementIndexUpdatesIfReadOnly() throws IOException
    {
        assertOperationThrowInReadOnlyMode( iss -> () -> iss.incrementIndexUpdates( 1, 1 ) );
    }

    private void assertOperationThrowInReadOnlyMode( Function<IndexStatisticsStore,Executable> operation ) throws IOException
    {
        final File file = testDirectory.file( "existing" );

        // Create store
        IndexStatisticsStore store = new IndexStatisticsStore( pageCache, file, immediate(), false );
        try
        {
            store.init();
        }
        finally
        {
            store.shutdown();
        }

        // Start in readOnly mode
        IndexStatisticsStore readOnlyStore = new IndexStatisticsStore( pageCache, file, immediate(), true );
        try
        {
            readOnlyStore.init();
            final UnsupportedOperationException e = assertThrows( UnsupportedOperationException.class, operation.apply( readOnlyStore ) );
            assertEquals( "Can not write to index statistics store while in read only mode.", e.getMessage() );
        }
        finally
        {
            readOnlyStore.shutdown();
        }
    }

    private void replaceAndVerifySample( long indexId, IndexSample indexSample )
    {
        store.replaceStats( indexId, indexSample );
        assertEquals( indexSample, store.indexSample( indexId ) );
    }
}
