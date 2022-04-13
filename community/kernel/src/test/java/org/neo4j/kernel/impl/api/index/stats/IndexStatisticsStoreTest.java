/*
 * Copyright (c) "Neo4j"
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

import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.WriteOnReadOnlyAccessDbException;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.annotations.documented.ReporterFactories.noopReporterFactory;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.test.Race.throwing;

@EphemeralPageCacheExtension
@ExtendWith( RandomExtension.class )
class IndexStatisticsStoreTest
{
    private LifeSupport lifeSupport = new LifeSupport();

    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;

    private IndexStatisticsStore store;
    private final PageCacheTracer pageCacheTracer = new DefaultPageCacheTracer();
    private CursorContextFactory contextFactory;

    @BeforeEach
    void setUp() throws IOException
    {
        contextFactory = new CursorContextFactory( pageCacheTracer, EmptyVersionContextSupplier.EMPTY );
        store = openStore( "stats" );
        lifeSupport.start();
    }

    @AfterEach
    void stop()
    {
        lifeSupport.shutdown();
    }

    private IndexStatisticsStore openStore( String fileName ) throws IOException
    {
        var statisticsStore = new IndexStatisticsStore( pageCache, testDirectory.file( fileName ), immediate(), writable(), DEFAULT_DATABASE_NAME,
                                                        contextFactory, Sets.immutable.empty() );
        return lifeSupport.add( statisticsStore );
    }

    @Test
    void tracePageCacheAccessOnConsistencyCheck() throws IOException
    {
        var store = openStore("consistencyCheck" );
        try ( var cursorContext = contextFactory.create( "tracePageCacheAccessOnConsistencyCheck" ) )
        {
            for ( int i = 0; i < 100; i++ )
            {
                store.replaceStats( i, new IndexSample() );
            }
            store.checkpoint( CursorContext.NULL_CONTEXT );
            store.consistencyCheck( noopReporterFactory(), cursorContext );

            PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
            assertThat( cursorTracer.pins() ).isEqualTo( 16 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 16 );
            assertThat( cursorTracer.hits() ).isEqualTo( 16 );
        }
    }

    @Test
    void tracePageCacheAccessOnStatisticStoreInitialisation() throws IOException
    {
        long initialPins = pageCacheTracer.pins();
        long initialUnpins = pageCacheTracer.unpins();
        long initialHits = pageCacheTracer.hits();
        long initialFaults = pageCacheTracer.faults();

        openStore( "tracedStats" );

        assertThat( pageCacheTracer.faults() - initialFaults ).isEqualTo( 5 );
        assertThat( pageCacheTracer.pins() - initialPins ).isEqualTo( 14 );
        assertThat( pageCacheTracer.unpins() - initialUnpins ).isEqualTo( 14 );
        assertThat( pageCacheTracer.hits() - initialHits ).isEqualTo( 9 );
    }

    @Test
    void tracePageCacheAccessOnCheckpoint() throws IOException
    {
        var store = openStore( "checkpoint" );

        try ( var cursorContext = contextFactory.create( "tracePageCacheAccessOnCheckpoint" ) )
        {
            for ( int i = 0; i < 100; i++ )
            {
                store.replaceStats( i, new IndexSample() );
            }

            store.checkpoint( cursorContext );
            PageCursorTracer cursorTracer = cursorContext.getCursorTracer();
            assertThat( cursorTracer.pins() ).isEqualTo( 36 );
            assertThat( cursorTracer.unpins() ).isEqualTo( 36 );
            assertThat( cursorTracer.hits() ).isEqualTo( 28 );
            assertThat( cursorTracer.faults() ).isEqualTo( 8 );
        }
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
        store.checkpoint( CursorContext.NULL_CONTEXT );
        lifeSupport.shutdown();
        lifeSupport = new LifeSupport();
        store = openStore( "stats" );
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
                store.checkpoint( CursorContext.NULL_CONTEXT );
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
        final Exception e = assertThrows( Exception.class,
                () -> new IndexStatisticsStore( pageCache, testDirectory.file( "non-existing" ), immediate(), readOnly(), DEFAULT_DATABASE_NAME,
                        contextFactory, Sets.immutable.empty() ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof WriteOnReadOnlyAccessDbException ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof TreeFileNotFoundException ) );
        assertTrue( Exceptions.contains( e, t -> t instanceof IllegalStateException ) );
    }

    private void replaceAndVerifySample( long indexId, IndexSample indexSample )
    {
        store.replaceStats( indexId, indexSample );
        assertEquals( indexSample, store.indexSample( indexId ) );
    }
}
