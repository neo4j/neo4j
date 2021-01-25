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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.register.Registers.newDoubleLongRegister;
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
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private RandomRule randomRule;

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

        // when
        store.replaceStats( indexId, 123, 456, 0, 0 );

        // then
        assertRegister( 123, 456, store.indexSample( indexId, newDoubleLongRegister() ) );

        // and when
        store.replaceStats( indexId, 444, 555, 0, 0 );

        // then
        assertRegister( 444, 555, store.indexSample( indexId, newDoubleLongRegister() ) );
    }

    @Test
    void shouldReplaceIndexStatistics()
    {
        // given
        long indexId = 4;

        // when
        store.replaceStats( indexId, 0, 0, 123, 456 );

        // then
        assertRegister( 123, 456, store.indexUpdatesAndSize( indexId, newDoubleLongRegister() ) );

        // and when
        store.replaceStats( indexId, 0, 0, 444, 555 );

        // then
        assertRegister( 444, 555, store.indexUpdatesAndSize( indexId, newDoubleLongRegister() ) );
    }

    @Test
    void shouldIncrementIndexUpdates()
    {
        // given
        long indexId = 4;
        store.replaceStats( indexId, 0, 0, 123, 456 );

        // when
        store.incrementIndexUpdates( indexId, 5 );

        // then
        assertRegister( 123 + 5, 456, store.indexUpdatesAndSize( indexId, newDoubleLongRegister() ) );
    }

    @Test
    void shouldStoreDataOnCheckpoint() throws IOException
    {
        // given
        long indexId1 = 1;
        long indexId2 = 2;
        store.replaceStats( indexId1, 100, 200, 15, 20 );
        store.replaceStats( indexId2, 200, 300, 25, 35 );

        // when
        restartStore();

        // then
        assertRegister( 15, 20, store.indexUpdatesAndSize( indexId1, newDoubleLongRegister() ) );
        assertRegister( 25, 35, store.indexUpdatesAndSize( indexId2, newDoubleLongRegister() ) );
        assertRegister( 100, 200, store.indexSample( indexId1, newDoubleLongRegister() ) );
        assertRegister( 200, 300, store.indexSample( indexId2, newDoubleLongRegister() ) );
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
        store.replaceStats( indexId, 0, 0, 0 );
        race.addContestants( contestants, () -> store.incrementIndexUpdates( indexId, delta ), 1 );

        // when
        race.go();

        // then
        assertRegister( contestants * delta, 0, store.indexUpdatesAndSize( indexId, newDoubleLongRegister() ) );
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
            store.replaceStats( indexId, 0, 0, 0 );
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
            assertRegister( expected.get( i ), 0, store.indexUpdatesAndSize( i, newDoubleLongRegister() ) );
        }
        restartStore();
        for ( int i = 0; i < indexes; i++ )
        {
            assertRegister( expected.get( i ), 0, store.indexUpdatesAndSize( i, newDoubleLongRegister() ) );
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
    void shouldNotWriteAnythingInReadOnlyMode() throws IOException
    {
        final File file = testDirectory.file( "existing" );

        // Create store
        IndexStatisticsStore store = new IndexStatisticsStore( pageCache, file, immediate(), false );
        randomActions( store, 1000 );
        byte[] data = readAll( file );

        // Start in readOnly mode
        IndexStatisticsStore readOnlyStore = new IndexStatisticsStore( pageCache, file, immediate(), true );
        randomActions( readOnlyStore, 10000 );

        assertArrayEquals( data, readAll( file ) );
    }

    void randomActions( IndexStatisticsStore store, int numActions ) throws IOException
    {
        try
        {
            store.init();
            for ( int i = 0; i < numActions; i++ )
            {
                randomAction( store );
            }
            store.checkpoint( IOLimiter.UNLIMITED );
        }
        finally
        {
            store.shutdown();
        }
    }

    void randomAction( IndexStatisticsStore store ) throws IOException
    {
        long indexId = randomRule.nextLong( 5 );
        switch ( randomRule.nextInt( 7 ) )
        {
        case 0:
            store.checkpoint( IOLimiter.UNLIMITED );
            break;
        case 1:
            store.indexSample( indexId, Registers.newDoubleLongRegister() );
            break;
        case 2:
            store.indexUpdatesAndSize( indexId, Registers.newDoubleLongRegister() );
            break;
        case 3:
            store.replaceStats( indexId, randomRule.nextLong( 100 ), randomRule.nextLong( 100 ), randomRule.nextLong( 100 ) );
            break;
        case 4:
            store.replaceStats( indexId, randomRule.nextLong( 100 ), randomRule.nextLong( 100 ), randomRule.nextLong( 100 ), randomRule.nextLong( 100 ) );
            break;
        case 5:
            store.incrementIndexUpdates( indexId, randomRule.nextLong( 100 ) );
            break;
        case 6:
            store.removeIndex( indexId );
            break;
        default:
            throw new UnsupportedOperationException( "Unknown Action" );
        }
    }

    byte[] readAll( File file ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.wrap( new byte[(int) (fs.getFileSize( file ) + ByteUnit.mebiBytes( 1 ))] );
        try ( StoreChannel channel = fs.read( file ) )
        {
            channel.read( buffer );
        }
        return buffer.array();
    }

    private static void assertRegister( long first, long second, DoubleLongRegister register )
    {
        assertEquals( first, register.readFirst() );
        assertEquals( second, register.readSecond() );
    }
}
