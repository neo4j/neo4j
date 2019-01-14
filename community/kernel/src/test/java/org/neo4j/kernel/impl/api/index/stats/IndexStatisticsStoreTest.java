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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.test.Race;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.register.Registers.newDoubleLongRegister;
import static org.neo4j.test.Race.throwing;

public class IndexStatisticsStoreTest
{
    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();

    private LifeSupport lifeSupport = new LifeSupport();

    private IndexStatisticsStore store;

    @Before
    public void start()
    {
        store = openStore();
        lifeSupport.start();
    }

    private IndexStatisticsStore openStore()
    {
        return lifeSupport.add(
                new IndexStatisticsStore( storage.pageCache(), storage.directory().file( "stats" ), RecoveryCleanupWorkCollector.immediate() ) );
    }

    @After
    public void stop()
    {
        lifeSupport.shutdown();
    }

    @Test
    public void shouldReplaceIndexSample()
    {
        // given
        long indexId = 4;

        // when
        store.replaceIndexSample( indexId, 123, 456 );

        // then
        assertRegister( 123, 456, store.indexSample( indexId, newDoubleLongRegister() ) );

        // and when
        store.replaceIndexSample( indexId, 444, 555 );

        // then
        assertRegister( 444, 555, store.indexSample( indexId, newDoubleLongRegister() ) );
    }

    @Test
    public void shouldReplaceIndexStatistics()
    {
        // given
        long indexId = 4;

        // when
        store.replaceIndexUpdateAndSize( indexId, 123, 456 );

        // then
        assertRegister( 123, 456, store.indexUpdatesAndSize( indexId, newDoubleLongRegister() ) );

        // and when
        store.replaceIndexUpdateAndSize( indexId, 444, 555 );

        // then
        assertRegister( 444, 555, store.indexUpdatesAndSize( indexId, newDoubleLongRegister() ) );
    }

    @Test
    public void shouldIncrementIndexUpdates()
    {
        // given
        long indexId = 4;
        store.replaceIndexUpdateAndSize( indexId, 123, 456 );

        // when
        store.incrementIndexUpdates( indexId, 5 );

        // then
        assertRegister( 123 + 5, 456, store.indexUpdatesAndSize( indexId, newDoubleLongRegister() ) );
    }

    @Test
    public void shouldStoreDataOnCheckpoint() throws IOException
    {
        // given
        long indexId1 = 1;
        long indexId2 = 2;
        store.replaceIndexUpdateAndSize( indexId1, 15, 20 );
        store.replaceIndexUpdateAndSize( indexId2, 25, 35 );
        store.replaceIndexSample( indexId1, 100, 200 );
        store.replaceIndexSample( indexId2, 200, 300 );

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
    public void shouldAllowMultipleThreadsIncrementIndexUpdates() throws Throwable
    {
        // given
        long indexId = 5;
        Race race = new Race();
        int contestants = 20;
        int delta = 3;
        store.replaceIndexUpdateAndSize( indexId, 0, 0 );
        race.addContestants( contestants, () -> store.incrementIndexUpdates( indexId, delta ), 1 );

        // when
        race.go();

        // then
        assertRegister( contestants * delta, 0, store.indexUpdatesAndSize( indexId, newDoubleLongRegister() ) );
    }

    @Test
    public void shouldHandleConcurrentUpdatesWithCheckpointing() throws Throwable
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
            store.replaceIndexUpdateAndSize( indexId, 0, 0 );
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

    private void assertRegister( long first, long second, DoubleLongRegister register )
    {
        assertEquals( first, register.readFirst() );
        assertEquals( second, register.readSecond() );
    }
}
