/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.countStore;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Writes updates to the count store and ensures that snapshots are correct. Correctness is tested
 * by generating ALL intermediate map states and comparing the snapshot to the corresponding
 * expected state.
 */
public class InMemoryCountsStoreIntegrationTest
{
    @Test
    public void singleWriteTest()
    {
        //GIVEN
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        IntermediateStateTestManager intermediateStateTestManager = new IntermediateStateTestManager();
        Map<CountsKey,long[]> snapshotMap;

        //WHEN
        ConcurrentHashMap<CountsKey,long[]> updateMap = new ConcurrentHashMap<>();
        Map<CountsKey,long[]> map = new ConcurrentHashMap<>();
        long txId = intermediateStateTestManager.getNextUpdateMap( updateMap );
        updateMapByDiff( map, updateMap );
        countStore.updateAll( txId, updateMap );
        CountsSnapshot countsSnapshot = countStore.snapshot( txId );
        snapshotMap = countsSnapshot.getMap();

        //THEN
        assertMapEquals( map, snapshotMap );
    }

    @Test
    public void sequentialWorkload()
    {
        //GIVEN
        CountsSnapshot countsSnapshot;
        Map<CountsKey,long[]> snapshotMap;
        Map<CountsKey,long[]> map = new ConcurrentHashMap<>();
        ConcurrentHashMap<CountsKey,long[]> updateMap = new ConcurrentHashMap<>();
        IntermediateStateTestManager intermediateStateTestManager = new IntermediateStateTestManager();
        InMemoryCountsStore countStore = new InMemoryCountsStore();

        for ( int i = 0; i < 1000; i++ )
        {
            //WHEN
            long txId = intermediateStateTestManager.getNextUpdateMap( updateMap );
            updateMapByDiff( map, updateMap );
            countStore.updateAll( txId, updateMap );
            countsSnapshot = countStore.snapshot( txId );
            snapshotMap = countsSnapshot.getMap();

            //THEN
            assertMapEquals( map, snapshotMap );
        }
    }

    @Test
    public void concurrentWorkload() throws Exception
    {
        //GIVEN
        InMemoryCountsStore countStore = new InMemoryCountsStore();
        IntermediateStateTestManager intermediateStateTestManager = new IntermediateStateTestManager();
        ExecutorService executor = Executors.newFixedThreadPool( 10 );
        ExecutorCompletionService<Void> ecs = new ExecutorCompletionService<>( executor );

        List<Runnable> workers = new ArrayList<>( 10 );
        AtomicBoolean stop = new AtomicBoolean();
        for ( int i = 0; i < 9; i++ )
        {
            workers.add( new UpdateWorker( stop, intermediateStateTestManager, countStore ) );
        }
        workers.add( new SnapshotWorker( 10, stop, intermediateStateTestManager, countStore ) );

        //WHEN
        for ( Runnable worker : workers )
        {
            ecs.submit( worker, null );
        }

        // THEN
        for ( int i = 0; i < workers.size(); i++ )
        {
            ecs.take().get();
        }

        executor.shutdown();
    }

    private class UpdateWorker implements Runnable
    {
        private final AtomicBoolean stop;
        private final IntermediateStateTestManager manager;
        private final InMemoryCountsStore countStore;

        public UpdateWorker( AtomicBoolean stop, IntermediateStateTestManager manager, InMemoryCountsStore countStore )
        {
            this.stop = stop;
            this.manager = manager;
            this.countStore = countStore;
        }

        @Override
        public void run()
        {
            while ( !stop.get() )
            {
                Map<CountsKey,long[]> map = new HashMap<>();
                int txId = manager.getNextUpdateMap( map );
                //Lets mix up the order the updates are applied.
                if ( ThreadLocalRandom.current().nextInt( 0, 5 ) == 3 ) // 3/5 = 60% of the time. Just a guess.
                {
                    Thread.yield();
                }
                countStore.updateAll( txId, map );
            }
        }
    }

    private class SnapshotWorker implements Runnable
    {
        private AtomicBoolean stop;
        private final IntermediateStateTestManager intermediateStateTestManager;
        private final InMemoryCountsStore countStore;
        private final int repeatTimes;

        public SnapshotWorker( int repeatTimes, AtomicBoolean stop,
                IntermediateStateTestManager intermediateStateTestManager, InMemoryCountsStore countStore )
        {
            this.stop = stop;
            this.intermediateStateTestManager = intermediateStateTestManager;
            this.countStore = countStore;
            this.repeatTimes = repeatTimes;
        }

        @Override
        public void run()
        {
            for ( int i = 0; i < repeatTimes; i++ )
            {
                int id = intermediateStateTestManager.getId();
                long txId = id + ThreadLocalRandom.current().nextLong( 0, 5 );
                CountsSnapshot countsSnapshot = countStore.snapshot( txId );
                long snapshotTxId = countsSnapshot.getTxId();

                Map<CountsKey,long[]> snapshotMap = countsSnapshot.getMap();
                Map<CountsKey,long[]> expectedMap =
                        intermediateStateTestManager.getIntermediateMap( (int) snapshotTxId );

                //THEN
                assertThat( "Counts store snapshot was recorded with transaction ID less than the requested value.",
                        snapshotTxId, greaterThanOrEqualTo( txId ) );

                assertMapEquals( expectedMap, snapshotMap );
            }

            stop.set( true );
        }
    }

    private static void assertMapEquals( Map<CountsKey,long[]> expected, Map<CountsKey,long[]> actual )
    {
        try
        {
            assertEquals( expected.size(), actual.size() );
            actual.forEach( ( key, value ) -> {
                assertNotNull( "Example counts store snapshot has null where key was expected.", expected.get( key ) );
                assertArrayEquals( "Example counts store snapshot has different value for a key than expected.",
                        expected.get( key ), value );
            } );
        }
        catch ( Throwable t )
        {
            actual.forEach( ( key, value ) -> System.out.printf( "(%s) -> (%s)\n", key, Arrays.toString( value ) ) );
            System.out.println();
            expected.forEach( ( key, value ) -> System.out.printf( "(%s) -> (%s)\n", key, Arrays.toString( value ) ) );
            System.out.println();

            throw t;
        }
    }

    private static synchronized Map<CountsKey,long[]> updateMapByDiff( Map<CountsKey,long[]> map,
            Map<CountsKey,long[]> diff )
    {
        diff.entrySet().forEach( ( pair ) -> map.compute( pair.getKey(),
                ( k, v ) -> v == null ? pair.getValue() : updateEachValue( v, pair.getValue() ) ) );
        return map;
    }

    private static long[] updateEachValue( long[] v, long[] value )
    {
        for ( int i = 0; i < v.length; i++ )
        {
            v[i] = v[i] + value[i];
        }
        return v;
    }
}
