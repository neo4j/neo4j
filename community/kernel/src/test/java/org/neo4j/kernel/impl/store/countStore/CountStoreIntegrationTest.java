/*
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
package org.neo4j.kernel.impl.store.countStore;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;

/**
 * Writes updates to the count store and ensures that snapshots are correct. Correctness is tested
 * by generating ALL intermediate map states and comparing the snapshot to the corresponding
 * expected state.
 */
public class CountStoreIntegrationTest
{
    @Test
    public void singleWriteTest()
    {
        //GIVEN
        CountStore countStore = new CountStore();
        IntermediateStateTestManager intermediateStateTestManager = new IntermediateStateTestManager( 5 );
        Map<CountsKey,long[]> snapshotMap;

        //WHEN
        Map<CountsKey,long[]> updateMap = new HashMap<>();
        Map<CountsKey,long[]> map = new HashMap<>();
        long txId = intermediateStateTestManager.getNextUpdateMap( updateMap );
        DiffMapValuesTest.updateMapByDiff( map, updateMap, txId );
        countStore.updateAll( txId, updateMap );
        Snapshot snapshot = countStore.snapshot( txId );
        snapshotMap = snapshot.getMap();

        //THEN
        compareMaps( map, snapshotMap );
    }

    @Test
    public void sequentialWorkload()
    {
        //GIVEN
        int numberOfUpdates = 1000;
        Snapshot snapshot;
        Map<CountsKey,long[]> snapshotMap;
        Map<CountsKey,long[]> map = new HashMap<>();
        Map<CountsKey,long[]> updateMap = new HashMap<>();
        IntermediateStateTestManager intermediateStateTestManager = new IntermediateStateTestManager( numberOfUpdates );
        CountStore countStore = new CountStore();

        for ( int i = 0; i < numberOfUpdates; i++ )
        {
            //WHEN
            long txId = intermediateStateTestManager.getNextUpdateMap( updateMap );
            DiffMapValuesTest.updateMapByDiff( map, updateMap, txId );
            countStore.updateAll( txId, updateMap );
            snapshot = countStore.snapshot( txId );
            snapshotMap = snapshot.getMap();

            //THEN
            compareMaps( map, snapshotMap );
        }
    }

    @Test
    public void concurrentWorkload() throws InterruptedException
    {
        //GIVEN
        int numberOfUpdates = 900;
        CountStore countStore = new CountStore();
        IntermediateStateTestManager intermediateStateTestManager = new IntermediateStateTestManager( numberOfUpdates );
        ExecutorService workerExecutorService = Executors.newFixedThreadPool( 10 );

        //There should only ever be one snapshot worker.
        ExecutorService snapshotExecutorService = Executors.newSingleThreadExecutor();

        for ( int i = 0; i < numberOfUpdates; i++ )
        {
            Map<CountsKey,long[]> map = new ConcurrentHashMap<>();
            long txid = intermediateStateTestManager.getNextUpdateMap( map );
            Runnable workerA = new UpdateWorker( txid, map, countStore );

            //WHEN
            workerExecutorService.execute( workerA );
            if ( i > 1 && ThreadLocalRandom.current().nextInt( 50 ) == 3 )
            {
                //THEN
                snapshotExecutorService.execute( new SnapshotWorker( i, intermediateStateTestManager, countStore ) );
            }
        }

        workerExecutorService.shutdown();
        snapshotExecutorService.shutdown();
        workerExecutorService.awaitTermination( 100, TimeUnit.SECONDS );
        snapshotExecutorService.awaitTermination( 100, TimeUnit.SECONDS );


    }

    private class UpdateWorker implements Runnable
    {
        long txId;
        Map<CountsKey,long[]> map;
        private CountStore countStore;

        public UpdateWorker( long txId, Map<CountsKey,long[]> map, CountStore countStore )
        {
            this.txId = txId;
            this.map = map;
            this.countStore = countStore;
        }

        @Override
        public void run()
        {
            //Lets mix up the order the updates are applied.
            if ( ThreadLocalRandom.current().nextInt( 5 ) == 3 ) // 3/5 = 60% of the time. Just a guess.
            {
                Thread.yield();
            }
            countStore.updateAll( txId, map );
        }
    }

    private class SnapshotWorker implements Runnable
    {
        int txId;
        IntermediateStateTestManager intermediateStateTestManager;
        private CountStore countStore;

        public SnapshotWorker( int txId, IntermediateStateTestManager intermediateStateTestManager,
                CountStore countStore )
        {
            this.txId = txId;
            this.intermediateStateTestManager = intermediateStateTestManager;
            this.countStore = countStore;
        }

        @Override
        public void run()
        {
            Snapshot snapshot = countStore.snapshot( txId );

            Map<CountsKey,long[]> snapshotMap = snapshot.getMap();
            Map<CountsKey,long[]> expectedMap =
                    intermediateStateTestManager.getIntermediateMap( (int) snapshot.getTxId() );

            //THEN
            Assert.assertTrue( "Counts store snapshot was recorded with transaction ID less than the requested value.",
                    snapshot.getTxId() >= txId );
            Assert.assertEquals( "Counts store snapshot has an incorrect number of k/v pairs.", snapshotMap.size(),
                    expectedMap.size() );

            compareMaps( expectedMap, snapshotMap );
        }
    }

    private void compareMaps( Map<CountsKey,long[]> expected, Map<CountsKey,long[]> actual )
    {
        actual.forEach( ( key, value ) -> {
            Assert.assertTrue( "Example counts store snapshot has null where key was expected.",
                    expected.get( key ) != null );
            Assert.assertTrue( "Example counts store snapshot has different value for a key than expected.",
                    Arrays.equals( expected.get( key ), value ) );
        } );

        expected.forEach( ( key, value ) -> {
            Assert.assertTrue( "Counts store snapshot has null where key was expected.", actual.get( key ) != null );
            Assert.assertTrue( "Counts store snapshot has different value for a key than expected.",
                    Arrays.equals( actual.get( key ), value ) );
        } );
    }
}
