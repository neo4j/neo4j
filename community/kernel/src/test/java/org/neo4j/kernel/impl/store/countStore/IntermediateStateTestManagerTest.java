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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;

/**
 * A test for the test class generating all intermediate states the count
 */
public class IntermediateStateTestManagerTest
{

    @Test
    public void futureMapsAreConsistentTest()
    {
        //GIVEN
        int numberOfUpdates = 1000;
        IntermediateStateTestManager intermediateStateTestManager = new IntermediateStateTestManager( numberOfUpdates );
        ConcurrentHashMap<CountsKey,long[]> testMap = new ConcurrentHashMap<>(); //Only for testing same thing.
        Map nextDiff = new ConcurrentHashMap<>();
        for ( int i = 1; i < numberOfUpdates; i++ )
        {
            //WHEN
            intermediateStateTestManager.getNextUpdateMap( nextDiff );
            IntermediateStateTestManager.applyDiffToMap( testMap, nextDiff );

            //THEN
            Assert.assertTrue(
                    countStoreMapsAreEqual( intermediateStateTestManager.getIntermediateMap( i ), testMap ) );
        }
    }

    public static boolean countStoreSnapshotsAreEqual( Snapshot snapshotA, Snapshot snapshotB )
    {
        if ( Long.compare( snapshotA.getTxId(), snapshotB.getTxId() ) != 0 )
        {
            return false;
        }
        return countStoreMapsAreEqual( snapshotA.getMap(), snapshotB.getMap() );
    }

    public static boolean countStoreMapsAreEqual( Map<CountsKey,long[]> mapA, Map<CountsKey,long[]> mapB )
    {
        if ( mapA.size() != mapB.size() )
        {
            return false;
        }

        for ( Map.Entry<CountsKey,long[]> pair : mapA.entrySet() )
        {
            long[] value = mapB.get( pair.getKey() );
            if ( value == null || !Arrays.equals( value, pair.getValue() ) )
            {
                return false;
            }
        }

        for ( Map.Entry<CountsKey,long[]> pair : mapB.entrySet() )
        {
            long[] value = mapA.get( pair.getKey() );
            if ( value == null || !Arrays.equals( value, pair.getValue() ) )
            {
                return false;
            }
        }
        return true;
    }
}
