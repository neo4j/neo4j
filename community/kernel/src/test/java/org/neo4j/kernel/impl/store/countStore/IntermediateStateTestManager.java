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

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;

public class IntermediateStateTestManager
{
    private int numberOfUpdates;
    private int id = 1;
    private ConcurrentHashMap[] maps;
    private ConcurrentHashMap[] intermediateStateMaps;
    int largestKey = 5;

    public IntermediateStateTestManager( int numberOfUpdates )
    {
        this.numberOfUpdates = numberOfUpdates + 1;
        this.maps = new ConcurrentHashMap[this.numberOfUpdates];
        IntStream.range( 0, this.numberOfUpdates ).forEach( ( i ) -> this.maps[i] = allOnesMap() );
        this.intermediateStateMaps = computeAllUpdates();
    }

    private ConcurrentHashMap[] computeAllUpdates()
    {
        ConcurrentHashMap[] intermediateStateMaps = new ConcurrentHashMap[this.numberOfUpdates];
        ConcurrentHashMap<CountsKey,long[]> nextMap = new ConcurrentHashMap<>();

        for ( int i = 1; i < this.maps.length; i++ )
        {
            applyDiffToMap( nextMap, maps[i] );
            ConcurrentHashMap<CountsKey,long[]> newMap = copyOfMap( nextMap );
            intermediateStateMaps[i] = newMap;
        }
        return intermediateStateMaps;
    }

    private ConcurrentHashMap<CountsKey,long[]> copyOfMap( ConcurrentHashMap<CountsKey,long[]> nextMap )
    {
        ConcurrentHashMap<CountsKey,long[]> newMap = new ConcurrentHashMap<>();
        nextMap.forEach( ( key, value ) -> newMap.put( key, Arrays.copyOf( value, value.length ) ) );
        return newMap;
    }

    public synchronized static ConcurrentHashMap<CountsKey,long[]> applyDiffToMap(
            ConcurrentHashMap<CountsKey,long[]> map, ConcurrentHashMap<CountsKey,long[]> diff )
    {
        diff.forEach( ( key, value ) -> map.compute( key,
                ( k, v ) -> v == null ? Arrays.copyOf( value, value.length ) : updateEachValue( v, value ) ) );
        return map;
    }

    private static long[] updateEachValue( long[] v, long[] diff )
    {
        for ( int i = 0; i < v.length; i++ )
        {
            v[i] = v[i] + diff[i];
        }
        return v;
    }

    public synchronized ConcurrentHashMap<CountsKey,long[]> getIntermediateMap( int txId )
    {
        return intermediateStateMaps[txId];
    }

    public synchronized int getNextUpdateMap( ConcurrentHashMap<CountsKey,long[]> map )
    {
        if ( id < numberOfUpdates )
        {
            map.clear();
            map.putAll( maps[id] );
            return id++;
        }
        else
        {
            return -1;
        }
    }

    private ConcurrentHashMap<CountsKey,long[]> allZerosMap()
    {
        ConcurrentHashMap<CountsKey,long[]> pairs = new ConcurrentHashMap<>();
        for ( int i = 0; i < largestKey; i++ )
        {
            pairs.put( nodeKey( i ), new long[]{0L} );
        }
        return pairs;
    }

    private ConcurrentHashMap<CountsKey,long[]> allOnesMap()
    {
        ConcurrentHashMap<CountsKey,long[]> pairs = new ConcurrentHashMap<>();
        for ( int i = 0; i < largestKey; i++ )
        {
            pairs.put( nodeKey( i ), new long[]{1L} );
        }
        return pairs;
    }

    private ConcurrentHashMap<CountsKey,long[]> randomPositiveMap()
    {
        int largestDiff = 100;
        ConcurrentHashMap<CountsKey,long[]> pairs = new ConcurrentHashMap<>();
        for ( int i = 0; i < largestKey; i++ )
        {
            pairs.put( nodeKey( ThreadLocalRandom.current().nextInt( 0, largestKey ) ),
                    new long[]{ThreadLocalRandom.current().nextLong( 1, largestDiff )} );
        }
        return pairs;
    }

    private ConcurrentHashMap<CountsKey,long[]> randomMap()
    {
        int largestDiff = 100;
        ConcurrentHashMap<CountsKey,long[]> pairs = new ConcurrentHashMap<>();
        for ( int i = 0; i < largestKey; i++ )
        {
            pairs.put( nodeKey( ThreadLocalRandom.current().nextInt( 0, largestKey ) ),
                    new long[]{ThreadLocalRandom.current().nextLong( -1 * largestDiff, largestDiff )} );
        }
        return pairs;

    }
}