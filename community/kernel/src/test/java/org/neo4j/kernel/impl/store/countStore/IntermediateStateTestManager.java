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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;

public class IntermediateStateTestManager
{
    private volatile int id = 1;
    private List<ConcurrentHashMap<CountsKey,long[]>> intermediateStateMaps = new ArrayList<>();
    private Iterator<ConcurrentHashMap<CountsKey,long[]>> maps;

    static final int LARGEST_KEY = 5;

    public IntermediateStateTestManager()
    {
        intermediateStateMaps.add( new ConcurrentHashMap<>() );
        this.maps = new Iterator<ConcurrentHashMap<CountsKey,long[]>>()
        {
            @Override
            public boolean hasNext()
            {
                return true;
            }

            @Override
            public ConcurrentHashMap<CountsKey,long[]> next()
            {
                ConcurrentHashMap<CountsKey,long[]> map = allOnesMap();
                ConcurrentHashMap<CountsKey,long[]> nextMap =
                        copyOfMap( intermediateStateMaps.get( intermediateStateMaps.size() - 1 ) );
                applyDiffToMap( nextMap, map );
                intermediateStateMaps.add( nextMap );
                return map;
            }
        };
    }

    private static ConcurrentHashMap<CountsKey,long[]> copyOfMap( ConcurrentHashMap<CountsKey,long[]> nextMap )
    {
        ConcurrentHashMap<CountsKey,long[]> newMap = new ConcurrentHashMap<>();
        nextMap.forEach( ( key, value ) -> newMap.put( key, Arrays.copyOf( value, value.length ) ) );
        return newMap;
    }

    private static ConcurrentHashMap<CountsKey,long[]> applyDiffToMap( ConcurrentHashMap<CountsKey,long[]> map,
            ConcurrentHashMap<CountsKey,long[]> diff )
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
        return intermediateStateMaps.get( txId );
    }

    public synchronized int getNextUpdateMap( Map<CountsKey,long[]> map )
    {
        if ( maps.hasNext() )
        {
            map.clear();
            map.putAll( maps.next() );
            return id++;
        }
        else
        {
            return -1;
        }
    }

    public int getId()
    {
        return id;
    }

    private ConcurrentHashMap<CountsKey,long[]> allOnesMap()
    {
        ConcurrentHashMap<CountsKey,long[]> pairs = new ConcurrentHashMap<>();
        for ( int i = 0; i < LARGEST_KEY; i++ )
        {
            pairs.put( nodeKey( i ), new long[]{1L} );
        }
        return pairs;
    }
}
