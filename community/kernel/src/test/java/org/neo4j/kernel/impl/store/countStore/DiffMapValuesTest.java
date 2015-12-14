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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;

/**
 * A test for the test class generating all intermediate states the count
 */
public class DiffMapValuesTest
{
    public synchronized static Map<CountsKey,long[]> updateMapByDiff( Map<CountsKey,long[]> map,
            Map<CountsKey,long[]> diff, long txId )
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


    @Test
    public void diffTest()
    {
        //GIVEN
        Map<CountsKey,long[]> map = new HashMap<>();
        map.put( nodeKey( 1 ), new long[]{1L} );
        map.put( nodeKey( 2 ), new long[]{1L} );
        map.put( nodeKey( 3 ), new long[]{1L} );
        map.put( nodeKey( 4 ), new long[]{1L} );

        Map<CountsKey,long[]> updateByMap = new HashMap<>();
        updateByMap.put( nodeKey( 1 ), new long[]{4L} );
        updateByMap.put( nodeKey( 2 ), new long[]{8L} );


        Map<CountsKey,long[]> updateByOtherMap = new HashMap<>();
        updateByOtherMap.put( nodeKey( 3 ), new long[]{100L} );
        updateByOtherMap.put( nodeKey( 4 ), new long[]{96L} );

        //WHEN

        map = updateMapByDiff( map, updateByMap, 0 );
        map = updateMapByDiff( map, updateByOtherMap, 0 );

        //THEN

        /**
         * Works because the JVM uses the same object for Long objects if they're < ~100
         */
        Assert.assertSame( 5L, map.get( nodeKey( 1 ) )[0] );
        Assert.assertSame( 9L, map.get( nodeKey( 2 ) )[0] );
        Assert.assertSame( 101L, map.get( nodeKey( 3 ) )[0] );
        Assert.assertSame( 97L, map.get( nodeKey( 4 ) )[0] );
    }
}