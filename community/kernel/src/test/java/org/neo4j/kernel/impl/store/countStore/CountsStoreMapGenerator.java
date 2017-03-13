/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;

/**
 * Generates a Map<CountsKey, long[]> of CountsKeys and values for testing.
 */
public class CountsStoreMapGenerator
{
    public static Map<CountsKey,long[]> simpleCountStoreMap( int num )
    {
        Map<CountsKey,long[]> map = new ConcurrentHashMap<>();
        addNodeKeys( num, map );
        addRelationshipKeys( num, map );
        addIndexSampleKeys( num, map );
        addIndexStatisticsKeys( num, map );
        return map;
    }

    private static void addNodeKeys( int num, Map<CountsKey,long[]> map )
    {
        for ( int i = 0; i < num; i++ )
        {
            map.put( CountsKeyFactory.nodeKey( i ), new long[]{i} );
        }
    }

    private static void addRelationshipKeys( int num, Map<CountsKey,long[]> map )
    {
        for ( int i = 0; i < num; i++ )
        {
            map.put( CountsKeyFactory.relationshipKey( i, i, i ), new long[]{i} );
        }
    }

    private static void addIndexSampleKeys( int num, Map<CountsKey,long[]> map )
    {
        for ( int i = 0; i < num; i++ )
        {
            map.put( CountsKeyFactory.indexSampleKey( i ), new long[]{i, i} );
        }
    }

    private static void addIndexStatisticsKeys( int num, Map<CountsKey,long[]> map )
    {
        for ( int i = 0; i < num; i++ )
        {
            map.put( CountsKeyFactory.indexStatisticsKey( i ), new long[]{i, i} );
        }
    }
}
