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
package org.neo4j.unsafe.impl.batchimport;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.HEAP;

public class RelationshipGroupCacheTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldPutGroupsOnlyWithinPreparedRange() throws Exception
    {
        // GIVEN
        int nodeCount = 1000;
        RelationshipGroupCache cache = new RelationshipGroupCache( HEAP, ByteUnit.kibiBytes( 4 ), nodeCount );
        int[] counts = new int[nodeCount];
        for ( int nodeId = 0; nodeId < counts.length; nodeId++ )
        {
            counts[nodeId] = random.nextInt( 10 );
            setCount( cache, nodeId, counts[nodeId] );
        }

        long toNodeId = cache.prepare( 0 );
        assertTrue( toNodeId < nodeCount );

        // WHEN
        boolean thereAreMoreGroups = true;
        int cachedCount = 0;
        while ( thereAreMoreGroups )
        {
            thereAreMoreGroups = false;
            for ( int nodeId = 0; nodeId < nodeCount; nodeId++ )
            {
                if ( counts[nodeId] > 0 )
                {
                    thereAreMoreGroups = true;
                    int typeId = counts[nodeId]--;
                    if ( cache.put( new RelationshipGroupRecord( nodeId )
                            .initialize( true, typeId, -1, -1, -1, nodeId, -1 ) ) )
                    {
                        cachedCount++;
                    }
                }
            }
        }
        assertTrue( cachedCount >= toNodeId );

        // THEN the relationship groups we get back are only for those we prepared for
        int readCount = 0;
        for ( RelationshipGroupRecord cachedGroup : cache )
        {
            assertTrue( cachedGroup.getOwningNode() >= 0 && cachedGroup.getOwningNode() < toNodeId );
            readCount++;
        }
        assertEquals( cachedCount, readCount );
    }

    @Test
    public void shouldNotFindSpaceToPutMoreGroupsThanSpecifiedForANode() throws Exception
    {
        // GIVEN
        int nodeCount = 10;
        RelationshipGroupCache cache = new RelationshipGroupCache( HEAP, ByteUnit.kibiBytes( 4 ), nodeCount );
        setCount( cache, 1, 7 );
        assertEquals( nodeCount, cache.prepare( 0 ) );

        // WHEN
        for ( int i = 0; i < 7; i++ )
        {
            cache.put( new RelationshipGroupRecord( i+1 ).initialize( true, i, -1, -1, -1, 1, -1 ) );
        }
        try
        {
            cache.put( new RelationshipGroupRecord( 8 ).initialize( true, 8, -1, -1, -1, 1, -1 ) );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {   // Good
        }
    }

    @Test
    public void shouldSortOutOfOrderTypes() throws Exception
    {
        // GIVEN
        int nodeCount = 100;
        RelationshipGroupCache cache = new RelationshipGroupCache( HEAP, ByteUnit.kibiBytes( 40 ), nodeCount );
        int[] counts = new int[nodeCount];
        int groupCount = 0;
        for ( int nodeId = 0; nodeId < counts.length; nodeId++ )
        {
            counts[nodeId] = random.nextInt( 10 );
            setCount( cache, nodeId, counts[nodeId] );
            groupCount += counts[nodeId];
        }
        assertEquals( nodeCount, cache.prepare( 0 ) );
        boolean thereAreMoreGroups = true;
        int cachedCount = 0;
        int[] types = scrambledTypes( 10 );
        for ( int i = 0; thereAreMoreGroups; i++ )
        {
            int typeId = types[i];
            thereAreMoreGroups = false;
            for ( int nodeId = 0; nodeId < nodeCount; nodeId++ )
            {
                if ( counts[nodeId] > 0 )
                {
                    thereAreMoreGroups = true;
                    if ( cache.put( new RelationshipGroupRecord( nodeId )
                            .initialize( true, typeId, -1, -1, -1, nodeId, -1 ) ) )
                    {
                        cachedCount++;
                        counts[nodeId]--;
                    }
                }
            }
        }
        assertEquals( groupCount, cachedCount );

        // WHEN/THEN
        long currentNodeId = -1;
        int currentTypeId = -1;
        int readCount = 0;
        for( RelationshipGroupRecord group : cache )
        {
            assertTrue( group.getOwningNode() >= currentNodeId );
            if ( group.getOwningNode() > currentNodeId )
            {
                currentNodeId = group.getOwningNode();
                currentTypeId = -1;
            }
            assertTrue( group.getType() > currentTypeId );
            readCount++;
        }
        assertEquals( cachedCount, readCount );
    }

    private int[] scrambledTypes( int count )
    {
        int[] types = new int[count];
        for ( int i = 0; i < count; i++ )
        {
            types[i] = i + Short.MAX_VALUE ;
        }

        for ( int i = 0; i < 10; i++ )
        {
            swap( types, i, random.nextInt( count ) );
        }
        return types;
    }

    private void swap( int[] types, int a, int b )
    {
        int temp = types[a];
        types[a] = types[b];
        types[b] = temp;
    }

    private void setCount( RelationshipGroupCache cache, int nodeId, int count )
    {
        for ( int i = 0; i < count; i++ )
        {
            cache.incrementGroupCount( nodeId );
        }
    }
}
