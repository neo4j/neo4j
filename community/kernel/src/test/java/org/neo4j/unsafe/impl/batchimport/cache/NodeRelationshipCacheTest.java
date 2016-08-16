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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache.GroupVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache.NodeChangeVisitor;

import static java.lang.Math.max;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

@RunWith( Parameterized.class )
public class NodeRelationshipCacheTest
{
    @Rule
    public final RandomRule random = new RandomRule();
    @Parameterized.Parameter( 0 )
    public long base;
    private NodeRelationshipCache cache;

    @After
    public void after()
    {
        cache.close();
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        Collection<Object[]> data = new ArrayList<>();
        data.add( new Object[]{ 0L } );
        data.add( new Object[]{ (long) Integer.MAX_VALUE * 2 } );
        return data;
    }

    @Test
    public void shouldReportCorrectNumberOfDenseNodes() throws Exception
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.AUTO, 5, 100, base );
        increment( cache, 2, 10 );
        increment( cache, 5, 2 );
        increment( cache, 7, 12 );
        increment( cache, 23, 4 );
        increment( cache, 24, 5 );
        increment( cache, 25, 6 );
        cache.setHighNodeId( 25 );

        // THEN
        assertFalse( cache.isDense( 0 ) );
        assertTrue( cache.isDense( 2 ) );
        assertFalse( cache.isDense( 5 ) );
        assertTrue( cache.isDense( 7 ) );
        assertFalse( cache.isDense( 23 ) );
        assertTrue( cache.isDense( 24 ) );
        assertTrue( cache.isDense( 25 ) );
    }

    @Test
    public void shouldGoThroughThePhases() throws Exception
    {
        // GIVEN
        int nodeCount = 10;
        cache = new NodeRelationshipCache( NumberArrayFactory.OFF_HEAP, 20, 100, base );
        cache.setHighNodeId( nodeCount );
        incrementRandomCounts( cache, nodeCount, nodeCount*20 );

        // Test sparse node semantics
        {
            long node = findNode( cache, nodeCount, false );
            testNode( cache, node, null );
        }

        // Test dense node semantics
        {
            long node = findNode( cache, nodeCount, true );
            testNode( cache, node, Direction.OUTGOING );
            testNode( cache, node, Direction.INCOMING );
        }
    }

    @Test
    public void shouldObserveFirstRelationshipAsEmptyInEachDirection() throws Exception
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.AUTO, 1, 100, base );
        int nodes = 100;
        Direction[] directions = Direction.values();
        GroupVisitor groupVisitor = mock( GroupVisitor.class );
        cache.setForwardScan( true );
        cache.setHighNodeId( nodes );
        for ( int i = 0; i < nodes; i++ )
        {
            assertEquals( -1L, cache.getFirstRel( nodes, groupVisitor ) );
            cache.incrementCount( i );
            long previous = cache.getAndPutRelationship( i, directions[i % directions.length],
                    random.nextInt( 1_000_000 ), true );
            assertEquals( -1L, previous );
        }

        // WHEN
        cache.setForwardScan( false );
        for ( int i = 0; i < nodes; i++ )
        {
            long previous = cache.getAndPutRelationship( i, directions[i % directions.length],
                    random.nextInt( 1_000_000 ), false );
            assertEquals( -1L, previous );
        }

        // THEN
        cache.setForwardScan( true );
        for ( int i = 0; i < nodes; i++ )
        {
            assertEquals( -1L, cache.getFirstRel( nodes, groupVisitor ) );
        }
    }

    @Test
    public void shouldResetCountAfterGetOnDenseNodes() throws Exception
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.AUTO, 1, 100, base );
        long nodeId = 0;
        cache.setHighNodeId( 1 );
        cache.incrementCount( nodeId );
        cache.incrementCount( nodeId );
        cache.getAndPutRelationship( nodeId, OUTGOING, 10, true );
        cache.getAndPutRelationship( nodeId, OUTGOING, 12, true );
        assertTrue( cache.isDense( nodeId ) );

        // WHEN
        long count = cache.getCount( nodeId, OUTGOING );
        assertEquals( 2, count );

        // THEN
        assertEquals( 0, cache.getCount( nodeId, OUTGOING ) );
    }

    @Test
    public void shouldGetAndPutRelationshipAroundChunkEdge() throws Exception
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 10 );

        // WHEN
        long nodeId = 1_000_000 - 1;
        cache.setHighNodeId( nodeId );
        Direction direction = Direction.OUTGOING;
        long relId = 10;
        cache.getAndPutRelationship( nodeId, direction, relId, false );

        // THEN
        assertEquals( relId, cache.getFirstRel( nodeId, mock( GroupVisitor.class ) ) );
    }

    @Test
    public void shouldPutRandomStuff() throws Exception
    {
        // GIVEN
        int nodes = 10_000;
        PrimitiveLongObjectMap<long[]> key = Primitive.longObjectMap( nodes );
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 1, 1000, base );

        // mark random nodes as dense (dense node threshold is 1 so enough with one increment
        for ( long nodeId = 0; nodeId < nodes; nodeId++ )
        {
            if ( random.nextBoolean() )
            {
                cache.incrementCount( nodeId );
            }
        }
        cache.setHighNodeId( nodes );

        // WHEN
        for ( int i = 0; i < 100_000; i++ )
        {
            long nodeId = random.nextLong( nodes );
            boolean dense = cache.isDense( nodeId );
            Direction direction = random.among( Direction.values() );
            long relationshipId = random.nextLong( 1_000_000 );
            long previousHead = cache.getAndPutRelationship( nodeId, direction, relationshipId, false );
            long[] keyIds = key.get( nodeId );
            int keyIndex = dense ? direction.ordinal() : 0;
            if ( keyIds == null )
            {
                key.put( nodeId, keyIds = minusOneLongs( Direction.values().length ) );
            }
            assertEquals( keyIds[keyIndex], previousHead );
            keyIds[keyIndex] = relationshipId;
        }
    }

    @Test
    public void shouldPut6ByteRelationshipIds() throws Exception
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 1, 100, base );
        long sparseNode = 0;
        long denseNode = 1;
        long relationshipId = (1L << 48) - 2;
        cache.incrementCount( denseNode );
        cache.setHighNodeId( 1 );

        // WHEN
        assertEquals( -1L, cache.getAndPutRelationship( sparseNode, OUTGOING, relationshipId, false ) );
        assertEquals( -1L, cache.getAndPutRelationship( denseNode, OUTGOING, relationshipId, false ) );

        // THEN
        assertEquals( relationshipId, cache.getAndPutRelationship( sparseNode, OUTGOING, 1, false ) );
        assertEquals( relationshipId, cache.getAndPutRelationship( denseNode, OUTGOING, 1, false ) );
    }

    @Test
    public void shouldFailFastIfTooBigRelationshipId() throws Exception
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 1, 100, base );
        cache.setHighNodeId( 1 );

        // WHEN
        cache.getAndPutRelationship( 0, OUTGOING, (1L << 48) - 2, false );
        try
        {
            cache.getAndPutRelationship( 0, OUTGOING, (1L << 48) - 1, false );
            fail( "Should fail" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN Good
            assertTrue( e.getMessage().contains( "max" ) );
        }
    }

    @Test
    public void shouldVisitChangedNodes() throws Exception
    {
        // GIVEN
        int nodes = 10;
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 2, 100, base );
        for ( long nodeId = 0; nodeId < nodes; nodeId++ )
        {
            cache.incrementCount( nodeId );
            if ( random.nextBoolean() )
            {
                cache.incrementCount( nodeId );
            }
        }
        cache.setHighNodeId( nodes );
        PrimitiveLongSet keySparseChanged = Primitive.longSet( nodes );
        PrimitiveLongSet keyDenseChanged = Primitive.longSet( nodes );
        for ( int i = 0; i < nodes / 2; i++ )
        {
            long nodeId = random.nextLong( nodes );
            cache.getAndPutRelationship( nodeId, Direction.OUTGOING, random.nextLong( 1_000_000 ), false );
            boolean dense = cache.isDense( nodeId );
            (dense ? keyDenseChanged : keySparseChanged).add( nodeId );
        }

        {
            // WHEN (sparse)
            NodeChangeVisitor visitor = (nodeId, array) ->
            {
                // THEN (sparse)
                assertTrue( "Unexpected sparse change reported for " + nodeId, keySparseChanged.remove( nodeId ) );
            };
            cache.visitChangedNodes( visitor, false/*sparse*/ );
            assertTrue( "There was " + keySparseChanged.size() + " expected sparse changes that weren't reported",
                    keySparseChanged.isEmpty() );
        }

        {
            // WHEN (dense)
            NodeChangeVisitor visitor = (nodeId, array) ->
            {
                // THEN (dense)
                assertTrue( "Unexpected dense change reported for " + nodeId, keyDenseChanged.remove( nodeId ) );
            };
            cache.visitChangedNodes( visitor, true/*dense*/ );
            assertTrue( "There was " + keyDenseChanged.size() + " expected dense changes that weren reported",
                    keyDenseChanged.isEmpty() );
        }
    }

    @Test
    public void shouldFailFastOnTooHighCountOnNode() throws Exception
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 10, 100, base );
        long nodeId = 5;
        int count = NodeRelationshipCache.MAX_COUNT - 5;
        cache.setCount( nodeId, count );

        // WHEN
        for ( int i = 0; i < 10; i++ )
        {
            try
            {
                cache.incrementCount( i );
            }
            catch ( IllegalStateException e )
            {
                assertEquals( NodeRelationshipCache.MAX_COUNT + 1, i );
                break;
            }
        }
    }

    @Test
    public void shouldKeepNextGroupIdForNextRound() throws Exception
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 1, 100, base );
        long nodeId = 0;
        cache.incrementCount( nodeId );
        cache.setHighNodeId( nodeId+1 );
        GroupVisitor groupVisitor = mock( GroupVisitor.class );
        when( groupVisitor.visit( anyLong(), anyLong(), anyLong(), anyLong(), anyLong() ) ).thenReturn( 1L, 2L, 3L );

        long firstRelationshipGroupId;
        {
            // WHEN importing the first type
            long relationshipId = 10;
            cache.getAndPutRelationship( nodeId, OUTGOING, relationshipId, true );
            firstRelationshipGroupId = cache.getFirstRel( nodeId, groupVisitor );

            // THEN
            assertEquals( 1L, firstRelationshipGroupId );
            verify( groupVisitor ).visit( nodeId, -1L, relationshipId, -1L, -1L );

            // Also simulate going back again ("clearing" of the cache requires this)
            cache.setForwardScan( false );
            cache.getAndPutRelationship( nodeId, OUTGOING, relationshipId, false );
            cache.setForwardScan( true );
        }

        long secondRelationshipGroupId;
        {
            // WHEN importing the second type
            long relationshipId = 11;
            cache.getAndPutRelationship( nodeId, INCOMING, relationshipId, true );
            secondRelationshipGroupId = cache.getFirstRel( nodeId, groupVisitor );

            // THEN
            assertEquals( 2L, secondRelationshipGroupId );
            verify( groupVisitor ).visit( nodeId, firstRelationshipGroupId, -1, relationshipId, -1L );

            // Also simulate going back again ("clearing" of the cache requires this)
            cache.setForwardScan( false );
            cache.getAndPutRelationship( nodeId, OUTGOING, relationshipId, false );
            cache.setForwardScan( true );
        }

        {
            // WHEN importing the third type
            long relationshipId = 10;
            cache.getAndPutRelationship( nodeId, BOTH, relationshipId, true );
            long thirdRelationshipGroupId = cache.getFirstRel( nodeId, groupVisitor );
            assertEquals( 3L, thirdRelationshipGroupId );
            verify( groupVisitor ).visit( nodeId, secondRelationshipGroupId, -1L, -1L, relationshipId );
        }
    }

    private void testNode( NodeRelationshipCache link, long node, Direction direction )
    {
        int count = link.getCount( node, direction );
        assertEquals( -1, link.getAndPutRelationship( node, direction, 5, false ) );
        assertEquals( 5, link.getAndPutRelationship( node, direction, 10, false ) );
        assertEquals( count, link.getCount( node, direction ) );
    }

    private long findNode( NodeRelationshipCache link, long nodeCount, boolean isDense )
    {
        for ( long i = 0; i < nodeCount; i++ )
        {
            if ( link.isDense( i ) == isDense )
            {
                return i;
            }
        }
        throw new IllegalArgumentException( "No dense node found" );
    }

    private int incrementRandomCounts( NodeRelationshipCache link, int nodeCount, int i )
    {
        int highestSeenCount = 0;
        while ( i --> 0 )
        {
            long node = random.nextInt( nodeCount );
            highestSeenCount = max( highestSeenCount, link.incrementCount( node ) );
        }
        return highestSeenCount;
    }

    private void increment( NodeRelationshipCache cache, long node, int count )
    {
        for ( int i = 0; i < count; i++ )
        {
            cache.incrementCount( node );
        }
    }

    private long[] minusOneLongs( int length )
    {
        long[] array = new long[length];
        Arrays.fill( array, -1 );
        return array;
    }
}
