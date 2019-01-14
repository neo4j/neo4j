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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache.GroupVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache.NodeChangeVisitor;

import static java.lang.Math.max;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
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
    public void shouldReportCorrectNumberOfDenseNodes()
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.AUTO_WITHOUT_PAGECACHE, 5, 100, base );
        cache.setNodeCount( 26 );
        increment( cache, 2, 10 );
        increment( cache, 5, 2 );
        increment( cache, 7, 12 );
        increment( cache, 23, 4 );
        increment( cache, 24, 5 );
        increment( cache, 25, 6 );

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
    public void shouldGoThroughThePhases()
    {
        // GIVEN
        int nodeCount = 10;
        cache = new NodeRelationshipCache( NumberArrayFactory.OFF_HEAP, 20, 100, base );
        cache.setNodeCount( nodeCount );
        incrementRandomCounts( cache, nodeCount, nodeCount * 20 );

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
    public void shouldObserveFirstRelationshipAsEmptyInEachDirection()
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.AUTO_WITHOUT_PAGECACHE, 1, 100, base );
        int nodes = 100;
        int typeId = 5;
        Direction[] directions = Direction.values();
        GroupVisitor groupVisitor = mock( GroupVisitor.class );
        cache.setForwardScan( true, true );
        cache.setNodeCount( nodes + 1 );
        for ( int i = 0; i < nodes; i++ )
        {
            assertEquals( -1L, cache.getFirstRel( nodes, groupVisitor ) );
            cache.incrementCount( i );
            long previous = cache.getAndPutRelationship( i, typeId, directions[i % directions.length],
                    random.nextInt( 1_000_000 ), true );
            assertEquals( -1L, previous );
        }

        // WHEN
        cache.setForwardScan( false, true );
        for ( int i = 0; i < nodes; i++ )
        {
            long previous = cache.getAndPutRelationship( i, typeId, directions[i % directions.length],
                    random.nextInt( 1_000_000 ), false );
            assertEquals( -1L, previous );
        }

        // THEN
        cache.setForwardScan( true, true );
        for ( int i = 0; i < nodes; i++ )
        {
            assertEquals( -1L, cache.getFirstRel( nodes, groupVisitor ) );
        }
    }

    @Test
    public void shouldResetCountAfterGetOnDenseNodes()
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.AUTO_WITHOUT_PAGECACHE, 1, 100, base );
        long nodeId = 0;
        int typeId = 3;
        cache.setNodeCount( 1 );
        cache.incrementCount( nodeId );
        cache.incrementCount( nodeId );
        cache.getAndPutRelationship( nodeId, typeId, OUTGOING, 10, true );
        cache.getAndPutRelationship( nodeId, typeId, OUTGOING, 12, true );
        assertTrue( cache.isDense( nodeId ) );

        // WHEN
        long count = cache.getCount( nodeId, typeId, OUTGOING );
        assertEquals( 2, count );

        // THEN
        assertEquals( 0, cache.getCount( nodeId, typeId, OUTGOING ) );
    }

    @Test
    public void shouldGetAndPutRelationshipAroundChunkEdge()
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 10 );

        // WHEN
        long nodeId = 1_000_000 - 1;
        int typeId = 10;
        cache.setNodeCount( nodeId + 1 );
        Direction direction = Direction.OUTGOING;
        long relId = 10;
        cache.getAndPutRelationship( nodeId, typeId, direction, relId, false );

        // THEN
        assertEquals( relId, cache.getFirstRel( nodeId, mock( GroupVisitor.class ) ) );
    }

    @Test
    public void shouldPutRandomStuff()
    {
        // GIVEN
        int typeId = 10;
        int nodes = 10_000;
        PrimitiveLongObjectMap<long[]> key = Primitive.longObjectMap( nodes );
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 1, 1000, base );

        // mark random nodes as dense (dense node threshold is 1 so enough with one increment
        cache.setNodeCount( nodes );
        for ( long nodeId = 0; nodeId < nodes; nodeId++ )
        {
            if ( random.nextBoolean() )
            {
                cache.incrementCount( nodeId );
            }
        }

        // WHEN
        for ( int i = 0; i < 100_000; i++ )
        {
            long nodeId = random.nextLong( nodes );
            boolean dense = cache.isDense( nodeId );
            Direction direction = random.among( Direction.values() );
            long relationshipId = random.nextLong( 1_000_000 );
            long previousHead = cache.getAndPutRelationship( nodeId, typeId, direction, relationshipId, false );
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
    public void shouldPut6ByteRelationshipIds()
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 1, 100, base );
        long sparseNode = 0;
        long denseNode = 1;
        long relationshipId = (1L << 48) - 2;
        int typeId = 10;
        cache.setNodeCount( 2 );
        cache.incrementCount( denseNode );

        // WHEN
        assertEquals( -1L, cache.getAndPutRelationship( sparseNode, typeId, OUTGOING, relationshipId, false ) );
        assertEquals( -1L, cache.getAndPutRelationship( denseNode, typeId, OUTGOING, relationshipId, false ) );

        // THEN
        assertEquals( relationshipId, cache.getAndPutRelationship( sparseNode, typeId, OUTGOING, 1, false ) );
        assertEquals( relationshipId, cache.getAndPutRelationship( denseNode, typeId, OUTGOING, 1, false ) );
    }

    @Test
    public void shouldFailFastIfTooBigRelationshipId()
    {
        // GIVEN
        int typeId = 10;
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 1, 100, base );
        cache.setNodeCount( 1 );

        // WHEN
        cache.getAndPutRelationship( 0, typeId, OUTGOING, (1L << 48) - 2, false );
        try
        {
            cache.getAndPutRelationship( 0, typeId, OUTGOING, (1L << 48) - 1, false );
            fail( "Should fail" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN Good
            assertTrue( e.getMessage().contains( "max" ) );
        }
    }

    @Test
    public void shouldVisitChangedNodes()
    {
        // GIVEN
        int nodes = 10;
        int typeId = 10;
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 2, 100, base );
        cache.setNodeCount( nodes );
        for ( long nodeId = 0; nodeId < nodes; nodeId++ )
        {
            cache.incrementCount( nodeId );
            if ( random.nextBoolean() )
            {
                cache.incrementCount( nodeId );
            }
        }
        PrimitiveLongSet keySparseChanged = Primitive.longSet( nodes );
        PrimitiveLongSet keyDenseChanged = Primitive.longSet( nodes );
        for ( int i = 0; i < nodes / 2; i++ )
        {
            long nodeId = random.nextLong( nodes );
            cache.getAndPutRelationship( nodeId, typeId, Direction.OUTGOING, random.nextLong( 1_000_000 ), false );
            boolean dense = cache.isDense( nodeId );
            (dense ? keyDenseChanged : keySparseChanged).add( nodeId );
        }

        {
            // WHEN (sparse)
            NodeChangeVisitor visitor = ( nodeId, array ) ->
            {
                // THEN (sparse)
                assertTrue( "Unexpected sparse change reported for " + nodeId, keySparseChanged.remove( nodeId ) );
            };
            cache.visitChangedNodes( visitor, NodeType.NODE_TYPE_SPARSE );
            assertTrue( "There was " + keySparseChanged.size() + " expected sparse changes that weren't reported",
                    keySparseChanged.isEmpty() );
        }

        {
            // WHEN (dense)
            NodeChangeVisitor visitor = ( nodeId, array ) ->
            {
                // THEN (dense)
                assertTrue( "Unexpected dense change reported for " + nodeId, keyDenseChanged.remove( nodeId ) );
            };
            cache.visitChangedNodes( visitor, NodeType.NODE_TYPE_DENSE );
            assertTrue( "There was " + keyDenseChanged.size() + " expected dense changes that weren reported",
                    keyDenseChanged.isEmpty() );
        }
    }

    @Test
    public void shouldFailFastOnTooHighCountOnNode()
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 10, 100, base );
        long nodeId = 5;
        long count = NodeRelationshipCache.MAX_COUNT - 1;
        int typeId = 10;
        cache.setNodeCount( 10 );
        cache.setCount( nodeId, count, typeId, OUTGOING );

        // WHEN
        cache.incrementCount( nodeId );
        try
        {
            cache.incrementCount( nodeId );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            // THEN Good
        }
    }

    @Test
    public void shouldKeepNextGroupIdForNextRound()
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 1, 100, base );
        long nodeId = 0;
        int typeId = 10;
        cache.setNodeCount( nodeId + 1 );
        cache.incrementCount( nodeId );
        GroupVisitor groupVisitor = mock( GroupVisitor.class );
        when( groupVisitor.visit( anyLong(), anyInt(), anyLong(), anyLong(), anyLong() ) ).thenReturn( 1L, 2L, 3L );

        long firstRelationshipGroupId;
        {
            // WHEN importing the first type
            long relationshipId = 10;
            cache.getAndPutRelationship( nodeId, typeId, OUTGOING, relationshipId, true );
            firstRelationshipGroupId = cache.getFirstRel( nodeId, groupVisitor );

            // THEN
            assertEquals( 1L, firstRelationshipGroupId );
            verify( groupVisitor ).visit( nodeId, typeId, relationshipId, -1L, -1L );

            // Also simulate going back again ("clearing" of the cache requires this)
            cache.setForwardScan( false, true );
            cache.getAndPutRelationship( nodeId, typeId, OUTGOING, relationshipId, false );
            cache.setForwardScan( true, true );
        }

        long secondRelationshipGroupId;
        {
            // WHEN importing the second type
            long relationshipId = 11;
            cache.getAndPutRelationship( nodeId, typeId, INCOMING, relationshipId, true );
            secondRelationshipGroupId = cache.getFirstRel( nodeId, groupVisitor );

            // THEN
            assertEquals( 2L, secondRelationshipGroupId );
            verify( groupVisitor ).visit( nodeId, typeId, -1, relationshipId, -1L );

            // Also simulate going back again ("clearing" of the cache requires this)
            cache.setForwardScan( false, true );
            cache.getAndPutRelationship( nodeId, typeId, OUTGOING, relationshipId, false );
            cache.setForwardScan( true, true );
        }

        {
            // WHEN importing the third type
            long relationshipId = 10;
            cache.getAndPutRelationship( nodeId, typeId, BOTH, relationshipId, true );
            long thirdRelationshipGroupId = cache.getFirstRel( nodeId, groupVisitor );
            assertEquals( 3L, thirdRelationshipGroupId );
            verify( groupVisitor ).visit( nodeId, typeId, -1L, -1L, relationshipId );
        }
    }

    @Test
    public void shouldHaveDenseNodesWithBigCounts()
    {
        // A count of a dense node follow a different path during import, first there's counting per node
        // then import goes into actual import of relationships where individual chain degrees are
        // kept. So this test will first set a total count, then set count for a specific chain

        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 1, 100, base );
        long nodeId = 1;
        int typeId = 10;
        cache.setNodeCount( nodeId + 1 );
        cache.setCount( nodeId, 2, typeId, OUTGOING ); // surely dense now
        cache.getAndPutRelationship( nodeId, typeId, OUTGOING, 1, true );
        cache.getAndPutRelationship( nodeId, typeId, INCOMING, 2, true );

        // WHEN
        long highCountOut = NodeRelationshipCache.MAX_COUNT - 100;
        long highCountIn = NodeRelationshipCache.MAX_COUNT - 50;
        cache.setCount( nodeId, highCountOut, typeId, OUTGOING );
        cache.setCount( nodeId, highCountIn, typeId, INCOMING );
        cache.getAndPutRelationship( nodeId, typeId, OUTGOING, 1, true /*increment count*/ );
        cache.getAndPutRelationship( nodeId, typeId, INCOMING, 2, true /*increment count*/ );

        // THEN
        assertEquals( highCountOut + 1, cache.getCount( nodeId, typeId, OUTGOING ) );
        assertEquals( highCountIn + 1, cache.getCount( nodeId, typeId, INCOMING ) );
    }

    @Test
    public void shouldCacheMultipleDenseNodeRelationshipHeads()
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 1 );
        cache.setNodeCount( 10 );
        long nodeId = 3;
        cache.setCount( nodeId, 10, /*these do not matter ==>*/ 0, OUTGOING );

        // WHEN
        Map<Pair<Integer,Direction>,Long> firstRelationshipIds = new HashMap<>();
        int typeCount = 3;
        for ( int typeId = 0, relationshipId = 0; typeId < typeCount; typeId++ )
        {
            for ( Direction direction : Direction.values() )
            {
                long firstRelationshipId = relationshipId++;
                cache.getAndPutRelationship( nodeId, typeId, direction, firstRelationshipId, true );
                firstRelationshipIds.put( Pair.of( typeId, direction ), firstRelationshipId );
            }
        }
        AtomicInteger visitCount = new AtomicInteger();
        GroupVisitor visitor = ( nodeId1, typeId, out, in, loop ) ->
        {
            visitCount.incrementAndGet();
            assertEquals( firstRelationshipIds.get( Pair.of( typeId, OUTGOING ) ).longValue(), out );
            assertEquals( firstRelationshipIds.get( Pair.of( typeId, INCOMING ) ).longValue(), in );
            assertEquals( firstRelationshipIds.get( Pair.of( typeId, BOTH ) ).longValue(), loop );
            return 0;
        };
        cache.getFirstRel( nodeId, visitor );

        // THEN
        assertEquals( typeCount, visitCount.get() );
    }

    @Test
    public void shouldHaveSparseNodesWithBigCounts()
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 1, 100, base );
        long nodeId = 1;
        int typeId = 10;
        cache.setNodeCount( nodeId + 1 );

        // WHEN
        long highCount = NodeRelationshipCache.MAX_COUNT - 100;
        cache.setCount( nodeId, highCount, typeId, OUTGOING );
        long nextHighCount = cache.incrementCount( nodeId );

        // THEN
        assertEquals( highCount + 1, nextHighCount );
    }

    @Test
    public void shouldFailFastOnTooHighNodeCount()
    {
        // given
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 1 );

        try
        {
            // when
            cache.setNodeCount( 2L << (5 * Byte.SIZE) );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // then good
        }
    }

    private void testNode( NodeRelationshipCache link, long node, Direction direction )
    {
        int typeId = 0; // doesn't matter here because it's all sparse
        long count = link.getCount( node, typeId, direction );
        assertEquals( -1, link.getAndPutRelationship( node, typeId, direction, 5, false ) );
        assertEquals( 5, link.getAndPutRelationship( node, typeId, direction, 10, false ) );
        assertEquals( count, link.getCount( node, typeId, direction ) );
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

    private long incrementRandomCounts( NodeRelationshipCache link, int nodeCount, int i )
    {
        long highestSeenCount = 0;
        while ( i-- > 0 )
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
