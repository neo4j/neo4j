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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import org.neo4j.graphdb.Direction;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache.GroupVisitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.graphdb.Direction.OUTGOING;

@RunWith( Parameterized.class )
public class NodeRelationshipCacheTest
{
    @Parameterized.Parameter( 0 )
    public long base;
    private NodeRelationshipCache cache;

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
        incrementRandomCounts( cache, nodeCount, nodeCount*20 );

        // Test sparse node semantics
        {
            long node = findNode( cache, nodeCount, false );
            testNode( cache, node, -1, null );
        }

        // Test dense node semantics
        {
            long node = findNode( cache, nodeCount, true );
            testNode( cache, node, 4, Direction.OUTGOING );
            testNode( cache, node, 4, Direction.INCOMING );
            testNode( cache, node, 2, Direction.OUTGOING );
        }
    }

    @Test
    public void shouldAddGroupAfterTheFirst() throws Exception
    {
        // GIVEN a dense node
        long denseNode = 0;
        cache = new NodeRelationshipCache( NumberArrayFactory.AUTO, 1, 100, base );
        cache.incrementCount( denseNode );
        cache.getAndPutRelationship( denseNode, 0, Direction.OUTGOING, 0, true );

        // WHEN
        cache.getAndPutRelationship( denseNode, 1, Direction.INCOMING, 1, true );
        // just fill more data into the groups
        cache.getAndPutRelationship( denseNode, 0, Direction.INCOMING, 2, true );
        cache.getAndPutRelationship( denseNode, 1, Direction.OUTGOING, 3, true );

        // THEN
        GroupVisitor visitor = mock( GroupVisitor.class );
        assertEquals( 0L, cache.getFirstRel( denseNode, visitor ) );
        InOrder order = inOrder( visitor );
        order.verify( visitor ).visit( denseNode, 0, base + 1L, 0L, 2L, -1L );
        order.verify( visitor ).visit( denseNode, 1, -1L, 3L, 1L, -1L );
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldAddGroupBeforeTheFirst() throws Exception
    {
        // GIVEN a dense node
        long denseNode = 0;
        cache = new NodeRelationshipCache( NumberArrayFactory.AUTO, 1, 100, base );
        cache.incrementCount( denseNode );
        cache.getAndPutRelationship( denseNode, 1, Direction.INCOMING, 1, true );

        // WHEN
        cache.getAndPutRelationship( denseNode, 0, Direction.OUTGOING, 0, true );
        // just fill more data into the groups
        cache.getAndPutRelationship( denseNode, 0, Direction.INCOMING, 2, true );
        cache.getAndPutRelationship( denseNode, 1, Direction.OUTGOING, 3, true );

        // THEN
        GroupVisitor visitor = mock( GroupVisitor.class );
        assertEquals( 0L, cache.getFirstRel( denseNode, visitor ) );
        InOrder order = inOrder( visitor );
        order.verify( visitor ).visit( denseNode, 0, base + 1L, 0L, 2L, -1L );
        order.verify( visitor ).visit( denseNode, 1, -1L, 3L, 1L, -1L );
        order.verifyNoMoreInteractions();
    }

    @Test
    public void shouldAddGroupInTheMiddleIfTwo() throws Exception
    {
        // GIVEN a dense node
        long denseNode = 0;
        cache = new NodeRelationshipCache( NumberArrayFactory.AUTO, 1, 100, base );
        cache.incrementCount( denseNode );
        cache.getAndPutRelationship( denseNode, 0, Direction.OUTGOING, 0, true );
        cache.getAndPutRelationship( denseNode, 2, Direction.OUTGOING, 1, true );

        // WHEN
        cache.getAndPutRelationship( denseNode, 1, Direction.INCOMING, 2, true );
        // just fill more data into the groups
        cache.getAndPutRelationship( denseNode, 0, Direction.INCOMING, 3, true );
        cache.getAndPutRelationship( denseNode, 1, Direction.OUTGOING, 4, true );
        cache.getAndPutRelationship( denseNode, 2, Direction.INCOMING, 5, true );
        cache.getAndPutRelationship( denseNode, 1, Direction.BOTH, 6, true );

        // THEN
        GroupVisitor visitor = mock( GroupVisitor.class );
        assertEquals( 0L, cache.getFirstRel( denseNode, visitor ) );
        verify( visitor ).visit( denseNode, 0, base + 2L, 0L, 3L, -1L );
        verify( visitor ).visit( denseNode, 1, base + 1L, 4L, 2L, 6L );
        verify( visitor ).visit( denseNode, 2, -1L, 1L, 5L, -1L );
        verifyNoMoreInteractions( visitor );
    }

    @Test
    public void shouldClearRelationships() throws Exception
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.AUTO, 1, 100, base );
        int nodes = 100;
        Direction[] directions = Direction.values();
        GroupVisitor groupVisitor = mock( GroupVisitor.class );
        for ( int i = 0; i < nodes; i++ )
        {
            assertEquals( -1L, cache.getFirstRel( nodes, groupVisitor ) );
            cache.incrementCount( i );
            cache.getAndPutRelationship( i, i % 5, directions[i % directions.length],
                    random.nextInt( 1_000_000 ), true );
            assertEquals( 1, cache.getCount( i, i % 5, directions[i % directions.length] ) );
        }

        // WHEN
        cache.clearRelationships();

        // THEN
        for ( int i = 0; i < nodes; i++ )
        {
            assertEquals( -1L, cache.getFirstRel( nodes, groupVisitor ) );
            assertEquals( 1, cache.getCount( i, i % 5, directions[i % directions.length] ) );
        }
    }

    @Test
    public void shouldGetAndPutRelationshipAroundChunkEdge() throws Exception
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 10 );

        // WHEN
        long nodeId = 1_000_000 - 1;
        int type = 0;
        Direction direction = Direction.OUTGOING;
        long relId = 10;
        cache.getAndPutRelationship( nodeId, type, direction, relId, false );

        // THEN
        assertEquals( relId, cache.getFirstRel( nodeId, mock( GroupVisitor.class ) ) );
    }

    @Test
    public void shouldPutRandomStuff() throws Exception
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 10, 1000, base );

        // WHEN
        for ( int i = 0; i < 10_000; i++ )
        {
            cache.getAndPutRelationship( random.nextInt( 100_000 ), random.nextInt( 5 ),
                    Direction.OUTGOING, random.nextInt( 1_000_000 ), true );
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

        // WHEN
        assertEquals( -1L, cache.getAndPutRelationship( sparseNode, 0, OUTGOING, relationshipId, false ) );
        assertEquals( -1L, cache.getAndPutRelationship( denseNode, 0, OUTGOING, relationshipId, false ) );

        // THEN
        GroupVisitor groupVisitor = mock( GroupVisitor.class );
        assertEquals( relationshipId, cache.getAndPutRelationship( sparseNode, 0, OUTGOING, 1, false ) );
        assertEquals( relationshipId, cache.getAndPutRelationship( denseNode, 0, OUTGOING, 1, false ) );
    }

    @Test
    public void shouldFailFastIfTooBigRelationshipId() throws Exception
    {
        // GIVEN
        cache = new NodeRelationshipCache( NumberArrayFactory.HEAP, 1, 100, base );

        // WHEN
        cache.getAndPutRelationship( 0, 0, OUTGOING, (1L << 48) - 2, false );
        try
        {
            cache.getAndPutRelationship( 0, 0, OUTGOING, (1L << 48) - 1, false );
            fail( "Should fail" );
        }
        catch ( IllegalArgumentException e )
        {
            // THEN Good
            assertTrue( e.getMessage().contains( "max" ) );
        }
    }

    private void testNode( NodeRelationshipCache link, long node, int type, Direction direction )
    {
        int count = link.getCount( node, type, direction );
        assertEquals( -1, link.getAndPutRelationship( node, type, direction, 5, false ) );
        assertEquals( 5, link.getAndPutRelationship( node, type, direction, 10, false ) );
        assertEquals( count, link.getCount( node, type, direction ) );
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

    private Random random;

    @Before
    public void before()
    {
        long seed = currentTimeMillis();
        random = new Random( seed );
    }

    @After
    public void after()
    {
        cache.close();
    }

    private void increment( NodeRelationshipCache cache, long node, int count )
    {
        for ( int i = 0; i < count; i++ )
        {
            cache.incrementCount( node );
        }
    }
}
