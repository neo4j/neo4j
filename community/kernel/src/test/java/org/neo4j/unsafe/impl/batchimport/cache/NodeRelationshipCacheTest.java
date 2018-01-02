/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;

@RunWith( Parameterized.class )
public class NodeRelationshipCacheTest
{
    @Parameterized.Parameter( 0 )
    public long base;

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
        NodeRelationshipCache cache = new NodeRelationshipCache( NumberArrayFactory.AUTO, 5, base );
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
        NodeRelationshipCache link = new NodeRelationshipCache( NumberArrayFactory.OFF_HEAP, 20, base );
        incrementRandomCounts( link, nodeCount, nodeCount*20 );

        // Test sparse node semantics
        {
            long node = findNode( link, nodeCount, false );
            testNode( link, node, -1, null );
        }

        // Test dense node semantics
        {
            long node = findNode( link, nodeCount, true );
            testNode( link, node, 4, Direction.OUTGOING );
            testNode( link, node, 4, Direction.INCOMING );
            testNode( link, node, 2, Direction.OUTGOING );
        }
    }

    @Test
    public void shouldAddGroupAfterTheFirst() throws Exception
    {
        // GIVEN a dense node
        long denseNode = 0;
        NodeRelationshipCache link = new NodeRelationshipCache( NumberArrayFactory.AUTO, 1, base );
        link.incrementCount( denseNode );
        link.getAndPutRelationship( denseNode, 0, Direction.OUTGOING, 0, true );

        // WHEN
        link.getAndPutRelationship( denseNode, 1, Direction.INCOMING, 1, true );
        // just fill more data into the groups
        link.getAndPutRelationship( denseNode, 0, Direction.INCOMING, 2, true );
        link.getAndPutRelationship( denseNode, 1, Direction.OUTGOING, 3, true );

        // THEN
        GroupVisitor visitor = mock( GroupVisitor.class );
        assertEquals( 0L, link.getFirstRel( denseNode, visitor ) );
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
        NodeRelationshipCache link = new NodeRelationshipCache( NumberArrayFactory.AUTO, 1, base );
        link.incrementCount( denseNode );
        link.getAndPutRelationship( denseNode, 1, Direction.INCOMING, 1, true );

        // WHEN
        link.getAndPutRelationship( denseNode, 0, Direction.OUTGOING, 0, true );
        // just fill more data into the groups
        link.getAndPutRelationship( denseNode, 0, Direction.INCOMING, 2, true );
        link.getAndPutRelationship( denseNode, 1, Direction.OUTGOING, 3, true );

        // THEN
        GroupVisitor visitor = mock( GroupVisitor.class );
        assertEquals( 0L, link.getFirstRel( denseNode, visitor ) );
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
        NodeRelationshipCache link = new NodeRelationshipCache( NumberArrayFactory.AUTO, 1, base );
        link.incrementCount( denseNode );
        link.getAndPutRelationship( denseNode, 0, Direction.OUTGOING, 0, true );
        link.getAndPutRelationship( denseNode, 2, Direction.OUTGOING, 1, true );

        // WHEN
        link.getAndPutRelationship( denseNode, 1, Direction.INCOMING, 2, true );
        // just fill more data into the groups
        link.getAndPutRelationship( denseNode, 0, Direction.INCOMING, 3, true );
        link.getAndPutRelationship( denseNode, 1, Direction.OUTGOING, 4, true );
        link.getAndPutRelationship( denseNode, 2, Direction.INCOMING, 5, true );
        link.getAndPutRelationship( denseNode, 1, Direction.BOTH, 6, true );

        // THEN
        GroupVisitor visitor = mock( GroupVisitor.class );
        assertEquals( 0L, link.getFirstRel( denseNode, visitor ) );
        verify( visitor ).visit( denseNode, 0, base + 2L, 0L, 3L, -1L );
        verify( visitor ).visit( denseNode, 1, base + 1L, 4L, 2L, 6L );
        verify( visitor ).visit( denseNode, 2, -1L, 1L, 5L, -1L );
        verifyNoMoreInteractions( visitor );
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

    private void increment( NodeRelationshipCache cache, long node, int count )
    {
        for ( int i = 0; i < count; i++ )
        {
            cache.incrementCount( node );
        }
    }
}
