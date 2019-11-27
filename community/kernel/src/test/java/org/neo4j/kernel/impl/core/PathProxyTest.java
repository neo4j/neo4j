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
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class PathProxyTest
{
    private final InternalTransaction transaction = mock( InternalTransaction.class );

    @Test
    void shouldIterateThroughNodes()
    {
        // given
        Path path = new PathProxy( transaction, new long[] {1, 2, 3}, new long[] {100, 200}, new int[] {0, ~0} );

        Iterator<Node> iterator = path.nodes().iterator();
        Node node;

        // then
        assertTrue( iterator.hasNext() );
        assertThat( node = iterator.next() ).isInstanceOf( Node.class );
        assertEquals( 1, node.getId() );
        assertTrue( iterator.hasNext() );
        assertThat( node = iterator.next() ).isInstanceOf( Node.class );
        assertEquals( 2, node.getId() );
        assertTrue( iterator.hasNext() );
        assertThat( node = iterator.next() ).isInstanceOf( Node.class );
        assertEquals( 3, node.getId() );
        assertFalse( iterator.hasNext() );
    }

    @Test
    void shouldIterateThroughNodesInReverse()
    {
        // given
        Path path = new PathProxy( transaction, new long[] {1, 2, 3}, new long[] {100, 200}, new int[] {0, ~0} );

        Iterator<Node> iterator = path.reverseNodes().iterator();
        Node node;

        // then
        assertTrue( iterator.hasNext() );
        assertThat( node = iterator.next() ).isInstanceOf( Node.class );
        assertEquals( 3, node.getId() );
        assertTrue( iterator.hasNext() );
        assertThat( node = iterator.next() ).isInstanceOf( Node.class );
        assertEquals( 2, node.getId() );
        assertTrue( iterator.hasNext() );
        assertThat( node = iterator.next() ).isInstanceOf( Node.class );
        assertEquals( 1, node.getId() );
        assertFalse( iterator.hasNext() );
    }

    @Test
    void shouldIterateThroughRelationships()
    {
        // given
        Path path = new PathProxy( transaction, new long[] {1, 2, 3}, new long[] {100, 200}, new int[] {0, ~0} );

        Iterator<Relationship> iterator = path.relationships().iterator();
        Relationship relationship;

        // then
        assertTrue( iterator.hasNext() );
        assertThat( relationship = iterator.next() ).isInstanceOf( Relationship.class );
        assertEquals( 100, relationship.getId() );
        assertEquals( 1, relationship.getStartNodeId() );
        assertEquals( 2, relationship.getEndNodeId() );
        assertTrue( iterator.hasNext() );
        assertThat( relationship = iterator.next() ).isInstanceOf( Relationship.class );
        assertEquals( 200, relationship.getId() );
        assertEquals( 3, relationship.getStartNodeId() );
        assertEquals( 2, relationship.getEndNodeId() );
        assertFalse( iterator.hasNext() );
    }

    @Test
    void shouldIterateThroughRelationshipsInReverse()
    {
        // given
        Path path = new PathProxy( transaction, new long[] {1, 2, 3}, new long[] {100, 200}, new int[] {0, ~0} );

        Iterator<Relationship> iterator = path.reverseRelationships().iterator();
        Relationship relationship;

        // then
        assertTrue( iterator.hasNext() );
        assertThat( relationship = iterator.next() ).isInstanceOf( Relationship.class );
        assertEquals( 200, relationship.getId() );
        assertEquals( 3, relationship.getStartNodeId() );
        assertEquals( 2, relationship.getEndNodeId() );
        assertTrue( iterator.hasNext() );
        assertThat( relationship = iterator.next() ).isInstanceOf( Relationship.class );
        assertEquals( 100, relationship.getId() );
        assertEquals( 1, relationship.getStartNodeId() );
        assertEquals( 2, relationship.getEndNodeId() );
        assertFalse( iterator.hasNext() );
    }

    @Test
    void shouldIterateAlternatingNodesAndRelationships()
    {
        // given
        Path path = new PathProxy( transaction, new long[] {1, 2, 3}, new long[] {100, 200}, new int[] {0, ~0} );

        Iterator<Entity> iterator = path.iterator();
        Entity entity;

        // then
        assertTrue( iterator.hasNext() );
        assertThat( entity = iterator.next() ).isInstanceOf( Node.class );
        assertEquals( 1, entity.getId() );
        assertTrue( iterator.hasNext() );
        assertThat( entity = iterator.next() ).isInstanceOf( Relationship.class );
        assertEquals( 100, entity.getId() );
        assertTrue( iterator.hasNext() );
        assertThat( entity = iterator.next() ).isInstanceOf( Node.class );
        assertEquals( 2, entity.getId() );
        assertTrue( iterator.hasNext() );
        assertThat( entity = iterator.next() ).isInstanceOf( Relationship.class );
        assertEquals( 200, entity.getId() );
        assertTrue( iterator.hasNext() );
        assertThat( entity = iterator.next() ).isInstanceOf( Node.class );
        assertEquals( 3, entity.getId() );
        assertFalse( iterator.hasNext() );
    }
}
