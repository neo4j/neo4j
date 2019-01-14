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

import java.util.Iterator;

import org.junit.Test;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class PathProxyTest
{
    @Test
    public void shouldIterateThroughNodes()
    {
        // given
        Path path = new PathProxy( null, new long[] {1, 2, 3}, new long[] {100, 200}, new int[] {0, ~0} );

        Iterator<Node> iterator = path.nodes().iterator();
        Node node;

        // then
        assertTrue( iterator.hasNext() );
        assertThat( node = iterator.next(), instanceOf( Node.class ) );
        assertEquals( 1, node.getId() );
        assertTrue( iterator.hasNext() );
        assertThat( node = iterator.next(), instanceOf( Node.class ) );
        assertEquals( 2, node.getId() );
        assertTrue( iterator.hasNext() );
        assertThat( node = iterator.next(), instanceOf( Node.class ) );
        assertEquals( 3, node.getId() );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldIterateThroughNodesInReverse()
    {
        // given
        Path path = new PathProxy( null, new long[] {1, 2, 3}, new long[] {100, 200}, new int[] {0, ~0} );

        Iterator<Node> iterator = path.reverseNodes().iterator();
        Node node;

        // then
        assertTrue( iterator.hasNext() );
        assertThat( node = iterator.next(), instanceOf( Node.class ) );
        assertEquals( 3, node.getId() );
        assertTrue( iterator.hasNext() );
        assertThat( node = iterator.next(), instanceOf( Node.class ) );
        assertEquals( 2, node.getId() );
        assertTrue( iterator.hasNext() );
        assertThat( node = iterator.next(), instanceOf( Node.class ) );
        assertEquals( 1, node.getId() );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldIterateThroughRelationships()
    {
        // given
        Path path = new PathProxy( null, new long[] {1, 2, 3}, new long[] {100, 200}, new int[] {0, ~0} );

        Iterator<Relationship> iterator = path.relationships().iterator();
        Relationship relationship;

        // then
        assertTrue( iterator.hasNext() );
        assertThat( relationship = iterator.next(), instanceOf( Relationship.class ) );
        assertEquals( 100, relationship.getId() );
        assertEquals( 1, relationship.getStartNodeId() );
        assertEquals( 2, relationship.getEndNodeId() );
        assertTrue( iterator.hasNext() );
        assertThat( relationship = iterator.next(), instanceOf( Relationship.class ) );
        assertEquals( 200, relationship.getId() );
        assertEquals( 3, relationship.getStartNodeId() );
        assertEquals( 2, relationship.getEndNodeId() );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldIterateThroughRelationshipsInReverse()
    {
        // given
        Path path = new PathProxy( null, new long[] {1, 2, 3}, new long[] {100, 200}, new int[] {0, ~0} );

        Iterator<Relationship> iterator = path.reverseRelationships().iterator();
        Relationship relationship;

        // then
        assertTrue( iterator.hasNext() );
        assertThat( relationship = iterator.next(), instanceOf( Relationship.class ) );
        assertEquals( 200, relationship.getId() );
        assertEquals( 3, relationship.getStartNodeId() );
        assertEquals( 2, relationship.getEndNodeId() );
        assertTrue( iterator.hasNext() );
        assertThat( relationship = iterator.next(), instanceOf( Relationship.class ) );
        assertEquals( 100, relationship.getId() );
        assertEquals( 1, relationship.getStartNodeId() );
        assertEquals( 2, relationship.getEndNodeId() );
        assertFalse( iterator.hasNext() );
    }

    @Test
    public void shouldIterateAlternatingNodesAndRelationships()
    {
        // given
        Path path = new PathProxy( null, new long[] {1, 2, 3}, new long[] {100, 200}, new int[] {0, ~0} );

        Iterator<PropertyContainer> iterator = path.iterator();
        PropertyContainer entity;

        // then
        assertTrue( iterator.hasNext() );
        assertThat( entity = iterator.next(), instanceOf( Node.class ) );
        assertEquals( 1, ((Entity) entity).getId() );
        assertTrue( iterator.hasNext() );
        assertThat( entity = iterator.next(), instanceOf( Relationship.class ) );
        assertEquals( 100, ((Entity) entity).getId() );
        assertTrue( iterator.hasNext() );
        assertThat( entity = iterator.next(), instanceOf( Node.class ) );
        assertEquals( 2, ((Entity) entity).getId() );
        assertTrue( iterator.hasNext() );
        assertThat( entity = iterator.next(), instanceOf( Relationship.class ) );
        assertEquals( 200, ((Entity) entity).getId() );
        assertTrue( iterator.hasNext() );
        assertThat( entity = iterator.next(), instanceOf( Node.class ) );
        assertEquals( 3, ((Entity) entity).getId() );
        assertFalse( iterator.hasNext() );
    }
}
