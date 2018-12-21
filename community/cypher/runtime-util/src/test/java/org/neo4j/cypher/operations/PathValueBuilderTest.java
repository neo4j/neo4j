/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.operations;

import org.junit.jupiter.api.Test;

import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.storable.Values.EMPTY_TEXT_ARRAY;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.list;

class PathValueBuilderTest
{
    @Test
    void shouldComplainOnEmptyPath()
    {
        assertThrows( IllegalArgumentException.class, () -> new PathValueBuilder().build() );
    }

    @Test
    void shouldHandleSingleNode()
    {
        // Given
        PathValueBuilder builder = new PathValueBuilder();

        // When
        NodeValue node = node( 42 );
        builder.addNode( node );

        // Then
        assertEquals( path( node ), builder.build() );
    }

    @Test
    void shouldHandleLongerPath()
    {
        // Given  (n1)<--(n2)-->(n3)
        PathValueBuilder builder = new PathValueBuilder();
        NodeValue n1 = node( 42 );
        NodeValue n2 = node( 43 );
        NodeValue n3 = node( 44 );
        RelationshipValue r1 = relationship( 1337, n2, n1 );
        RelationshipValue r2 = relationship( 1338, n2, n3 );

        // When (n1)<--(n2)--(n3)
        builder.addNode( n1 );
        builder.addIncoming( r1 );
        builder.addUndirected( r2 );

        // Then
        assertEquals( path( n1, r1, n2, r2, n3 ), builder.build() );
    }

    @Test
    void shouldHandleEmptyMultiRel()
    {
        // Given  (n1)<--(n2)-->(n3)
        PathValueBuilder builder = new PathValueBuilder();
        NodeValue n1 = node( 42 );

        // When (n1)<--(n2)--(n3)
        builder.addNode( n1 );
        builder.addMultipleUndirected( EMPTY_LIST );

        // Then
        assertEquals( path( n1 ), builder.build() );
    }

    @Test
    void shouldHandleLongerPathWithMultiRel()
    {
        // Given  (n1)<--(n2)-->(n3)
        PathValueBuilder builder = new PathValueBuilder();
        NodeValue n1 = node( 42 );
        NodeValue n2 = node( 43 );
        NodeValue n3 = node( 44 );
        RelationshipValue r1 = relationship( 1337, n2, n1 );
        RelationshipValue r2 = relationship( 1338, n2, n3 );

        // When (n1)<--(n2)--(n3)
        builder.addNode( n1 );
        builder.addMultipleUndirected( list( r1, r2 ) );

        // Then
        assertEquals( path( n1, r1, n2, r2, n3 ), builder.build() );
    }

    @Test
    void shouldHandleNoValue()
    {
        // Given  (n1)<--(n2)-->(n3)
        PathValueBuilder builder = new PathValueBuilder();

        // When (n1)<--(n2)--(n3)
        builder.addNode( node( 42 ) );
        builder.addIncoming( relationship( 1337, node( 43 ), node( 42 ) ) );
        builder.addUndirected( NO_VALUE );

        // Then
        assertEquals( NO_VALUE, builder.build() );
    }

    @Test
    void shouldHandleNoValueInMultiRel()
    {
        // Given  (n1)<--(n2)-->(n3)
        PathValueBuilder builder = new PathValueBuilder();

        // When (n1)<--(n2)--(n3)
        builder.addNode( node( 42 ) );
        builder.addMultipleUndirected(
                list( relationship( 1337, node( 43 ), node( 42 ) ), relationship( 1338, node( 43 ), node( 44 ) ),
                        NO_VALUE ) );

        // Then
        assertEquals( NO_VALUE, builder.build() );
    }

    private NodeValue node( long id )
    {
        return VirtualValues.nodeValue( id, EMPTY_TEXT_ARRAY, EMPTY_MAP );
    }

    private RelationshipValue relationship( long id, NodeValue from, NodeValue to )
    {
        return VirtualValues.relationshipValue( id, from, to, stringValue( "R" ), EMPTY_MAP );
    }

    private PathValue path( AnyValue... nodeAndRel )
    {
        NodeValue[] nodes = new NodeValue[nodeAndRel.length / 2 + 1];
        RelationshipValue[] rels = new RelationshipValue[nodeAndRel.length / 2];
        for ( int i = 0; i < nodeAndRel.length; i++ )
        {
            if ( i % 2 == 0 )
            {
                nodes[i / 2] = (NodeValue) nodeAndRel[i];
            }
            else
            {
                rels[i / 2] = (RelationshipValue) nodeAndRel[i];
            }
        }
        return VirtualValues.path( nodes, rels );
    }
}
