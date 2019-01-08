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

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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
        assertThrows( IllegalArgumentException.class, () -> new PathValueBuilder( dbAccess() ).build() );
    }

    @Test
    void shouldHandleSingleNode()
    {
        // Given
        NodeValue node = node( 42 );
        PathValueBuilder builder = new PathValueBuilder( dbAccess( node ) );

        // When
        builder.addNode( node );

        // Then
        assertEquals( path( node ), builder.build() );
    }

    @Test
    void shouldHandleLongerPath()
    {
        // Given  (n1)<--(n2)-->(n3)
        NodeValue n1 = node( 42 );
        NodeValue n2 = node( 43 );
        NodeValue n3 = node( 44 );
        RelationshipValue r1 = relationship( 1337, n2, n1 );
        RelationshipValue r2 = relationship( 1338, n2, n3 );
        PathValueBuilder builder = new PathValueBuilder( dbAccess( n1, n2, n3, r1, r2 ) );

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
        NodeValue n1 = node( 42 );
        PathValueBuilder builder = new PathValueBuilder( dbAccess( n1 ) );

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
        NodeValue n1 = node( 42 );
        NodeValue n2 = node( 43 );
        NodeValue n3 = node( 44 );
        RelationshipValue r1 = relationship( 1337, n2, n1 );
        RelationshipValue r2 = relationship( 1338, n2, n3 );
        PathValueBuilder builder = new PathValueBuilder( dbAccess( n1, n2, n3, r1, r2 ) );

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
        NodeValue node = node( 42 );
        RelationshipValue relationship = relationship( 1337, node( 43 ), node );
        PathValueBuilder builder = new PathValueBuilder( dbAccess( node, relationship ) );

        // When (n1)<--(n2)--(n3)
        builder.addNode( node );
        builder.addIncoming( relationship );
        builder.addUndirected( NO_VALUE );

        // Then
        assertEquals( NO_VALUE, builder.build() );
    }

    @Test
    void shouldHandleNoValueInMultiRel()
    {
        // Given  (n1)<--(n2)-->(n3)
        NodeValue node1 = node( 42 );
        NodeValue node2 = node( 43 );
        NodeValue node3 = node( 44 );
        RelationshipValue relationship1 = relationship( 1337, node2, node1 );
        RelationshipValue relationship2 = relationship( 1338, node2, node3 );
        PathValueBuilder builder =
                new PathValueBuilder( dbAccess( node1, node2, node3, relationship1, relationship2 ) );

        // When (n1)<--(n2)--(n3)
        builder.addNode( node1 );
        builder.addMultipleUndirected( list( relationship1, relationship2, NO_VALUE ) );

        // Then
        assertEquals( NO_VALUE, builder.build() );
    }

    @Test
    void shouldHandleLongerPathWithMultiRelWhereEndNodeIsKnown()
    {
        // Given  (n1)<--(n2)-->(n3)
        NodeValue n1 = node( 42 );
        NodeValue n2 = node( 43 );
        NodeValue n3 = node( 44 );
        RelationshipValue r1 = relationship( 1337, n2, n1 );
        RelationshipValue r2 = relationship( 1338, n2, n3 );
        PathValueBuilder builder = new PathValueBuilder( dbAccess( n1, n2, n3, r1, r2 ) );

        // When (n1)<--(n2)--(n3)
        builder.addNode( n1 );
        builder.addMultipleUndirected( list( r1, r2 ), n3 );

        // Then
        assertEquals( path( n1, r1, n2, r2, n3 ), builder.build() );
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

    private DbAccess dbAccess( AnyValue... values )
    {
        DbAccess dbAccess = mock( DbAccess.class );
        for ( AnyValue value : values )
        {
            if ( value instanceof NodeValue )
            {
                NodeValue nodeValue = (NodeValue) value;
                when( dbAccess.nodeById( nodeValue.id() ) ).thenReturn( nodeValue );
            }
            else if ( value instanceof RelationshipValue )
            {
                RelationshipValue relationshipValue = (RelationshipValue) value;
                RelationshipScanCursor cursor = mock( RelationshipScanCursor.class );
                when( cursor.next() ).thenReturn( true );
                when( cursor.sourceNodeReference() ).thenReturn( relationshipValue.startNode().id() );
                when( cursor.targetNodeReference() ).thenReturn( relationshipValue.endNode().id() );
                when( dbAccess.singleRelationship( relationshipValue.id() ) ).thenReturn( cursor );
            }
            else
            {
                throw new AssertionError( "invalid input" );
            }
        }
        return dbAccess;
    }
}
