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
package org.neo4j.graphalgo.impl.util;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PathImplTest
{
    private final InternalTransaction transaction = mock( InternalTransaction.class, RETURNS_DEEP_STUBS );

    @Test
    void singularNodeWorksForwardsAndBackwards()
    {
        Node node = createNode( 1337L );
        Path path = PathImpl.singular( node );

        assertEquals( node, path.startNode() );
        assertEquals( node, path.endNode() );

        Iterator<Node> forwardIterator = path.nodes().iterator();

        assertTrue( forwardIterator.hasNext() );
        assertEquals( node, forwardIterator.next() );
        assertFalse( forwardIterator.hasNext() );

        Iterator<Node> reverseIterator = path.reverseNodes().iterator();

        assertTrue( reverseIterator.hasNext() );
        assertEquals( node, reverseIterator.next() );
        assertFalse( reverseIterator.hasNext() );
    }

    @Test
    void pathsWithTheSameContentsShouldBeEqual()
    {
        Node node = createNode( 1337L );
        Relationship relationship = createRelationship( 1337L, 7331L );

        // Given
        Path firstPath = new PathImpl.Builder( node ).push( relationship ).build();
        Path secondPath = new PathImpl.Builder( node ).push( relationship ).build();

        // When Then
        assertEquals( firstPath, secondPath );
        assertEquals( secondPath, firstPath );
    }

    @Test
    void pathsWithDifferentLengthAreNotEqual()
    {
        Node node = createNode( 1337L );
        Relationship relationship = createRelationship( 1337L, 7331L );

        // Given
        Path firstPath = new PathImpl.Builder( node ).push( relationship ).build();
        Path secondPath = new PathImpl.Builder( node ).push( relationship ).push( createRelationship( 1337L, 7331L ) ).build();

        // When Then
        assertThat( firstPath ).isNotEqualTo( secondPath );
        assertThat( secondPath ).isNotEqualTo( firstPath );
    }

    @Test
    void testPathReverseNodes()
    {
        when( transaction.newNodeEntity( Mockito.anyLong() ) ).thenAnswer( new NodeAnswer() );

        Path path = new PathImpl.Builder( createNodeEntity( 1 ) )
                                .push( createRelationshipEntity( 1, 2 ) )
                                .push( createRelationshipEntity( 2, 3 ) )
                                .build( new PathImpl.Builder( createNodeEntity( 3 ) ) );

        Iterable<Node> nodes = path.reverseNodes();
        List<Node> nodeList = Iterables.asList( nodes );

        assertEquals( 3, nodeList.size() );
        assertEquals( 3, nodeList.get( 0 ).getId() );
        assertEquals( 2, nodeList.get( 1 ).getId() );
        assertEquals( 1, nodeList.get( 2 ).getId() );
    }

    @Test
    void testPathNodes()
    {
        when( transaction.newNodeEntity( Mockito.anyLong() ) ).thenAnswer( new NodeAnswer() );

        Path path = new PathImpl.Builder( createNodeEntity( 1 ) )
                .push( createRelationshipEntity( 1, 2 ) )
                .push( createRelationshipEntity( 2, 3 ) )
                .build( new PathImpl.Builder( createNodeEntity( 3 ) ) );

        Iterable<Node> nodes = path.nodes();
        List<Node> nodeList = Iterables.asList( nodes );

        assertEquals( 3, nodeList.size() );
        assertEquals( 1, nodeList.get( 0 ).getId() );
        assertEquals( 2, nodeList.get( 1 ).getId() );
        assertEquals( 3, nodeList.get( 2 ).getId() );
    }

    private RelationshipEntity createRelationshipEntity( int startNodeId, int endNodeId )
    {
        return new RelationshipEntity( transaction, 1L, startNodeId, 1, endNodeId );
    }

    private NodeEntity createNodeEntity( int nodeId )
    {
        return new NodeEntity( transaction, nodeId );
    }

    private static Node createNode( long nodeId )
    {
        Node node = mock( Node.class );
        when( node.getId() ).thenReturn( nodeId );
        return node;
    }

    private static Relationship createRelationship( long startNodeId, long endNodeId )
    {
        Relationship relationship = mock( Relationship.class );
        Node startNode = createNode( startNodeId );
        Node endNode = createNode( endNodeId );
        when( relationship.getStartNode() ).thenReturn( startNode );
        when( relationship.getEndNode() ).thenReturn( endNode );
        return relationship;
    }

    private class NodeAnswer implements Answer<NodeEntity>
    {
        @Override
        public NodeEntity answer( InvocationOnMock invocation )
        {
            return createNodeEntity( ((Number) invocation.getArgument( 0 )).intValue() );
        }
    }
}
