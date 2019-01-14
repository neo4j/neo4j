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

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.RelationshipProxy;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PathImplTest
{
    private final EmbeddedProxySPI spi = mock( EmbeddedProxySPI.class );

    @Test
    public void singularNodeWorksForwardsAndBackwards()
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
    public void pathsWithTheSameContentsShouldBeEqual()
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
    public void pathsWithDifferentLengthAreNotEqual()
    {
        Node node = createNode( 1337L );
        Relationship relationship = createRelationship( 1337L, 7331L );

        // Given
        Path firstPath = new PathImpl.Builder( node ).push( relationship ).build();
        Path secondPath = new PathImpl.Builder( node ).push( relationship ).push( createRelationship( 1337L, 7331L ) ).build();

        // When Then
        assertThat( firstPath, not( equalTo( secondPath ) ) );
        assertThat( secondPath, not( equalTo( firstPath ) ) );
    }

    @Test
    public void testPathReverseNodes()
    {
        when( spi.newNodeProxy( Mockito.anyLong() ) ).thenAnswer( new NodeProxyAnswer() );

        Path path = new PathImpl.Builder( createNodeProxy( 1 ) )
                                .push( createRelationshipProxy( 1, 2 ) )
                                .push( createRelationshipProxy( 2, 3 ) )
                                .build( new PathImpl.Builder( createNodeProxy( 3 ) ) );

        Iterable<Node> nodes = path.reverseNodes();
        List<Node> nodeList = Iterables.asList( nodes );

        Assert.assertEquals( 3, nodeList.size() );
        Assert.assertEquals( 3, nodeList.get( 0 ).getId() );
        Assert.assertEquals( 2, nodeList.get( 1 ).getId() );
        Assert.assertEquals( 1, nodeList.get( 2 ).getId() );
    }

    @Test
    public void testPathNodes()
    {
        when( spi.newNodeProxy( Mockito.anyLong() ) ).thenAnswer( new NodeProxyAnswer() );

        Path path = new PathImpl.Builder( createNodeProxy( 1 ) )
                .push( createRelationshipProxy( 1, 2 ) )
                .push( createRelationshipProxy( 2, 3 ) )
                .build( new PathImpl.Builder( createNodeProxy( 3 ) ) );

        Iterable<Node> nodes = path.nodes();
        List<Node> nodeList = Iterables.asList( nodes );

        Assert.assertEquals( 3, nodeList.size() );
        Assert.assertEquals( 1, nodeList.get( 0 ).getId() );
        Assert.assertEquals( 2, nodeList.get( 1 ).getId() );
        Assert.assertEquals( 3, nodeList.get( 2 ).getId() );
    }

    private RelationshipProxy createRelationshipProxy( int startNodeId, int endNodeId )
    {
        return new RelationshipProxy( spi, 1L, startNodeId, 1, endNodeId );
    }

    private NodeProxy createNodeProxy( int nodeId )
    {
        return new NodeProxy( spi, nodeId );
    }

    private Node createNode( long nodeId )
    {
        Node node = mock( Node.class );
        when( node.getId() ).thenReturn( nodeId );
        return node;
    }

    private Relationship createRelationship( long startNodeId, long endNodeId )
    {
        Relationship relationship = mock( Relationship.class );
        Node startNode = createNode( startNodeId );
        Node endNode = createNode( endNodeId );
        when( relationship.getStartNode() ).thenReturn( startNode );
        when( relationship.getEndNode() ).thenReturn( endNode );
        return relationship;
    }

    private class NodeProxyAnswer implements Answer<NodeProxy>
    {
        @Override
        public NodeProxy answer( InvocationOnMock invocation )
        {
            return createNodeProxy( ((Number) invocation.getArgument( 0 )).intValue() );
        }
    }
}
