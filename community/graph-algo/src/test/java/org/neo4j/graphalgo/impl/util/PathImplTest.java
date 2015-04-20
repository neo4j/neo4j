/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.graphalgo.impl.util;

import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Traverser;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class PathImplTest
{

    @Test
    public void pathsWithTheSameContentsShouldBeEqual() throws Exception
    {
        // Given
        Path firstPath = new PathImpl.Builder( node( 1337l ) ).push( rel( 1337l, 7331l ) ).build();
        Path secondPath = new PathImpl.Builder( node( 1337l ) ).push( rel( 1337l, 7331l ) ).build();

        // When Then
        assertEquals( firstPath, secondPath );
        assertEquals( secondPath, firstPath );
    }

    @Test
    public void pathsWithDifferentLengthAreNotEqual() throws Exception
    {
        // Given
        Path firstPath = new PathImpl.Builder( node( 1337l ) ).push( rel( 1337l, 7331l ) ).build();
        Path secondPath = new PathImpl.Builder( node( 1337l ) ).push( rel( 1337l, 7331l ) ).push( rel( 7331l, 13l ) ).build();

        // When Then
        assertThat( firstPath, not( equalTo( secondPath ) ) );
        assertThat( secondPath, not( equalTo( firstPath ) ) );
    }

    private Node node( final long id )
    {
        return new MockNode( id );
    }

    private Relationship rel( final long fromId, final long toId )
    {
        return new MockRelationship( fromId, toId );
    }

    class MockPropertyContainer implements PropertyContainer
    {

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            return null;
        }

        @Override
        public boolean hasProperty( String key )
        {
            return false;
        }

        @Override
        public Object getProperty( String key )
        {
            return null;
        }

        @Override
        public Object getProperty( String key, Object defaultValue )
        {
            return null;
        }

        @Override
        public void setProperty( String key, Object value )
        {
        }

        @Override
        public Object removeProperty( String key )
        {
            return null;
        }

        @Override
        public Iterable<String> getPropertyKeys()
        {
            return null;
        }

    }

    @SuppressWarnings("deprecation")
    class MockNode extends MockPropertyContainer implements Node
    {

        private final long id;

        public MockNode( long id )
        {
            this.id = id;
        }

        @Override
        public long getId()
        {
            return id;
        }

        @Override
        public boolean equals( Object other )
        {
            return other instanceof Node && ((Node) other).getId() == id;
        }

        // Unimplemented

        @Override
        public void delete()
        {
        }

        @Override
        public Iterable<Relationship> getRelationships()
        {
            return null;
        }

        @Override
        public boolean hasRelationship()
        {
            return false;
        }

        @Override
        public Iterable<Relationship> getRelationships( RelationshipType... types )
        {
            return null;
        }

        @Override
        public Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types )
        {
            return null;
        }

        @Override
        public boolean hasRelationship( RelationshipType... types )
        {
            return false;
        }

        @Override
        public boolean hasRelationship( Direction direction, RelationshipType... types )
        {
            return false;
        }

        @Override
        public Iterable<Relationship> getRelationships( Direction dir )
        {
            return null;
        }

        @Override
        public boolean hasRelationship( Direction dir )
        {
            return false;
        }

        @Override
        public Iterable<Relationship> getRelationships( RelationshipType type, Direction dir )
        {
            return null;
        }

        @Override
        public boolean hasRelationship( RelationshipType type, Direction dir )
        {
            return false;
        }

        @Override
        public Relationship getSingleRelationship( RelationshipType type, Direction dir )
        {
            return null;
        }

        @Override
        public Relationship createRelationshipTo( Node otherNode, RelationshipType type )
        {
            return null;
        }

        @Override
        public Traverser traverse( Traverser.Order traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, RelationshipType relationshipType, Direction direction )
        {
            return null;
        }

        @Override
        public Traverser traverse( Traverser.Order traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, RelationshipType firstRelationshipType, Direction firstDirection, RelationshipType secondRelationshipType, Direction secondDirection )
        {
            return null;
        }

        @Override
        public Traverser traverse( Traverser.Order traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, Object... relationshipTypesAndDirections )
        {
            return null;
        }

        @Override
        public void addLabel( Label label )
        {
        }

        @Override
        public void removeLabel( Label label )
        {
        }

        @Override
        public boolean hasLabel( Label label )
        {
            return false;
        }

        @Override
        public ResourceIterable<Label> getLabels()
        {
            return null;
        }

        @Override
        public Iterable<RelationshipType> getRelationshipTypes()
        {
            return null;
        }

        @Override
        public int getDegree()
        {
            return 0;
        }

        @Override
        public int getDegree( RelationshipType type )
        {
            return 0;
        }

        @Override
        public int getDegree( Direction direction )
        {
            return 0;
        }

        @Override
        public int getDegree( RelationshipType type, Direction direction )
        {
            return 0;
        }
    }

    class MockRelationship extends MockPropertyContainer implements Relationship
    {

        private final long fromNode;
        private final long toNode;

        public MockRelationship( long fromNode, long toNode )
        {
            this.fromNode = fromNode;

            this.toNode = toNode;
        }

        @Override
        public long getId()
        {
            return fromNode ^ toNode;
        }

        @Override
        public boolean equals( Object other )
        {
            return other instanceof Relationship && ((Relationship) other).getId() == getId();
        }

        @Override
        public Node getOtherNode( Node node )
        {
            return node.getId() == fromNode ? new MockNode( toNode ) : new MockNode( fromNode );
        }

        @Override
        public Node getStartNode()
        {
            return new MockNode( fromNode );
        }

        @Override
        public RelationshipType getType()
        {
            return DynamicRelationshipType.withName( "Banana" );
        }

        @Override
        public void delete()
        {
        }

        @Override
        public Node getEndNode()
        {
            return null;
        }

        @Override
        public Node[] getNodes()
        {
            return new Node[0];
        }

        @Override
        public boolean isType( RelationshipType type )
        {
            return false;
        }
    }
}
