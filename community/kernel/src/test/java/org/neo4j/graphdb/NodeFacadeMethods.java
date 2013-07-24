/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.graphdb;

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.graphdb.ReturnableEvaluator.ALL;
import static org.neo4j.graphdb.StopEvaluator.DEPTH_ONE;
import static org.neo4j.graphdb.Traverser.Order.BREADTH_FIRST;

public class NodeFacadeMethods
{
    private static final DynamicRelationshipType FOO = withName( "foo" );
    private static final DynamicRelationshipType BAR = withName( "bar" );
    private static final DynamicRelationshipType BAZ = withName( "baz" );
    private static final Label QUUX = label( "quux" );

    private static final NodeFacadeMethod HAS_PROPERTY = new NodeFacadeMethod( "boolean hasProperty( " +
            "String key )" )
    {
        @Override
        public void call( Node node )
        {
            node.hasProperty( "foo" );
        }
    };

    private static final NodeFacadeMethod GET_PROPERTY = new NodeFacadeMethod( "Object getProperty( String key )" )
    {
        @Override
        public void call( Node node )
        {
            node.getProperty( "foo" );
        }
    };

    private static final NodeFacadeMethod GET_PROPERTY_WITH_DEFAULT = new NodeFacadeMethod( "Object getProperty( " +
            "String key, Object defaultValue )" )
    {
        @Override
        public void call( Node node )
        {
            node.getProperty( "foo", 42 );
        }
    };

    private static final NodeFacadeMethod SET_PROPERTY = new NodeFacadeMethod( "void setProperty( String key, " +
            "Object value )" )

    {
        @Override
        public void call( Node node )
        {
            node.setProperty( "foo", 42 );
        }
    };

    private static final NodeFacadeMethod REMOVE_PROPERTY = new NodeFacadeMethod( "Object removeProperty( String key " +
            ")" )

    {
        @Override
        public void call( Node node )
        {
            node.removeProperty( "foo" );
        }
    };

    private static final NodeFacadeMethod GET_PROPERTY_KEYS = new NodeFacadeMethod( "Iterable<String> getPropertyKeys" +
            "()" )
    {
        @Override
        public void call( Node node )
        {
            for ( String key : node.getPropertyKeys() )
            {

            }
        }
    };

    private static final NodeFacadeMethod GET_PROPERTY_VALUES = new NodeFacadeMethod( "Iterable<Object> " +
            "getPropertyValues()" )
    {
        @Override
        public void call( Node node )
        {
            for ( Object value : node.getPropertyValues() )
            {

            }

        }
    };

    private static final NodeFacadeMethod DELETE = new NodeFacadeMethod( "void delete()" )
    {
        @Override
        public void call( Node node )
        {
            node.delete();
        }
    };

    private static final NodeFacadeMethod GET_RELATIONSHIPS = new NodeFacadeMethod( "Iterable<Relationship> " +
            "getRelationships()" )
    {
        @Override
        public void call( Node node )
        {
            for ( Relationship relationship : node.getRelationships() )
            {

            }
        }
    };

    private static final NodeFacadeMethod HAS_RELATIONSHIP = new NodeFacadeMethod( "boolean hasRelationship()" )

    {
        @Override
        public void call( Node node )
        {
            node.hasRelationship();
        }
    };

    private static final NodeFacadeMethod GET_RELATIONSHIPS_BY_TYPE = new NodeFacadeMethod(
            "Iterable<Relationship> getRelationships( RelationshipType... types )" )
    {
        @Override
        public void call( Node node )
        {
            for ( Relationship relationship : node.getRelationships( FOO, BAR ) )
            {

            }
        }
    };

    private static final NodeFacadeMethod GET_RELATIONSHIPS_BY_DIRECTION_AND_TYPES = new NodeFacadeMethod(
            "Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types )" )
    {
        @Override
        public void call( Node node )
        {
            for ( Relationship relationship : node.getRelationships( BOTH, FOO, BAR ) )
            {

            }
        }
    };

    private static final NodeFacadeMethod HAS_RELATIONSHIP_BY_TYPE = new NodeFacadeMethod( "boolean " +
            "hasRelationship( RelationshipType... types )" )

    {
        @Override
        public void call( Node node )
        {
            node.hasRelationship( FOO );
        }
    };

    private static final NodeFacadeMethod HAS_RELATIONSHIP_BY_DIRECTION_AND_TYPE = new NodeFacadeMethod( "boolean " +
            "hasRelationship( Direction direction, RelationshipType... types )" )
    {
        @Override
        public void call( Node node )
        {
            node.hasRelationship( BOTH, FOO );
        }
    };

    private static final NodeFacadeMethod GET_RELATIONSHIPS_BY_DIRECTION = new NodeFacadeMethod(
            "Iterable<Relationship> getRelationships( Direction dir )" )
    {
        @Override
        public void call( Node node )
        {
            for ( Relationship relationship : node.getRelationships( BOTH ) )
            {

            }
        }
    };

    private static final NodeFacadeMethod HAS_RELATIONSHIP_BY_DIRECTION = new NodeFacadeMethod( "boolean " +
            "hasRelationship( Direction dir )" )

    {
        @Override
        public void call( Node node )
        {
            node.hasRelationship( BOTH );
        }
    };

    private static final NodeFacadeMethod GET_RELATIONSHIPS_BY_TYPE_AND_DIRECTION = new NodeFacadeMethod(
            "Iterable<Relationship> getRelationships( RelationshipType type, Direction dir );" )
    {
        @Override
        public void call( Node node )
        {
            for ( Relationship relationship : node.getRelationships( FOO, BOTH ) )
            {

            }
        }
    };

    private static final NodeFacadeMethod HAS_RELATIONSHIP_BY_TYPE_AND_DIRECTION = new NodeFacadeMethod( "boolean " +
            "hasRelationship( RelationshipType type, Direction dir )" )
    {
        @Override
        public void call( Node node )
        {
            node.hasRelationship( FOO, BOTH );
        }
    };

    private static final NodeFacadeMethod GET_SINGLE_RELATIONSHIP = new NodeFacadeMethod( "Relationship " +
            "getSingleRelationship( RelationshipType type, Direction dir )" )
    {
        @Override
        public void call( Node node )
        {
            node.getSingleRelationship( FOO, BOTH );
        }
    };

    private static final NodeFacadeMethod CREATE_RELATIONSHIP_TO = new NodeFacadeMethod( "Relationship " +
            "createRelationshipTo( Node otherNode, RelationshipType type )" )
    {
        @Override
        public void call( Node node )
        {
            node.createRelationshipTo( null, FOO );
        }
    };

    private static final NodeFacadeMethod TRAVERSE_USING_ONE_TYPE_AND_DIRECTION = new NodeFacadeMethod( "Traverser " +
            "traverse( Traverser.Order " +
            "traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, " +
            "RelationshipType relationshipType, Direction direction )" )

    {
        @Override
        public void call( Node node )
        {
            node.traverse( BREADTH_FIRST, DEPTH_ONE, ALL,
                    FOO, BOTH );
        }
    };

    private static final NodeFacadeMethod TRAVERSE_USING_TWO_TYPES_AND_DIRECTIONS = new NodeFacadeMethod( "Traverser " +
            "traverse( Traverser.Order " +
            "traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, " +
            "RelationshipType firstRelationshipType, Direction firstDirection, " +
            "RelationshipType secondRelationshipType, Direction secondDirection )" )
    {
        @Override
        public void call( Node node )
        {
            node.traverse( BREADTH_FIRST, DEPTH_ONE, ALL, FOO, BOTH, BAR, OUTGOING );
        }
    };

    private static final NodeFacadeMethod TRAVERSE_USING_ANY_NUMBER_OF_TYPES_AND_DIRECTIONS = new NodeFacadeMethod(
            "Traverser traverse( Traverser.Order " +
                    "traversalOrder, StopEvaluator stopEvaluator, ReturnableEvaluator returnableEvaluator, " +
                    "Object... relationshipTypesAndDirections )" )
    {
        @Override
        public void call( Node node )
        {
            node.traverse( BREADTH_FIRST, DEPTH_ONE, ALL, FOO, BOTH, BAR, OUTGOING, BAZ, INCOMING );
        }
    };

    private static final NodeFacadeMethod ADD_LABEL = new NodeFacadeMethod( "void addLabel( Label label )" )
    {
        @Override
        public void call( Node node )
        {
            node.addLabel( QUUX );
        }
    };

    private static final NodeFacadeMethod REMOVE_LABEL = new NodeFacadeMethod( "void removeLabel( Label label )" )
    {
        @Override
        public void call( Node node )
        {
            node.removeLabel( QUUX );
        }
    };

    private static final NodeFacadeMethod HAS_LABEL = new NodeFacadeMethod( "boolean hasLabel( Label label )" )
    {
        @Override
        public void call( Node node )
        {
            node.hasLabel( QUUX );
        }
    };

    private static final NodeFacadeMethod GET_LABELS = new NodeFacadeMethod( "ResourceIterable<Label> getLabels()" )

    {
        @Override
        public void call( Node node )
        {
            for ( Label label : node.getLabels() )
            {

            }
        }
    };

    static final NodeFacadeMethod[] ALL_NODE_FACADE_METHODS = {HAS_PROPERTY, GET_PROPERTY, GET_PROPERTY_WITH_DEFAULT,
            SET_PROPERTY, REMOVE_PROPERTY, GET_PROPERTY_KEYS, GET_PROPERTY_VALUES, DELETE, GET_RELATIONSHIPS,
            HAS_RELATIONSHIP, GET_RELATIONSHIPS_BY_TYPE, GET_RELATIONSHIPS_BY_DIRECTION_AND_TYPES,
            HAS_RELATIONSHIP_BY_TYPE, HAS_RELATIONSHIP_BY_DIRECTION_AND_TYPE, GET_RELATIONSHIPS_BY_DIRECTION,
            HAS_RELATIONSHIP_BY_DIRECTION, GET_RELATIONSHIPS_BY_TYPE_AND_DIRECTION,
            HAS_RELATIONSHIP_BY_TYPE_AND_DIRECTION, GET_SINGLE_RELATIONSHIP, CREATE_RELATIONSHIP_TO,
            TRAVERSE_USING_ONE_TYPE_AND_DIRECTION, TRAVERSE_USING_TWO_TYPES_AND_DIRECTIONS,
            TRAVERSE_USING_ANY_NUMBER_OF_TYPES_AND_DIRECTIONS, ADD_LABEL, REMOVE_LABEL, HAS_LABEL, GET_LABELS};
}
