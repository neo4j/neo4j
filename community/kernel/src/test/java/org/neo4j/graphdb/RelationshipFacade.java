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

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

abstract class RelationshipFacadeMethod
{
    private final String methodSignature;

    public RelationshipFacadeMethod( String methodSignature )
    {

        this.methodSignature = methodSignature;
    }

    abstract void call( Relationship relationship );

    @Override
    public String toString()
    {
        return methodSignature;
    }
}

class RelationshipFacadeMethods
{
    private static final RelationshipFacadeMethod HAS_PROPERTY = new RelationshipFacadeMethod( "boolean hasProperty( " +
            "String key )" )
    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.hasProperty( "foo" );
        }
    };

    private static final RelationshipFacadeMethod GET_PROPERTY = new RelationshipFacadeMethod( "Object getProperty( " +
            "String key )" )
    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.getProperty( "foo" );
        }
    };

    private static final RelationshipFacadeMethod GET_PROPERTY_WITH_DEFAULT = new RelationshipFacadeMethod( "Object " +
            "getProperty( String key, Object defaultValue )" )
    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.getProperty( "foo", 42 );
        }
    };

    private static final RelationshipFacadeMethod SET_PROPERTY = new RelationshipFacadeMethod( "void setProperty( " +
            "String key, Object value )" )

    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.setProperty( "foo", 42 );
        }
    };

    private static final RelationshipFacadeMethod REMOVE_PROPERTY = new RelationshipFacadeMethod( "Object " +
            "removeProperty( String key )" )

    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.removeProperty( "foo" );
        }
    };

    private static final RelationshipFacadeMethod GET_PROPERTY_KEYS = new RelationshipFacadeMethod( "Iterable<String>" +
            " getPropertyKeys()" )
    {
        @Override
        public void call( Relationship relationship )
        {
            for ( String key : relationship.getPropertyKeys() )
            {

            }
        }
    };

    private static final RelationshipFacadeMethod GET_PROPERTY_VALUES = new RelationshipFacadeMethod(
            "Iterable<Object> getPropertyValues()" )
    {
        @Override
        public void call( Relationship relationship )
        {
            for ( Object value : relationship.getPropertyValues() )
            {

            }
        }
    };

    private static final RelationshipFacadeMethod DELETE = new RelationshipFacadeMethod( "void delete()" )

    {
        @Override
        void call( Relationship relationship )
        {
            relationship.delete();
        }
    };

    private static final RelationshipFacadeMethod GET_START_NODE = new RelationshipFacadeMethod( "Node getStartNode()" )

    {
        @Override
        void call( Relationship relationship )
        {
            relationship.getStartNode();
        }
    };

    private static final RelationshipFacadeMethod GET_END_NODE = new RelationshipFacadeMethod( "Node getEndNode()" )

    {
        @Override
        void call( Relationship relationship )
        {
            relationship.getEndNode();
        }
    };

    private static final RelationshipFacadeMethod GET_OTHER_NODE = new RelationshipFacadeMethod( "Node getOtherNode( " +
            "Node node )" )
    {
        @Override
        void call( Relationship relationship )
        {
            relationship.getOtherNode( null );
        }
    };

    private static final RelationshipFacadeMethod GET_NODES = new RelationshipFacadeMethod( "Node[] getNodes()" )
    {
        @Override
        void call( Relationship relationship )
        {
            for ( Node node : relationship.getNodes() )
            {

            }
        }
    };

    private static final RelationshipFacadeMethod GET_TYPE = new RelationshipFacadeMethod( "RelationshipType getType" +
            "()" )
    {
        @Override
        void call( Relationship relationship )
        {
            relationship.getType();
        }
    };

    private static final RelationshipFacadeMethod IS_TYPE = new RelationshipFacadeMethod( "boolean isType( " +
            "RelationshipType type )" )
    {
        @Override
        void call( Relationship relationship )
        {
            relationship.isType( withName( "foo" ) );
        }
    };

    static final RelationshipFacadeMethod[] ALL_RELATIONSHIP_FACADE_METHODS = {HAS_PROPERTY, GET_PROPERTY,
            GET_PROPERTY_WITH_DEFAULT, SET_PROPERTY, REMOVE_PROPERTY, GET_PROPERTY_KEYS, GET_PROPERTY_VALUES, DELETE,
            GET_START_NODE, GET_END_NODE, GET_OTHER_NODE, GET_NODES, GET_TYPE, IS_TYPE};
}
