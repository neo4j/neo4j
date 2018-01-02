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
package org.neo4j.graphdb;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

@SuppressWarnings("UnusedDeclaration")
public class RelationshipFacadeMethods
{
    private static final FacadeMethod<Relationship> HAS_PROPERTY =
        new FacadeMethod<Relationship>( "boolean hasProperty( String key )" )
    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.hasProperty( "foo" );
        }
    };

    private static final FacadeMethod<Relationship> GET_PROPERTY =
        new FacadeMethod<Relationship>( "Object getProperty( String key )" )
    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.getProperty( "foo" );
        }
    };

    private static final FacadeMethod<Relationship> GET_PROPERTY_WITH_DEFAULT =
        new FacadeMethod<Relationship>( "Object getProperty( String key, Object defaultValue )" )
    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.getProperty( "foo", 42 );
        }
    };

    private static final FacadeMethod<Relationship> SET_PROPERTY =
        new FacadeMethod<Relationship>( "void setProperty( String key, Object value )" )

    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.setProperty( "foo", 42 );
        }
    };

    private static final FacadeMethod<Relationship> REMOVE_PROPERTY =
        new FacadeMethod<Relationship>( "Object removeProperty( String key )" )
    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.removeProperty( "foo" );
        }
    };

    private static final FacadeMethod<Relationship> GET_PROPERTY_KEYS =
        new FacadeMethod<Relationship>( "Iterable<String> getPropertyKeys()" )
    {
        @Override
        public void call( Relationship relationship )
        {
            for ( String key : relationship.getPropertyKeys() )
            {

            }
        }
    };

    private static final FacadeMethod<Relationship> DELETE = new FacadeMethod<Relationship>( "void delete()" )

    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.delete();
        }
    };

    private static final FacadeMethod<Relationship> GET_START_NODE =
        new FacadeMethod<Relationship>( "Node getStartNode()" )
    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.getStartNode();
        }
    };

    private static final FacadeMethod<Relationship> GET_END_NODE = new FacadeMethod<Relationship>( "Node getEndNode()" )
    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.getEndNode();
        }
    };

    private static final FacadeMethod<Relationship> GET_OTHER_NODE =
        new FacadeMethod<Relationship>( "Node getOtherNode( Node node )" )
    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.getOtherNode( null );
        }
    };

    private static final FacadeMethod<Relationship> GET_NODES = new FacadeMethod<Relationship>( "Node[] getNodes()" )
    {
        @SuppressWarnings("UnusedDeclaration")
        @Override
        public void call( Relationship relationship )
        {
            for ( Node node : relationship.getNodes() )
            {

            }
        }
    };

    private static final FacadeMethod<Relationship> GET_TYPE =
            new FacadeMethod<Relationship>( "RelationshipType getType()" )
    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.getType();
        }
    };

    private static final FacadeMethod<Relationship> IS_TYPE =
        new FacadeMethod<Relationship>( "boolean isType( RelationshipType type )" )
    {
        @Override
        public void call( Relationship relationship )
        {
            relationship.isType( withName( "foo" ) );
        }
    };

    static final Iterable<FacadeMethod<Relationship>> ALL_RELATIONSHIP_FACADE_METHODS = unmodifiableCollection( asList(
        HAS_PROPERTY,
        GET_PROPERTY,
        GET_PROPERTY_WITH_DEFAULT,
        SET_PROPERTY,
        REMOVE_PROPERTY,
        GET_PROPERTY_KEYS,
        DELETE,
        GET_START_NODE,
        GET_END_NODE,
        GET_OTHER_NODE,
        GET_NODES,
        GET_TYPE,
        IS_TYPE
    ) );
}
