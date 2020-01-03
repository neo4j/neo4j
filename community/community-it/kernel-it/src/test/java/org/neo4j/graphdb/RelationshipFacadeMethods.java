/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.graphdb;

import java.util.function.Consumer;

import static org.neo4j.graphdb.FacadeMethod.consume;
import static org.neo4j.graphdb.RelationshipType.withName;

public enum RelationshipFacadeMethods implements Consumer<Relationship>
{
    HAS_PROPERTY( new FacadeMethod<>( "boolean hasProperty( String key )", r -> r.hasProperty( "foo" ) ) ),
    GET_PROPERTY( new FacadeMethod<>( "Object getProperty( String key )", r -> r.getProperty( "foo" ) ) ),
    GET_PROPERTY_WITH_DEFAULT( new FacadeMethod<>( "Object getProperty( String key, Object defaultValue )", r -> r.getProperty( "foo", 42 ) ) ),
    SET_PROPERTY( new FacadeMethod<>( "void setProperty( String key, Object value )", r -> r.setProperty( "foo", 42 ) ) ),
    REMOVE_PROPERTY( new FacadeMethod<>( "Object removeProperty( String key )", r -> r.removeProperty( "foo" ) ) ),
    GET_PROPERTY_KEYS( new FacadeMethod<>( "Iterable<String> getPropertyKeys()", r -> consume( r.getPropertyKeys() ) ) ),
    DELETE( new FacadeMethod<>( "void delete()", Relationship::delete ) ),
    GET_START_NODE( new FacadeMethod<>( "Node getStartNode()", Relationship::getStartNode ) ),
    GET_END_NODE( new FacadeMethod<>( "Node getEndNode()", Relationship::getEndNode ) ),
    GET_OTHER_NODE( new FacadeMethod<>( "Node getOtherNode( Node node )", r -> r.getOtherNode( null ) ) ),
    GET_NODES( new FacadeMethod<>( "Node[] getNodes()", Relationship::getNodes ) ),
    GET_TYPE( new FacadeMethod<>( "RelationshipType getType()", Relationship::getType ) ),
    IS_TYPE( new FacadeMethod<>( "boolean isType( RelationshipType type )", r -> r.isType( withName( "foo" ) ) ) );

    private final FacadeMethod<Relationship> facadeMethod;

    RelationshipFacadeMethods( FacadeMethod<Relationship> facadeMethod )
    {
        this.facadeMethod = facadeMethod;
    }

    @Override
    public void accept( Relationship relationship )
    {
        facadeMethod.accept( relationship );
    }

    @Override
    public String toString()
    {
        return facadeMethod.toString();
    }
}
