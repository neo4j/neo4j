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
package org.neo4j.graphdb;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;
import static org.neo4j.graphdb.FacadeMethod.consume;
import static org.neo4j.graphdb.RelationshipType.withName;

@SuppressWarnings( "UnusedDeclaration" )
public class RelationshipFacadeMethods
{
    private static final FacadeMethod<Relationship> HAS_PROPERTY = new FacadeMethod<>( "boolean hasProperty( String key )", r -> r.hasProperty( "foo" ) );
    private static final FacadeMethod<Relationship> GET_PROPERTY = new FacadeMethod<>( "Object getProperty( String key )", r -> r.getProperty( "foo" ) );
    private static final FacadeMethod<Relationship> GET_PROPERTY_WITH_DEFAULT = new FacadeMethod<>( "Object getProperty( String key, Object defaultValue )", r -> r.getProperty( "foo", 42 ) );
    private static final FacadeMethod<Relationship> SET_PROPERTY = new FacadeMethod<>( "void setProperty( String key, Object value )", r -> r.setProperty( "foo", 42 ) );
    private static final FacadeMethod<Relationship> REMOVE_PROPERTY = new FacadeMethod<>( "Object removeProperty( String key )", r -> r.removeProperty( "foo" ) );
    private static final FacadeMethod<Relationship> GET_PROPERTY_KEYS = new FacadeMethod<>( "Iterable<String> getPropertyKeys()", r -> consume( r.getPropertyKeys() ) );
    private static final FacadeMethod<Relationship> DELETE = new FacadeMethod<>( "void delete()", Relationship::delete );
    private static final FacadeMethod<Relationship> GET_START_NODE = new FacadeMethod<>( "Node getStartNode()", Relationship::getStartNode );
    private static final FacadeMethod<Relationship> GET_START_NODE_ID = new FacadeMethod<>( "Node getStartNode()", Relationship::getStartNodeId );
    private static final FacadeMethod<Relationship> GET_END_NODE = new FacadeMethod<>( "Node getEndNode()", Relationship::getEndNode );
    private static final FacadeMethod<Relationship> GET_END_NODE_ID = new FacadeMethod<>( "Node getEndNode()", Relationship::getEndNodeId );
    private static final FacadeMethod<Relationship> GET_OTHER_NODE = new FacadeMethod<>( "Node getOtherNode( Node node )", r -> r.getOtherNode( null ) );
    private static final FacadeMethod<Relationship> GET_OTHER_NODE_ID = new FacadeMethod<>( "Node getOtherNode( Node node )", r -> r.getOtherNodeId( 42 ) );
    private static final FacadeMethod<Relationship> GET_NODES = new FacadeMethod<>( "Node[] getNodes()", Relationship::getNodes );
    private static final FacadeMethod<Relationship> GET_TYPE = new FacadeMethod<>( "RelationshipType getType()", Relationship::getType );
    private static final FacadeMethod<Relationship> IS_TYPE = new FacadeMethod<>( "boolean isType( RelationshipType type )", r -> r.isType( withName( "foo" ) ) );

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
        GET_OTHER_NODE_ID,
        GET_NODES,
        GET_TYPE,
        IS_TYPE
    ) );

    private RelationshipFacadeMethods()
    {
    }
}
