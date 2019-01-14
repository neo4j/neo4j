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
package org.neo4j.graphdb;

import java.util.function.Consumer;

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.FacadeMethod.BAR;
import static org.neo4j.graphdb.FacadeMethod.FOO;
import static org.neo4j.graphdb.FacadeMethod.QUUX;
import static org.neo4j.graphdb.FacadeMethod.consume;

public enum NodeFacadeMethods implements Consumer<Node>
{
    HAS_PROPERTY( new FacadeMethod<>( "boolean hasProperty( String key )", n -> n.hasProperty( "foo" ) ) ),
    GET_PROPERTY( new FacadeMethod<>( "Object getProperty( String key )", n -> n.getProperty( "foo" ) ) ),
    GET_PROPERTY_WITH_DEFAULT( new FacadeMethod<>( "Object getProperty( String key, Object defaultValue )", n -> n.getProperty( "foo", 42 ) ) ),
    SET_PROPERTY( new FacadeMethod<>( "void setProperty( String key, Object value )", n -> n.setProperty( "foo", 42 ) ) ),
    REMOVE_PROPERTY( new FacadeMethod<>( "Object removeProperty( String key )", n -> n.removeProperty( "foo" ) ) ),
    GET_PROPERTY_KEYS( new FacadeMethod<>( "Iterable<String> getPropertyKeys()", n -> consume( n.getPropertyKeys() ) ) ),
    DELETE( new FacadeMethod<>( "void delete()", Node::delete ) ),
    GET_RELATIONSHIPS( new FacadeMethod<>( "Iterable<Relationship> getRelationships()", n -> consume( n.getRelationships() ) ) ),
    HAS_RELATIONSHIP( new FacadeMethod<>( "boolean hasRelationship()", Node::hasRelationship ) ),
    GET_RELATIONSHIPS_BY_TYPE(
            new FacadeMethod<>( "Iterable<Relationship> getRelationships( RelationshipType... types )", n -> consume( n.getRelationships( FOO, BAR ) ) ) ),
    GET_RELATIONSHIPS_BY_DIRECTION_AND_TYPES( new FacadeMethod<>( "Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types )",
            n -> consume( n.getRelationships( BOTH, FOO, BAR ) ) ) ),
    HAS_RELATIONSHIP_BY_TYPE( new FacadeMethod<>( "boolean hasRelationship( RelationshipType... types )", n -> n.hasRelationship( FOO ) ) ),
    HAS_RELATIONSHIP_BY_DIRECTION_AND_TYPE(
            new FacadeMethod<>( "boolean hasRelationship( Direction direction, RelationshipType... types )", n -> n.hasRelationship( BOTH, FOO ) ) ),
    GET_RELATIONSHIPS_BY_DIRECTION(
            new FacadeMethod<>( "Iterable<Relationship> getRelationships( Direction dir )", n -> consume( n.getRelationships( BOTH ) ) ) ),
    HAS_RELATIONSHIP_BY_DIRECTION( new FacadeMethod<>( "boolean hasRelationship( Direction dir )", n -> n.hasRelationship( BOTH ) ) ),
    GET_RELATIONSHIPS_BY_TYPE_AND_DIRECTION( new FacadeMethod<>( "Iterable<Relationship> getRelationships( RelationshipType type, Direction dir )",
            n -> consume( n.getRelationships( FOO, BOTH ) ) ) ),
    HAS_RELATIONSHIP_BY_TYPE_AND_DIRECTION(
            new FacadeMethod<>( "boolean hasRelationship( RelationshipType type, Direction dir )", n -> n.hasRelationship( FOO, BOTH ) ) ),
    GET_SINGLE_RELATIONSHIP(
            new FacadeMethod<>( "Relationship getSingleRelationship( RelationshipType type, Direction dir )", n -> n.getSingleRelationship( FOO, BOTH ) ) ),
    CREATE_RELATIONSHIP_TO(
            new FacadeMethod<>( "Relationship createRelationshipTo( Node otherNode, RelationshipType type )", n -> n.createRelationshipTo( n, FOO ) ) ),
    ADD_LABEL( new FacadeMethod<>( "void addLabel( Label label )", n -> n.addLabel( QUUX ) ) ),
    REMOVE_LABEL( new FacadeMethod<>( "void removeLabel( Label label )", n -> n.removeLabel( QUUX ) ) ),
    HAS_LABEL( new FacadeMethod<>( "boolean hasLabel( Label label )", n -> n.hasLabel( QUUX ) ) ),
    GET_LABELS( new FacadeMethod<>( "ResourceIterable<Label> getLabels()", n -> consume( n.getLabels() ) ) );

    private final FacadeMethod<Node> facadeMethod;

    NodeFacadeMethods( FacadeMethod<Node> facadeMethod )
    {
        this.facadeMethod = facadeMethod;
    }

    @Override
    public void accept( Node node )
    {
        facadeMethod.accept( node );
    }

    @Override
    public String toString()
    {
        return facadeMethod.toString();
    }
}
