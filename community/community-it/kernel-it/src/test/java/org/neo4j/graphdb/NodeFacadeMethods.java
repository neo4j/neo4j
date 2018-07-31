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
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.FacadeMethod.consume;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;

@SuppressWarnings( "UnusedDeclaration" )
public class NodeFacadeMethods
{
    private static final RelationshipType FOO = withName( "foo" );
    private static final RelationshipType BAR = withName( "bar" );
    private static final Label QUUX = label( "quux" );

    private static final FacadeMethod<Node> HAS_PROPERTY = new FacadeMethod<>( "boolean hasProperty( String key )", n -> n.hasProperty( "foo" ) );
    private static final FacadeMethod<Node> GET_PROPERTY = new FacadeMethod<>( "Object getProperty( String key )", n -> n.getProperty( "foo" ) );
    private static final FacadeMethod<Node> GET_PROPERTY_WITH_DEFAULT = new FacadeMethod<>( "Object getProperty( String key, Object defaultValue )", n -> n.getProperty( "foo", 42 ) );
    private static final FacadeMethod<Node> SET_PROPERTY = new FacadeMethod<>( "void setProperty( String key, Object value )", n -> n.setProperty( "foo", 42 ) );
    private static final FacadeMethod<Node> REMOVE_PROPERTY = new FacadeMethod<>( "Object removeProperty( String key )", n -> n.removeProperty( "foo" ) );
    private static final FacadeMethod<Node> GET_PROPERTY_KEYS = new FacadeMethod<>( "Iterable<String> getPropertyKeys()", n -> consume( n.getPropertyKeys() ) );
    private static final FacadeMethod<Node> DELETE = new FacadeMethod<>( "void delete()", Node::delete );
    private static final FacadeMethod<Node> GET_RELATIONSHIPS = new FacadeMethod<>( "Iterable<Relationship> getRelationships()", n -> consume( n.getRelationships() ) );
    private static final FacadeMethod<Node> HAS_RELATIONSHIP = new FacadeMethod<>( "boolean hasRelationship()", Node::hasRelationship );
    private static final FacadeMethod<Node> GET_RELATIONSHIPS_BY_TYPE = new FacadeMethod<>( "Iterable<Relationship> getRelationships( RelationshipType... types )", n -> consume( n.getRelationships( FOO, BAR ) ) );
    private static final FacadeMethod<Node> GET_RELATIONSHIPS_BY_DIRECTION_AND_TYPES = new FacadeMethod<>( "Iterable<Relationship> getRelationships( Direction direction, RelationshipType... types )", n -> consume( n.getRelationships( BOTH, FOO, BAR ) ) );
    private static final FacadeMethod<Node> HAS_RELATIONSHIP_BY_TYPE = new FacadeMethod<>( "boolean hasRelationship( RelationshipType... types )", n -> n.hasRelationship( FOO ) );
    private static final FacadeMethod<Node> HAS_RELATIONSHIP_BY_DIRECTION_AND_TYPE = new FacadeMethod<>( "boolean hasRelationship( Direction direction, RelationshipType... types )", n -> n.hasRelationship( BOTH, FOO ) );
    private static final FacadeMethod<Node> GET_RELATIONSHIPS_BY_DIRECTION = new FacadeMethod<>( "Iterable<Relationship> getRelationships( Direction dir )", n -> consume( n.getRelationships( BOTH ) ) );
    private static final FacadeMethod<Node> HAS_RELATIONSHIP_BY_DIRECTION = new FacadeMethod<>( "boolean hasRelationship( Direction dir )", n -> n.hasRelationship( BOTH ) );
    private static final FacadeMethod<Node> GET_RELATIONSHIPS_BY_TYPE_AND_DIRECTION = new FacadeMethod<>( "Iterable<Relationship> getRelationships( RelationshipType type, Direction dir )", n -> consume( n.getRelationships( FOO, BOTH ) ) );
    private static final FacadeMethod<Node> HAS_RELATIONSHIP_BY_TYPE_AND_DIRECTION = new FacadeMethod<>( "boolean hasRelationship( RelationshipType type, Direction dir )", n -> n.hasRelationship( FOO, BOTH ) );
    private static final FacadeMethod<Node> GET_SINGLE_RELATIONSHIP = new FacadeMethod<>( "Relationship getSingleRelationship( RelationshipType type, Direction dir )", n -> n.getSingleRelationship( FOO, BOTH ) );
    private static final FacadeMethod<Node> CREATE_RELATIONSHIP_TO = new FacadeMethod<>( "Relationship createRelationshipTo( Node otherNode, RelationshipType type )", n -> n.createRelationshipTo( n, FOO ) );
    private static final FacadeMethod<Node> ADD_LABEL = new FacadeMethod<>( "void addLabel( Label label )", n -> n.addLabel( QUUX ) );
    private static final FacadeMethod<Node> REMOVE_LABEL = new FacadeMethod<>( "void removeLabel( Label label )", n -> n.removeLabel( QUUX ) );
    private static final FacadeMethod<Node> HAS_LABEL = new FacadeMethod<>( "boolean hasLabel( Label label )", n -> n.hasLabel( QUUX ) );
    private static final FacadeMethod<Node> GET_LABELS = new FacadeMethod<>( "ResourceIterable<Label> getLabels()", n -> consume( n.getLabels() ) );

    static final Iterable<FacadeMethod<Node>> ALL_NODE_FACADE_METHODS = unmodifiableCollection( asList(
        HAS_PROPERTY,
        GET_PROPERTY,
        GET_PROPERTY_WITH_DEFAULT,
        SET_PROPERTY,
        REMOVE_PROPERTY,
        GET_PROPERTY_KEYS,
        DELETE, GET_RELATIONSHIPS,
        HAS_RELATIONSHIP,
        GET_RELATIONSHIPS_BY_TYPE,
        GET_RELATIONSHIPS_BY_DIRECTION_AND_TYPES,
        HAS_RELATIONSHIP_BY_TYPE,
        HAS_RELATIONSHIP_BY_DIRECTION_AND_TYPE,
        GET_RELATIONSHIPS_BY_DIRECTION,
        HAS_RELATIONSHIP_BY_DIRECTION,
        GET_RELATIONSHIPS_BY_TYPE_AND_DIRECTION,
        HAS_RELATIONSHIP_BY_TYPE_AND_DIRECTION,
        GET_SINGLE_RELATIONSHIP, CREATE_RELATIONSHIP_TO,
        ADD_LABEL,
        REMOVE_LABEL,
        HAS_LABEL,
        GET_LABELS
    ) );

    private NodeFacadeMethods()
    {
    }
}
