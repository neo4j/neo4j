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

/**
 * A relationship type is mandatory on all relationships and is used to navigate
 * the graph. RelationshipType is in particular a key part of the
 * {@link Traverser traverser framework} but it's also used in various
 * {@link Node#getRelationships() relationship operations} on Node.
 * <p>
 * Relationship types are declared by the client and can be handled either
 * dynamically or statically in a Neo4j-based application. Internally,
 * relationship types are dynamic. This means that every time a client invokes
 * {@link Node#createRelationshipTo(Node,RelationshipType)
 * node.createRelationshipTo(anotherNode, newRelType)} and passes in a new
 * relationship type then the new type will be transparently created. So
 * instantiating a RelationshipType instance will not create it in the
 * underlying storage, it is persisted only when the first relationship of that
 * type is created.
 * <p>
 * However, in case the application does not need to dynamically create
 * relationship types (most don't), then it's nice to have the compile-time
 * benefits of a static set of relationship types. Fortunately, RelationshipType
 * is designed to work well with Java 5 enums. This means that it's very easy to
 * define a set of valid relationship types by declaring an enum that implements
 * RelationshipType and then reuse that across the application. For example,
 * here's how you would define an enum to hold all your relationship types:
 * 
 * <pre>
 * <code>
 * enum MyRelationshipTypes implements {@link RelationshipType}
 * {
 *     CONTAINED_IN, KNOWS
 * }
 * </code>
 * </pre>
 * 
 * Then later, it's as easy to use as:
 * 
 * <pre>
 * <code>
 * node.{@link Node#createRelationshipTo(Node, RelationshipType) createRelationshipTo}( anotherNode, {@link RelationshipType MyRelationshipTypes.KNOWS} );
 * for ( {@link Relationship} rel : node.{@link Node#getRelationships(RelationshipType...) getRelationships}( MyRelationshipTypes.KNOWS ) )
 * {
 * 	// ...
 * }
 * </code>
 * </pre>
 * 
 * <p>
 * It's very important to note that a relationship type is uniquely identified
 * by its name, not by any particular instance that implements this interface.
 * This means that the proper way to check if two relationship types are equal
 * is by invoking <code>equals()</code> on their {@link #name names}, NOT by
 * using Java's identity operator (<code>==</code>) or <code>equals()</code> on
 * the relationship type instances. A consequence of this is that you can NOT
 * use relationship types in hashed collections such as
 * {@link java.util.HashMap HashMap} and {@link java.util.HashSet HashSet}.
 * <p>
 * However, you usually want to check whether a specific relationship
 * <i>instance</i> is of a certain type. That is best achieved with the
 * {@link Relationship#isType Relationship.isType} method, such as:
 * <pre>
 * <code>
 * if ( rel.isType( MyRelationshipTypes.CONTAINED_IN ) )
 * {
 *     ...
 * }
 * </code>
 * </pre>
 */
public interface RelationshipType
{
    /**
     * Returns the name of the relationship type. The name uniquely identifies a
     * relationship type, i.e. two different RelationshipType instances with
     * different object identifiers (and possibly even different classes) are
     * semantically equivalent if they have {@link String#equals(Object) equal}
     * names.
     * 
     * @return the name of the relationship type
     */
    String name();
}
