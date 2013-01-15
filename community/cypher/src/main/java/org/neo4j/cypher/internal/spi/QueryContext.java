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
package org.neo4j.cypher.internal.spi;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/*
 * Developer note: This is an attempt at an internal graph database API, which defines a clean cut between
 * two layers, the query engine layer and, for lack of a better name, the core database layer.
 *
 * Building the query engine layer on top of an internal layer means we can move much faster, not
 * having to worry about deprecations and so on. It is also acceptable if this layer is a bit clunkier, in this
 * case we are, for instance, not exposing any node or relationship objects, but provide direct methods for manipulating
 * them by ids instead.
 *
 * The driver for this was clarifying who is responsible for ensuring query isolation. By exposing a query concept in
 * the core layer, we can move that responsibility outside of the scope of cypher.
 */

public interface QueryContext
{
    Operations<Node> nodeOps();

    Operations<Relationship> relationshipOps();

    Node createNode();

    Relationship createRelationship( Node start, Node end, String relType );

    // TODO: Expose a high-tech traversal framework here in some way, and remove the getRelationshipsFor method
    Iterable<Relationship> getRelationshipsFor( Node node, Direction dir, String... types );

    void addLabelsToNode( Node node, Iterable<Long> labelIds );

    // TODO: Figure out how this can be a primitive type
    Long getOrCreateLabelId( String labelName );

    /**
     * Release all resources held by this context.
     */
    void close();

    public interface Operations<T extends PropertyContainer>
    {
        void delete( T obj );

        void setProperty( T obj, String propertyKey, Object value );

        void removeProperty( T obj, String propertyKey );

        Object getProperty( T obj, String propertyKey );

        boolean hasProperty( T obj, String propertyKey );

        Iterable<String> propertyKeys( T obj );

        T getById( long id );
    }
}

