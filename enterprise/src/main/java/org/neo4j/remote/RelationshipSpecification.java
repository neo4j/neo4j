/*
 * Copyright (c) 2008-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.remote;

import org.neo4j.graphdb.Relationship;

/**
 * Serialized object representing a {@link Relationship}.
 * 
 * @author Tobias Ivarsson
 */
public final class RelationshipSpecification implements EncodedObject
{
    private static final long serialVersionUID = 1L;
    final long relationshipId;
    final String name;
    final long startNodeId;
    final long endNodeId;

    RelationshipSpecification( long id, String typeName, long startNode,
        long endNode )
    {
        relationshipId = id;
        name = typeName;
        startNodeId = startNode;
        endNodeId = endNode;
    }

    /**
     * Create a new {@link Relationship} serialization.
     * @param relationship
     *            the {@link Relationship} to serialize.
     */
    public RelationshipSpecification( Relationship relationship )
    {
        this( relationship.getId(), relationship.getType().name(), relationship
            .getStartNode().getId(), relationship.getEndNode().getId() );
    }
}
