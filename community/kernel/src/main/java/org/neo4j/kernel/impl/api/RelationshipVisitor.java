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
package org.neo4j.kernel.impl.api;

/**
 * Visitor of relationship data.
 *
 * @param <EXCEPTION> exception thrown from the {@link #visit(long, int, long, long)} method.
 */
public interface RelationshipVisitor<EXCEPTION extends Exception>
{
    /**
     * Objects which can accept these {@link RelationshipVisitor visitors} should implement this interface.
     */
    interface Home
    {
        <EXCEPTION extends Exception> boolean relationshipVisit( long relId,
                RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION;
    }

    /**
     * Visits data about a relationship.
     *
     * @param relationshipId relationship id to visit data for.
     * @param typeId relationship type id for the relationship.
     * @param startNodeId id of start node of the relationship.
     * @param endNodeId id of the end node of the relationship.
     */
    void visit( long relationshipId, int typeId, long startNodeId, long endNodeId ) throws EXCEPTION;
}
