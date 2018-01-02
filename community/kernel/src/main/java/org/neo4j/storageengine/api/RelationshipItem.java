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
package org.neo4j.storageengine.api;

import org.neo4j.kernel.impl.locking.Lock;

/**
 * Represents a single relationship from a cursor.
 */
public interface RelationshipItem
{
    /**
     * @return id of current entity
     * @throws IllegalStateException if no current entity is selected
     */
    long id();

    /**
     * @return relationship type for current relationship
     */
    int type();

    /**
     * @return start node of this relationship
     */
    long startNode();

    /**
     * @return end node of this relationship
     */
    long endNode();

    /**
     *
     * @param nodeId of the node you are not interested in
     * @return end node if start node is passed in, start node if end node is passed
     */
    long otherNode( long nodeId );

    long nextPropertyId();

    Lock lock();
}
