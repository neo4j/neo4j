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
package org.neo4j.storageengine.api;

import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.kernel.impl.locking.Lock;

/**
 * Represents a single node from a cursor.
 */
public interface NodeItem
{
    /**
     * @return id of current node
     * @throws IllegalStateException if no current entity is selected
     */
    long id();

    /**
     * @return label cursor for current node
     * @throws IllegalStateException if no current node is selected
     */
    PrimitiveIntSet labels();

    /**
     * @return whether or not this node has been marked as being dense, i.e. exceeding a certain threshold
     * of number of relationships.
     */
    boolean isDense();

    /**
     * @param labelId label token id to check.
     * @return whether or not this node has the given label.
     */
    boolean hasLabel( int labelId );

    long nextGroupId();

    long nextRelationshipId();

    long nextPropertyId();

    Lock lock();
}
