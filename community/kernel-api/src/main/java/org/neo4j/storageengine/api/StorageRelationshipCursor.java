/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api;

/**
 * Shared interface between the two {@link StorageRelationshipScanCursor} and {@link StorageRelationshipTraversalCursor}.
 */
public interface StorageRelationshipCursor extends StorageEntityCursor {
    /**
     * @return relationship type of the relationship this cursor is placed at.
     */
    int type();

    /**
     * @return source node of the relationship this cursor is placed at.
     */
    long sourceNodeReference();

    /**
     * @return target node of the relationship this cursor is placed at.
     */
    long targetNodeReference();

    /**
     * @return a relationship ID similar to {@link #entityReference()}, but can contain more information about how to identify and find a relationship.
     */
    default Reference relationshipReference() {
        return new LongReference(entityReference());
    }
}
