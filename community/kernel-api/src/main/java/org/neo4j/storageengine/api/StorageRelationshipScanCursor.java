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
 * Cursor over relationships.
 */
public interface StorageRelationshipScanCursor
        extends StorageRelationshipCursor, StorageEntityScanCursor<AllRelationshipsScan> {
    /**
     * Initializes this cursor so that the next call to {@link #next()} will place this cursor at that entity.
     * @param reference entity to place this cursor at the next call to {@link #next()}.
     * @param sourceNodeReference source node reference of this relationship.
     * @param type type of this relationship.
     * @param targetNodeReference target node reference of this relationship.
     */
    void single(long reference, long sourceNodeReference, int type, long targetNodeReference);
}
