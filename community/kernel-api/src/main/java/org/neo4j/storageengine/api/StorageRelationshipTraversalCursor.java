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

public interface StorageRelationshipTraversalCursor extends StorageRelationshipCursor {
    long neighbourNodeReference();

    long originNodeReference();

    /**
     * Called when traversing relationships for a node.
     * @param nodeReference reference to the node that has these relationships.
     * @param reference reference to the relationships.
     * @param selection which relationships to select.
     */
    void init(long nodeReference, long reference, RelationshipSelection selection);

    /**
     * Called when traversing all relationships for a node.
     *
     * @param nodeCursor node cursor refers to these relationships.
     */
    default void init(StorageNodeCursor nodeCursor, RelationshipSelection selection) {
        init(nodeCursor.entityReference(), nodeCursor.relationshipsReference(), selection);
    }
}
