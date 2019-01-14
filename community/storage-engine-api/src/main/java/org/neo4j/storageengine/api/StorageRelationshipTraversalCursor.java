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

public interface StorageRelationshipTraversalCursor extends StorageRelationshipCursor
{
    long neighbourNodeReference();

    long originNodeReference();

    /**
     * Called when traversing all relationships for a node.
     *
     * @param nodeReference reference to the node that has these relationships.
     * @param reference reference to the relationships.
     * @param nodeIsDense whether or not node is dense.
     */
    void init( long nodeReference, long reference, boolean nodeIsDense );

    /**
     * Called when traversing all relationships for a node.
     *
     * @param nodeReference reference to the node that has these relationships.
     * @param reference reference to the relationships.
     * @param type type of relationships. If -1 then type/direction of first read relationship will act as filter.
     * @param direction direction of relationships.
     * @param nodeIsDense whether or not node is dense.
     */
    void init( long nodeReference, long reference, int type, RelationshipDirection direction, boolean nodeIsDense );
}
