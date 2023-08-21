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
package org.neo4j.internal.schema;

public interface StorageEngineIndexingBehaviour {

    StorageEngineIndexingBehaviour EMPTY = new StorageEngineIndexingBehaviour() {
        @Override
        public boolean useNodeIdsInRelationshipTokenIndex() {
            return false;
        }

        @Override
        public boolean requireCoordinationLocks() {
            return true;
        }

        @Override
        public int nodesPerPage() {
            return 0;
        }

        @Override
        public int relationshipsPerPage() {
            return 0;
        }
    };

    /**
     * @return {@code true} if this storage engine wants to let any relationship type lookup index be based around node ids
     * instead of relationship ids so that generated updates from this storage engine for that index will contain which nodes
     * contain at least one outgoing relationship of a given relationship type.
     */
    boolean useNodeIdsInRelationshipTokenIndex();

    /**
     * @return whether the store/lookup-index scans for building indexes require locks coordination using
     * {@link org.neo4j.lock.LockService}. If {@code true} then locks will be acquired for each entity during
     * scan, otherwise not.
     */
    boolean requireCoordinationLocks();

    /**
     * @return number of nodes per page cache page
     */
    int nodesPerPage();

    /**
     * @return number of relationship per page cache page, but engines that return true for {@link #useNodeIdsInRelationshipTokenIndex}
     * should return nodes per page...
     */
    int relationshipsPerPage();
}
