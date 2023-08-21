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
package org.neo4j.internal.counts;

import org.neo4j.storageengine.api.RelationshipDirection;

public interface DegreeUpdater extends AutoCloseable {
    DegreeUpdater NO_OP_UPDATER = new DegreeUpdater() {
        @Override
        public void close() {}

        @Override
        public void increment(long groupId, RelationshipDirection direction, long delta) {}
    };

    @Override
    void close();

    /**
     * Changes the degree of the given groupId and direction.
     *
     * @param groupId   the relationship group ID to make the change for.
     * @param direction the direction to make the change for.
     * @param delta     delta value to apply, can be either positive or negative.
     */
    void increment(long groupId, RelationshipDirection direction, long delta);
}
