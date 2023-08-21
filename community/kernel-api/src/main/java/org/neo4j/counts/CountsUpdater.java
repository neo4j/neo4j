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
package org.neo4j.counts;

/**
 * Updater of counts.
 */
public interface CountsUpdater extends AutoCloseable {

    CountsUpdater NO_OP_UPDATER = new CountsUpdater() {
        @Override
        public void incrementNodeCount(long labelId, long delta) { // no-op
        }

        @Override
        public void incrementRelationshipCount(long startLabelId, int typeId, long endLabelId, long delta) { // no-op
        }

        @Override
        public void close() { // no-op
        }
    };

    /**
     * Increments (or decrements if delta is negative) the count for the node label token id.
     *
     * @param labelId node label token id.
     * @param delta   delta (positive or negative) to apply for the label.
     */
    void incrementNodeCount(long labelId, long delta);

    /**
     * Increments (or decrements if delta is negative) the count for the combination of the start/end labels and relationship type.
     *
     * @param startLabelId node label token id of start node of relationship.
     * @param typeId       relationship type token id of relationship.
     * @param endLabelId   node label token id of end node of relationship.
     * @param delta        delta (positive or negative) to apply for the label.
     */
    void incrementRelationshipCount(long startLabelId, int typeId, long endLabelId, long delta);

    /**
     * Closes this updater and ensures that counts are applied as well as no more deltas can be applied after closed.
     */
    @Override
    void close();
}
