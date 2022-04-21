/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.index.internal.gbptree;

import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.util.VisibleForTesting;

/**
 * Simplistic latch service which uses a {@link ConcurrentHashMap} to keep active latches.
 */
public class TreeNodeLatchService {
    private final ConcurrentHashMap<Long, LongSpinLatch> latches = new ConcurrentHashMap<>();

    /**
     * Acquires a read latch for the {@code treeNodeId} and returns a {@link LongSpinLatch} instance which can further manipulate
     * that latch, and release it.
     * @param treeNodeId tree node id to acquire read latch for.
     * @return the latch for the tree node id.
     */
    LongSpinLatch acquireRead(long treeNodeId) {
        while (true) {
            LongSpinLatch latch = latches.computeIfAbsent(treeNodeId, id -> new LongSpinLatch(id, latches::remove));
            if (latch.acquireRead() > 0) {
                return latch;
            }
        }
    }

    /**
     * Acquires a write latch for the {@code treeNodeId} and returns a {@link LongSpinLatch} instance which can further manipulate
     * that latch, and release it.
     * @param treeNodeId tree node id to acquire write latch for.
     * @return the latch for the tree node id.
     */
    LongSpinLatch acquireWrite(long treeNodeId) {
        while (true) {
            LongSpinLatch latch = latches.computeIfAbsent(treeNodeId, id -> new LongSpinLatch(id, latches::remove));
            if (latch.acquireWrite()) {
                return latch;
            }
        }
    }

    @VisibleForTesting
    int size() {
        return latches.size();
    }
}
