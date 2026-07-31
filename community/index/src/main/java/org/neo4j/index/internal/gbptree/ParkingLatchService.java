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
package org.neo4j.index.internal.gbptree;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.LongConsumer;
import org.neo4j.util.VisibleForTesting;

/**
 * {@link TreeNodeLatchService} handing out {@link ParkingLatch} instances.
 */
final class ParkingLatchService implements TreeNodeLatchService {
    private final ConcurrentHashMap<Long, ParkingLatch> latches = new ConcurrentHashMap<>();
    private final LongConsumer removeAction = latches::remove;
    private final Function<Long, ParkingLatch> newLatch = id -> new ParkingLatch(id, removeAction);

    @Override
    public ParkingLatch latch(long treeNodeId) {
        while (true) {
            var latch = latches.get(treeNodeId);
            if (latch == null) {
                latch = latches.computeIfAbsent(treeNodeId, newLatch);
            }
            if (latch.ref()) {
                return latch;
            }
            Thread.onSpinWait();
        }
    }

    @VisibleForTesting
    int size() {
        return latches.size();
    }
}
