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
package org.neo4j.internal.id.indexed;

import static org.neo4j.index.internal.gbptree.ValueMerger.MergeResult.MERGED;
import static org.neo4j.index.internal.gbptree.ValueMerger.MergeResult.REMOVED;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.NO_MONITOR;

import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.index.internal.gbptree.ValueMerger;

/**
 * Merges ID state changes for a particular tree entry. Differentiates between recovery/normal mode.
 * Updates to a tree entry of an older generation during normal mode will first normalize states before applying new changes.
 */
final class IdRangeMerger implements ValueMerger<IdRangeKey, IdRange> {
    public static final IdRangeMerger DEFAULT = new IdRangeMerger(false, NO_MONITOR, null);
    public static final IdRangeMerger RECOVERY = new IdRangeMerger(true, NO_MONITOR, null);

    private final boolean recoveryMode;
    private final IndexedIdGenerator.Monitor monitor;
    private final AtomicLong numUnusedIds;
    private int diffNumUnusedIds;

    IdRangeMerger(boolean recoveryMode, IndexedIdGenerator.Monitor monitor, AtomicLong numUnusedIds) {
        this.recoveryMode = recoveryMode;
        this.monitor = monitor;
        this.numUnusedIds = numUnusedIds;
    }

    @Override
    public MergeResult merge(IdRangeKey existingKey, IdRangeKey newKey, IdRange existingValue, IdRange newValue) {
        if (!recoveryMode && existingValue.getGeneration() != newValue.getGeneration()) {
            existingValue.normalize();
            existingValue.setGeneration(newValue.getGeneration());
            monitor.normalized(existingKey.getIdRangeIdx());
        }

        diffNumUnusedIds = existingValue.mergeFrom(existingKey, newValue, recoveryMode);
        return existingValue.isEmpty() ? REMOVED : MERGED;
    }

    @Override
    public void added(IdRangeKey newKey, IdRange newValue) {
        diffNumUnusedIds = newValue.numUnusedIdsForAdded();
    }

    @Override
    public void completed() {
        if (diffNumUnusedIds != 0) {
            if (numUnusedIds != null) {
                numUnusedIds.addAndGet(diffNumUnusedIds);
            }
            diffNumUnusedIds = 0;
        }
    }
}
