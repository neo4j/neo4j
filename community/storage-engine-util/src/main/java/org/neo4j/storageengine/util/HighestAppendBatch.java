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
package org.neo4j.storageengine.util;

import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.kernel.impl.transaction.log.AppendBatchInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;

/**
 * Can accept offerings about {@link AppendBatchInfo}, but will always only keep the highest one,
 * always available in {@link #get()}.
 */
public class HighestAppendBatch {
    private final AtomicReference<AppendBatchInfo> highest = new AtomicReference<>();

    public HighestAppendBatch(AppendBatchInfo appendBatchInfo) {
        highest.set(appendBatchInfo);
    }

    /**
     * Offers an append index. Will be accepted if this is higher than the current highest.
     * This method is thread-safe.
     */
    public void offer(long appendIndex, LogPosition logPositionAfter) {
        AppendBatchInfo high = highest.getAcquire();
        if (appendIndex < high.appendIndex()) { // a higher appendIndex has already been offered
            return;
        }

        AppendBatchInfo update = new AppendBatchInfo(appendIndex, logPositionAfter);
        while (!highest.weakCompareAndSetRelease(high, update)) {
            high = highest.getAcquire();
            // Someone else set a higher appendIndex while we were trying to set this appendIndex
            if (high.appendIndex() >= appendIndex) {
                return;
            }
        }
    }

    /**
     * Overrides the highest {@link AppendBatchInfo} value, no matter what it currently is. Used for initialization purposes.
     */
    public final void set(long appendIndex, LogPosition logPositionAfter) {
        highest.set(new AppendBatchInfo(appendIndex, logPositionAfter));
    }

    /**
     * @return the currently highest {@link AppendBatchInfo}
     */
    public AppendBatchInfo get() {
        return highest.get();
    }
}
