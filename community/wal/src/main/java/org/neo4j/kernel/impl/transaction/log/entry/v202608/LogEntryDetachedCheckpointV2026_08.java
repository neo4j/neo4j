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
package org.neo4j.kernel.impl.transaction.log.entry.v202608;

import java.util.Objects;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.AbstractDetachedCheckpointLogEntry;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.string.Mask;

public class LogEntryDetachedCheckpointV2026_08 extends AbstractDetachedCheckpointLogEntry {
    private final TransactionId transactionId;
    private final long lastAppendIndex;
    private final LogPosition oldestNotCompletedPosition;
    private final boolean consensusIndexInCheckpoint;

    public LogEntryDetachedCheckpointV2026_08(
            KernelVersion kernelVersion,
            TransactionId transactionId,
            long lastAppendIndex,
            LogPosition oldestNotCompletedPosition,
            LogPosition checkpointedLogPosition,
            long checkpointMillis,
            StoreId storeId,
            String reason) {
        super(kernelVersion, checkpointedLogPosition, checkpointMillis, storeId, reason);
        this.transactionId = transactionId;
        this.oldestNotCompletedPosition = oldestNotCompletedPosition;
        this.lastAppendIndex = lastAppendIndex;
        this.consensusIndexInCheckpoint = true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogEntryDetachedCheckpointV2026_08 that = (LogEntryDetachedCheckpointV2026_08) o;
        return lastAppendIndex == that.lastAppendIndex
                && checkpointTime == that.checkpointTime
                && consensusIndexInCheckpoint == that.consensusIndexInCheckpoint
                && Objects.equals(transactionId, that.transactionId)
                && Objects.equals(oldestNotCompletedPosition, that.oldestNotCompletedPosition)
                && Objects.equals(checkpointedLogPosition, that.checkpointedLogPosition)
                && Objects.equals(storeId, that.storeId)
                && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                transactionId,
                lastAppendIndex,
                oldestNotCompletedPosition,
                checkpointedLogPosition,
                checkpointTime,
                storeId,
                reason,
                consensusIndexInCheckpoint);
    }

    public LogPosition getOldestNotCompletedPosition() {
        return oldestNotCompletedPosition;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public boolean consensusIndexInCheckpoint() {
        return consensusIndexInCheckpoint;
    }

    public long getLastAppendIndex() {
        return lastAppendIndex;
    }

    @Override
    public String toString(Mask mask) {
        return "LogEntryDetachedCheckpointV2026_08{" + "transactionId=" + transactionId + ", lastAppendIndex="
                + lastAppendIndex + ", oldestNotCompletedPosition="
                + oldestNotCompletedPosition + ", checkpointedLogPosition=" + checkpointedLogPosition
                + ", checkpointTime=" + checkpointTime
                + ", storeId=" + storeId + ", reason='" + reason + '\'' + ", consensusIndexInCheckpoint="
                + consensusIndexInCheckpoint + '}';
    }
}
