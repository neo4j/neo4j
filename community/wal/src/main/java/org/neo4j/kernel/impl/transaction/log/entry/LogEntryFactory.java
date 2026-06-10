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
package org.neo4j.kernel.impl.transaction.log.entry;

import static org.neo4j.kernel.KernelVersion.V2025_05;
import static org.neo4j.kernel.KernelVersion.VERSION_APPEND_INDEX_INTRODUCED;
import static org.neo4j.kernel.KernelVersion.VERSION_MERGED_LOG_INFO_IN_START_ENTRIES;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.entry.v202505.LogEntryStartV2025_05;
import org.neo4j.kernel.impl.transaction.log.entry.v202606.LogEntryStartV2026_06;
import org.neo4j.kernel.impl.transaction.log.entry.v42.LogEntryCommitV4_2;
import org.neo4j.kernel.impl.transaction.log.entry.v42.LogEntryStartV4_2;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryChunkStart;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryRollback;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryStartV5_20;

public final class LogEntryFactory {
    private LogEntryFactory() {}

    public static LogEntryStart newStartEntry(
            KernelVersion version,
            long timeWritten,
            long lastCommittedTxWhenTransactionStarted,
            long appendIndex,
            long transactionSequenceNumber,
            int previousChecksum,
            byte[] additionalHeader) {
        if (version.isAtLeast(VERSION_MERGED_LOG_INFO_IN_START_ENTRIES)) {
            return new LogEntryStartV2026_06(
                    version,
                    timeWritten,
                    lastCommittedTxWhenTransactionStarted,
                    appendIndex,
                    transactionSequenceNumber,
                    additionalHeader);
        }
        if (version.isAtLeast(V2025_05)) {
            return new LogEntryStartV2025_05(
                    version, timeWritten, lastCommittedTxWhenTransactionStarted, appendIndex, additionalHeader);
        }
        if (version.isAtLeast(VERSION_APPEND_INDEX_INTRODUCED)) {
            return new LogEntryStartV5_20(
                    version,
                    timeWritten,
                    lastCommittedTxWhenTransactionStarted,
                    appendIndex,
                    previousChecksum,
                    additionalHeader);
        }
        return new LogEntryStartV4_2(
                version, timeWritten, lastCommittedTxWhenTransactionStarted, previousChecksum, additionalHeader);
    }

    public static LogEntryCommit newCommitEntry(KernelVersion version, long txId, long timeWritten, int checksum) {
        return new LogEntryCommitV4_2(version, txId, timeWritten, checksum);
    }

    public static AbstractVersionAwareLogEntry newRollbackEntry(
            KernelVersion kernelVersion,
            long transactionId,
            long appendIndex,
            long chunkId,
            long timeWritten,
            long transactionSequenceNumber,
            long lastBatchAppendIndex) {
        return new LogEntryRollback(
                kernelVersion,
                transactionId,
                appendIndex,
                chunkId,
                timeWritten,
                0,
                transactionSequenceNumber,
                lastBatchAppendIndex);
    }

    public static LogEntry newChunkStartEntry(
            KernelVersion kernelVersion,
            long timeWritten,
            long chunkId,
            long appendIndex,
            long previousBatchAppendIndex,
            long transactionSequenceNumber,
            byte[] additionalHeader) {
        return new LogEntryChunkStart(
                kernelVersion,
                timeWritten,
                chunkId,
                appendIndex,
                previousBatchAppendIndex,
                transactionSequenceNumber,
                additionalHeader);
    }
}
