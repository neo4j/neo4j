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

import static org.neo4j.kernel.KernelVersion.VERSION_APPEND_INDEX_INTRODUCED;
import static org.neo4j.kernel.KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_INTRODUCED;

import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.v42.LogEntryCommitV4_2;
import org.neo4j.kernel.impl.transaction.log.entry.v42.LogEntryStartV4_2;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryChunkStartV5_20;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryRollbackV5_20;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryStartV5_20;

public final class LogEntryFactory {
    private LogEntryFactory() {}

    public static LogEntryStart newStartEntry(
            KernelVersion version,
            long timeWritten,
            long lastCommittedTxWhenTransactionStarted,
            long appendIndex,
            int previousChecksum,
            byte[] additionalHeader,
            LogPosition startPosition) {
        if (version.isAtLeast(VERSION_ENVELOPED_TRANSACTION_LOGS_INTRODUCED)) {
            return new LogEntryStartV5_20(
                    version,
                    timeWritten,
                    lastCommittedTxWhenTransactionStarted,
                    appendIndex,
                    previousChecksum,
                    additionalHeader,
                    startPosition);
        }
        if (version.isAtLeast(VERSION_APPEND_INDEX_INTRODUCED)) {
            return new LogEntryStartV5_20(
                    version,
                    timeWritten,
                    lastCommittedTxWhenTransactionStarted,
                    appendIndex,
                    previousChecksum,
                    additionalHeader,
                    startPosition);
        }
        return new LogEntryStartV4_2(
                version,
                timeWritten,
                lastCommittedTxWhenTransactionStarted,
                previousChecksum,
                additionalHeader,
                startPosition);
    }

    public static LogEntryCommit newCommitEntry(KernelVersion version, long txId, long timeWritten, int checksum) {
        if (version.isAtLeast(VERSION_ENVELOPED_TRANSACTION_LOGS_INTRODUCED)) {
            return new LogEntryCommit(version, txId, timeWritten);
        }
        return new LogEntryCommitV4_2(version, txId, timeWritten, checksum);
    }

    public static AbstractVersionAwareLogEntry newRollbackEntry(
            KernelVersion kernelVersion, long transactionId, long appendIndex, long timeWritten) {
        return new LogEntryRollbackV5_20(kernelVersion, transactionId, appendIndex, timeWritten, 0);
    }

    public static LogEntry newChunkStartEntry(
            KernelVersion kernelVersion,
            long timeWritten,
            long chunkId,
            long appendIndex,
            long previousBatchAppendIndex) {
        return new LogEntryChunkStartV5_20(kernelVersion, timeWritten, chunkId, appendIndex, previousBatchAppendIndex);
    }
}
