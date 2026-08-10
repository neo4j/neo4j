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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.KernelVersion.VERSION_CHECKPOINT_NOT_COMPLETED_POSITION_INTRODUCED;
import static org.neo4j.kernel.KernelVersion.VERSION_CHECKPOINT_POWER_OF_2_IN_ENVELOPES;
import static org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent.NULL;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.time.Instant;
import java.util.function.IntConsumer;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogSegments;
import org.neo4j.kernel.impl.transaction.log.entry.v202608.DetachedCheckpointLogEntrySerializerV2026_08;
import org.neo4j.kernel.impl.transaction.log.entry.v520.DetachedCheckpointLogEntrySerializerV5_20;
import org.neo4j.kernel.impl.transaction.log.entry.v522.DetachedCheckpointLogEntrySerializerV5_22;
import org.neo4j.storageengine.api.TransactionId;

public class CheckpointLogSerializationHelper {
    static final long CONFIG_ROTATION_THRESHOLD = kibiBytes(1);

    // Checkpoint log files should always have space for at least 2 envelop segments, so even if the
    // threshold passed to the DBMS is very small we should be seeing bigger files.
    static final long ACTUAL_ROTATION_THRESHOLD = LogSegments.DEFAULT_LOG_SEGMENT_SIZE * 2L;

    static final String CHECKPOINT_REASON = "checkpoint for rotation test";
    static final LogPosition LOG_POSITION = new LogPosition(1000, 12345);
    static final TransactionId TRANSACTION_ID = new TransactionId(100, 101, LATEST_KERNEL_VERSION, 101, 102, 103);

    public static int getCheckpointRecordLengthBytes() {
        if (LATEST_KERNEL_VERSION.isAtLeast(VERSION_CHECKPOINT_POWER_OF_2_IN_ENVELOPES)) {
            return DetachedCheckpointLogEntrySerializerV2026_08.checkPointRecordSizeDependingOnVersion(
                    LATEST_LOG_FORMAT.usesSegments());
        } else if (LATEST_KERNEL_VERSION.isAtLeast(VERSION_CHECKPOINT_NOT_COMPLETED_POSITION_INTRODUCED)) {
            return DetachedCheckpointLogEntrySerializerV5_22.checkPointRecordSizeDependingOnVersion(
                    LATEST_LOG_FORMAT.usesSegments());
        }
        return DetachedCheckpointLogEntrySerializerV5_20.RECORD_LENGTH_BYTES;
    }

    public static int getExpectedPositionAfterOneCheckpoint() {
        return (int) LATEST_LOG_FORMAT.getDefaultDataStartByteOffset() + getCheckpointRecordLengthBytes();
    }

    public static long getMaxCheckpointFileSize() {
        if (LATEST_LOG_FORMAT.usesSegments()) {
            return ACTUAL_ROTATION_THRESHOLD;
        }
        return ACTUAL_ROTATION_THRESHOLD + getCheckpointRecordLengthBytes() - 1L;
    }

    /**
     * This method create enough checkpoint entries to fill {@param files} checkpoint files.
     */
    public static void fillWithCheckpoints(int files, CheckpointAppender appender) throws IOException {
        fillWithCheckpointsWithCallback(files, appender, i -> {});
    }

    public static void fillWithCheckpointsWithCallback(int files, CheckpointAppender appender, IntConsumer afterAppend)
            throws IOException {
        DetachedCheckpointAppender detached = (DetachedCheckpointAppender) appender;
        for (int fileCount = 0; fileCount < files; fileCount++) {
            var fullyFilledFileSize = LATEST_LOG_FORMAT.usesSegments()
                    ? ACTUAL_ROTATION_THRESHOLD - LogEnvelopeHeader.HEADER_SIZE
                    : ACTUAL_ROTATION_THRESHOLD;
            do {
                appender.checkPoint(
                        NULL,
                        TRANSACTION_ID,
                        TRANSACTION_ID.id() + 77,
                        LATEST_KERNEL_VERSION,
                        LOG_POSITION,
                        LOG_POSITION,
                        Instant.now(),
                        CHECKPOINT_REASON);
                afterAppend.accept(fileCount);
            } while (detached.getCurrentPosition() < fullyFilledFileSize);
        }
    }

    public static long expectedNewCheckpointFileSize() {
        return LATEST_LOG_FORMAT.getDefaultDataStartByteOffset() + getCheckpointRecordLengthBytes();
    }
}
