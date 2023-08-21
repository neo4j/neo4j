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
package org.neo4j.kernel.impl.transaction.log.entry.legacy;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.LEGACY_CHECK_POINT;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializer;
import org.neo4j.storageengine.api.CommandReaderFactory;

/**
 * Prior to neo4j 4.2, checkpoint entries was located inside the transaction log.
 * Since checkpoints are server specific, any replication setup would have to filter
 * out those when shipping the log files, imposing a performance cost. As of neo4j 4.2,
 * the checkpoint log entries lives in a separate file, allowing replication to ship the
 * raw transaction log files without any filtering.
 */
public class InlineCheckpointLogEntrySerializer extends LogEntrySerializer<LegacyInlinedCheckPoint> {

    private final boolean haveChecksum;

    public InlineCheckpointLogEntrySerializer(boolean haveChecksum) {
        super(LEGACY_CHECK_POINT);
        this.haveChecksum = haveChecksum;
    }

    @Override
    public LegacyInlinedCheckPoint parse(
            KernelVersion version,
            ReadableChannel channel,
            LogPositionMarker marker,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        long logVersion = channel.getLong();
        long byteOffset = channel.getLong();
        if (haveChecksum) {
            // checksums present in some of old versions. We do not need them here, so we ignore them.
            channel.getInt();
        }
        return new LegacyInlinedCheckPoint(new LogPosition(logVersion, byteOffset));
    }

    @Override
    public int write(WritableChannel channel, LegacyInlinedCheckPoint logEntry) {
        throw new UnsupportedOperationException("Unable to write inline checkpoints");
    }
}
