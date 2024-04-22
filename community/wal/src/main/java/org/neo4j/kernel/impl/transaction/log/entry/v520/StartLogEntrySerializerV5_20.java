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
package org.neo4j.kernel.impl.transaction.log.entry.v520;

import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializer;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes;
import org.neo4j.storageengine.api.CommandReaderFactory;

public class StartLogEntrySerializerV5_20 extends LogEntrySerializer<LogEntryStartV5_20> {
    public StartLogEntrySerializerV5_20() {
        super(LogEntryTypeCodes.TX_START);
    }

    @Override
    public LogEntryStartV5_20 parse(
            KernelVersion version,
            ReadableChannel channel,
            LogPositionMarker marker,
            CommandReaderFactory commandReaderFactory)
            throws IOException {
        LogPosition position = marker.newPosition();
        long timeWritten = channel.getLong();
        long latestCommittedTxWhenStarted = channel.getLong();
        int previousChecksum = channel.getInt();
        long appendIndex = channel.getLong();
        int additionalHeaderLength = channel.getInt();
        byte[] additionalHeader = new byte[additionalHeaderLength];
        channel.get(additionalHeader, additionalHeaderLength);
        return new LogEntryStartV5_20(
                version,
                timeWritten,
                latestCommittedTxWhenStarted,
                appendIndex,
                previousChecksum,
                additionalHeader,
                position);
    }

    @Override
    public int write(WritableChannel channel, LogEntryStartV5_20 logEntry) throws IOException {
        channel.beginChecksumForWriting();
        writeLogEntryHeader(logEntry.kernelVersion(), TX_START, channel);
        byte[] additionalHeaderData = logEntry.getAdditionalHeader();
        channel.putLong(logEntry.getTimeWritten())
                .putLong(logEntry.getLastCommittedTxWhenTransactionStarted())
                .putInt(logEntry.getPreviousChecksum())
                .putLong(logEntry.getAppendIndex())
                .putInt(additionalHeaderData.length)
                .put(additionalHeaderData, additionalHeaderData.length);
        return NO_RETURN_VALUE;
    }
}
