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
package org.neo4j.kernel.impl.transaction.log.entry.v202606;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.ReadableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryEmpty;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializer;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandReaderFactory;

public class EmptyLogEntrySerializerV2026_06 extends LogEntrySerializer<LogEntryEmpty> {
    public EmptyLogEntrySerializerV2026_06() {
        super(LogEntryTypeCodes.EMPTY_TX);
    }

    @Override
    public LogEntryEmpty parse(
            KernelVersion version,
            ReadableChannel channel,
            LogPositionMarker marker,
            CommandReaderFactory commandReaderFactory,
            MemoryTracker memoryTracker)
            throws IOException {
        if (channel instanceof ReadableLogPositionAwareChannel eChannel && eChannel.supportsEntrySkipping()) {
            byte contentType = eChannel.getContentType();
            long appendIndex = eChannel.getAppendIndex();
            eChannel.goToEndOfEntry();
            return new LogEntryEmpty(appendIndex, version, contentType);
        } else {
            throw new IllegalStateException(
                    "LogEntryEmpty should only have been injected for an envelope channel, or delegate to one, but channel was "
                            + channel);
        }
    }

    @Override
    public int write(WritableChannel channel, LogEntryEmpty logEntry) throws IOException {
        throw new IllegalStateException("LogEntryEmpty should never be written");
    }
}
