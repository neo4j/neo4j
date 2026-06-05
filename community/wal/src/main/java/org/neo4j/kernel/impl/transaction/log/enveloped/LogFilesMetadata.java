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
package org.neo4j.kernel.impl.transaction.log.enveloped;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;

import java.io.IOException;
import org.neo4j.cursor.RawCursor;
import org.neo4j.memory.EmptyMemoryTracker;

public class LogFilesMetadata implements RawCursor<LogFileMetadata, IOException> {
    private final LogsRepository logsRepository;
    private final long[] versions;
    private int currentVersionIndex = -1;
    private LogFileMetadata nextMetadata = null;
    private final boolean reversed;

    LogFilesMetadata(LogsRepository logsRepository) throws IOException {
        this(logsRepository, false);
    }

    LogFilesMetadata(LogsRepository logsRepository, boolean reversed) throws IOException {
        this.logsRepository = logsRepository;
        this.versions = logsRepository.logVersions(reversed);
        this.reversed = reversed;
    }

    @Override
    public boolean next() throws IOException {
        if (versions.length != 0 && currentVersionIndex < versions.length) {
            setNext();
            return nextMetadata != null;
        }
        return false;
    }

    @Override
    public LogFileMetadata get() {
        return nextMetadata;
    }

    @Override
    public void close() throws IOException {
        // ignored
    }

    private void setNext() throws IOException {
        nextMetadata = null;
        if (++currentVersionIndex != versions.length) {
            var version = versions[currentVersionIndex];
            try (var logChannel = logsRepository.openReadChannel(version)) {
                var currentPath = logChannel.path();
                var logHeader = readLogHeader(logChannel.channel(), true, null, EmptyMemoryTracker.INSTANCE);
                if (logHeader != null) {
                    nextMetadata = new LogFileMetadata(
                            logHeader, version, currentPath, logsRepository.lastModifiedTime(version));
                } else if (reversed) {
                    setNext(); // skip pre-allocated empty file
                }
            }
        }
    }
}
