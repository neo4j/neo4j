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
import java.util.List;
import org.neo4j.cursor.RawCursor;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.memory.EmptyMemoryTracker;

/**
 * Allows iteration over metadata for all log files that exist at the time of construction.
 * If a populated cache is provided, it will be used to avoid reading any log headers from disk. If no cache is
 * provided, or if the cache is empty at the time of construction, all required log headers will be read from disk.
 */
public class LogFilesMetadata implements RawCursor<LogFileMetadata, IOException> {
    private final LogsRepository logsRepository;
    private final List<LogHeader> cachedLogHeaders;
    private final long[] versions;
    private final boolean reversed;
    private int currentVersionIndex = -1;
    private LogFileMetadata nextMetadata = null;

    LogFilesMetadata(LogsRepository logsRepository) throws IOException {
        this(logsRepository, false);
    }

    LogFilesMetadata(LogsRepository logsRepository, boolean reversed) throws IOException {
        this(logsRepository, List.of(), reversed);
    }

    LogFilesMetadata(LogsRepository logsRepository, EnvelopedLogHeaderCache logHeaderCache) throws IOException {
        this(logsRepository, logHeaderCache, false);
    }

    LogFilesMetadata(LogsRepository logsRepository, EnvelopedLogHeaderCache logHeaderCache, boolean reversed)
            throws IOException {
        this(logsRepository, logHeaderCache.currentLogHeaders(reversed), reversed);
    }

    private LogFilesMetadata(LogsRepository logsRepository, List<LogHeader> cachedLogHeaders, boolean reversed)
            throws IOException {
        this.logsRepository = logsRepository;
        this.cachedLogHeaders = cachedLogHeaders;
        this.versions = cachedLogHeaders.isEmpty() ? logsRepository.logVersions(reversed) : null;
        this.reversed = reversed;
    }

    @Override
    public boolean next() throws IOException {
        if (hasNext()) {
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
    public void close() {
        // ignored
    }

    private boolean hasNext() {
        if (cachedLogHeaders.isEmpty()) {
            return versions.length != 0 && currentVersionIndex < versions.length;
        }
        return currentVersionIndex < cachedLogHeaders.size();
    }

    private void setNext() throws IOException {
        nextMetadata = null;
        currentVersionIndex++;
        if (hasNext()) {
            if (cachedLogHeaders.isEmpty()) {
                setNextByReading();
            } else {
                setNextFromCache();
            }
        }
    }

    private void setNextByReading() throws IOException {
        var version = versions[currentVersionIndex];
        try (var logChannel = logsRepository.openReadChannel(version)) {
            var logHeader = readLogHeader(logChannel.channel(), true, null, EmptyMemoryTracker.INSTANCE);
            if (logHeader != null) {
                nextMetadata = new LogFileMetadata(logHeader, version, logChannel.path());
            } else {
                if (reversed) {
                    setNext(); // keep iterating until we find non-preallocated file
                }
            }
        }
    }

    private void setNextFromCache() {
        var logHeader = cachedLogHeaders.get(currentVersionIndex);
        long currentVersion = logHeader.getLogVersion();
        nextMetadata = new LogFileMetadata(logHeader, currentVersion, logsRepository.pathFor(currentVersion));
    }
}
