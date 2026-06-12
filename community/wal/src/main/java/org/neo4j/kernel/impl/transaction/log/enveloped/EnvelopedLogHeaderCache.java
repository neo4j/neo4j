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

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;

public class EnvelopedLogHeaderCache {

    private final List<LogHeader> logHeaders = new ArrayList<>();

    public synchronized void cache(LogHeader logHeader) {
        Objects.requireNonNull(logHeader);
        if (logHeaders.isEmpty()) {
            logHeaders.add(logHeader);
            return;
        }
        long newVersion = logHeader.getLogVersion();
        long nextVersion = logHeaders.getFirst().getLogVersion() + logHeaders.size();
        if (newVersion == nextVersion) {
            logHeaders.add(logHeader);
            return;
        }
        if (newVersion > nextVersion) {
            throw new IllegalArgumentException(String.format(
                    "Log versions are not contiguous: newVersion %d, currentEndVersion %d",
                    newVersion, nextVersion - 1));
        }
        long currentStartVersion = logHeaders.getFirst().getLogVersion();
        long previousVersion = currentStartVersion - 1;
        if (newVersion == previousVersion) {
            logHeaders.addFirst(logHeader);
            return;
        }
        if (newVersion < previousVersion) {
            throw new IllegalArgumentException(String.format(
                    "Log versions are not contiguous: newVersion %d, currentStartVersion %d",
                    newVersion, currentStartVersion));
        }
        logHeaders.set((int) (newVersion - currentStartVersion), logHeader);
    }

    public synchronized Optional<LogHeader> getFirst() {
        if (logHeaders.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(logHeaders.getFirst());
    }

    public synchronized Optional<LogHeader> getLast() {
        if (logHeaders.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(logHeaders.getLast());
    }

    public synchronized LongRange logVersionsRange() {
        if (logHeaders.isEmpty()) {
            return LongRange.EMPTY_RANGE;
        }
        return LongRange.range(
                logHeaders.getFirst().getLogVersion(), logHeaders.getLast().getLogVersion());
    }

    public synchronized LogHeader get(long version) {
        if (logHeaders.isEmpty()) {
            throw new NoSuchElementException(
                    String.format("Log version %d not found in cache: cache is empty", version));
        }
        var logHeader = get(logHeaders, version);
        if (logHeader == null) {
            long currentStartVersion = logHeaders.getFirst().getLogVersion();
            throw new NoSuchElementException(String.format(
                    "Log version %d not found in cache: available range is [%d, %d]",
                    version, currentStartVersion, currentStartVersion + logHeaders.size() - 1));
        }
        return logHeader;
    }

    static LogHeader get(List<LogHeader> logHeaders, long version) {
        long index = version - logHeaders.getFirst().getLogVersion();
        if (index < 0 || index >= logHeaders.size()) {
            return null;
        }
        return logHeaders.get((int) index);
    }

    public synchronized void forEachLogHeader(Consumer<LogHeader> logHeaderConsumer) {
        logHeaders.forEach(logHeaderConsumer);
    }

    public synchronized void clear() {
        logHeaders.clear();
    }

    public synchronized void deleteTo(long version) {
        if (logHeaders.isEmpty()) {
            return;
        }
        long currentStartVersion = logHeaders.getFirst().getLogVersion();
        if (version < currentStartVersion) {
            return;
        }
        if (version >= currentStartVersion + logHeaders.size() - 1) {
            logHeaders.clear();
            return;
        }
        logHeaders.subList(0, (int) (version - currentStartVersion + 1)).clear();
    }

    public synchronized void deleteFrom(long version) {
        if (logHeaders.isEmpty()) {
            return;
        }
        long currentStartVersion = logHeaders.getFirst().getLogVersion();
        if (version <= currentStartVersion) {
            logHeaders.clear();
            return;
        }
        if (version >= currentStartVersion + logHeaders.size()) {
            return;
        }
        logHeaders
                .subList((int) (version - currentStartVersion), logHeaders.size())
                .clear();
    }

    public synchronized boolean isEmpty() {
        return logHeaders.isEmpty();
    }

    synchronized List<LogHeader> currentLogHeaders(boolean reversed) {
        var copy = List.copyOf(logHeaders);
        return reversed ? copy.reversed() : copy;
    }

    @Override
    public String toString() {
        return "EnvelopedLogHeaderCache{logVersionsRange=" + logVersionsRange() + '}';
    }

    public LogBinarySearch.BinarySearchReader binarySearchReader() {
        return new LogCacheBinarySearch(currentLogHeaders(false));
    }

    public static class LogCacheBinarySearch implements LogBinarySearch.BinarySearchReader {

        private final List<LogHeader> logHeaders;

        LogCacheBinarySearch(List<LogHeader> logHeaders) {
            this.logHeaders = logHeaders;
        }

        @Override
        public int size() {
            return logHeaders.size();
        }

        @Override
        public long get(int index) {
            if (index < 0 || index >= logHeaders.size()) {
                return -1;
            }
            return logHeaders.get(index).getLogVersion();
        }

        @Override
        public int compare(long version, long targetEntryIndex) {
            if (logHeaders.isEmpty()) {
                return 1;
            }
            var logHeader = EnvelopedLogHeaderCache.get(logHeaders, version);
            if (logHeader != null) {
                var lastAppendIndex = logHeader.getLastAppendIndex();
                if (lastAppendIndex < targetEntryIndex) {
                    return -1;
                } else {
                    return 1;
                }
            }
            return 1;
        }
    }
}
