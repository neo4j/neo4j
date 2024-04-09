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
package org.neo4j.test;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Predicate;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadUtils;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;

/**
 * Utility for reading and filtering logical logs as well as tx logs.
 */
public final class LogTestUtils {
    private LogTestUtils() {}

    public interface LogHook<RECORD> extends Predicate<RECORD> {
        void file(Path file);

        void done(Path file);
    }

    public abstract static class LogHookAdapter<RECORD> implements LogHook<RECORD> {
        @Override
        public void file(Path file) { // Do nothing
        }

        @Override
        public void done(Path file) { // Do nothing
        }
    }

    public static class CountingLogHook<RECORD> extends LogHookAdapter<RECORD> {
        private int count;

        @Override
        public boolean test(RECORD item) {
            count++;
            return true;
        }

        public int getCount() {
            return count;
        }
    }

    public static Path[] filterNeostoreLogicalLog(
            LogFiles logFiles, FileSystemAbstraction fileSystem, LogHook<LogEntry> filter) throws IOException {
        Path[] files = logFiles.logFiles();
        for (Path file : files) {
            filterTransactionLogFile(fileSystem, file, filter);
        }

        return files;
    }

    private static void filterTransactionLogFile(
            FileSystemAbstraction fileSystem, Path file, final LogHook<LogEntry> filter) throws IOException {
        filter.file(file);
        try (StoreChannel in = fileSystem.read(file)) {
            LogHeader logHeader = readLogHeader(in, true, file, INSTANCE);
            assert logHeader != null : "Looks like we tried to read a log header of an empty pre-allocated file.";
            var inChannel = new PhysicalLogVersionedStoreChannel(
                    in, logHeader, file, ChannelNativeAccessor.EMPTY_ACCESSOR, DatabaseTracer.NULL);
            ReadableLogChannel inBuffer = ReadAheadUtils.newChannel(inChannel, logHeader, INSTANCE);
            LogEntryReader entryReader =
                    new VersionAwareLogEntryReader(TestCommandReaderFactory.INSTANCE, LatestVersions.BINARY_VERSIONS);

            LogEntry entry;
            while ((entry = entryReader.readLogEntry(inBuffer)) != null) {
                filter.test(entry);
            }
        }
    }
}
