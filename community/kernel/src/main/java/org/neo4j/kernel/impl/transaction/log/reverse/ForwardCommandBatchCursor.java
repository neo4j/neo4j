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
package org.neo4j.kernel.impl.transaction.log.reverse;

import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.CommittedCommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.IncompleteLogHeaderException;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;

/**
 * Handles log format changes between files by reading the files one at the time in increasing order.
 */
public final class ForwardCommandBatchCursor implements CommandBatchCursor {

    private final LogFile logFile;
    private final LogEntryReader reader;
    private LogPosition beginning;
    private final FileSystemAbstraction fs;
    private long nextVersion;
    private CommandBatchCursor currentLogCommandBatchCursor;
    private LogPosition lastReasonableLogPosition;

    public ForwardCommandBatchCursor(
            LogFile logFile, LogPosition beginning, LogEntryReader reader, FileSystemAbstraction fs)
            throws IOException {
        this.logFile = logFile;
        this.reader = reader;
        this.beginning = beginning;
        this.fs = fs;
        this.currentLogCommandBatchCursor =
                new CommittedCommandBatchCursor(logFile.getReader(beginning, NO_MORE_CHANNELS), reader);
        this.lastReasonableLogPosition = currentLogCommandBatchCursor.position();
        this.nextVersion = beginning.getLogVersion() + 1;
    }

    private CommandBatchCursor internalNext() {
        try {
            ReadableLogChannel logChannel;
            try {
                beginning = getCursorStartPosition();
                logChannel = logFile.getReader(beginning, NO_MORE_CHANNELS);
            } catch (NoSuchFileException | IncompleteLogHeaderException e) {
                return currentLogCommandBatchCursor;
            }
            CommandBatchCursor cursor = new CommittedCommandBatchCursor(logChannel, reader);
            nextVersion++;
            return cursor;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean next() throws IOException {
        while (!currentLogCommandBatchCursor.next()) {
            if (currentLogCommandBatchCursor.position().getByteOffset()
                    != fs.getFileSize(logFile.getLogFileForVersion(nextVersion - 1))) {
                // Did not read to end, must have encountered weirdness, let's say that we have nothing left even if
                // there might be newer files.
                return false;
            }

            var cursor = internalNext();
            if (cursor == currentLogCommandBatchCursor) {
                // Did not find any more channels
                return false;
            }
            // Need to keep track of the last good position because even if we find another file it might only have
            // incomplete things
            lastReasonableLogPosition =
                    currentLogCommandBatchCursor != null ? currentLogCommandBatchCursor.position() : beginning;
            closeCurrent();
            currentLogCommandBatchCursor = cursor;
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        closeCurrent();
    }

    private void closeCurrent() throws IOException {
        if (currentLogCommandBatchCursor != null) {
            currentLogCommandBatchCursor.close();
            currentLogCommandBatchCursor = null;
        }
    }

    private LogPosition getCursorStartPosition() throws IOException {
        LogHeader logHeader = logFile.extractHeader(nextVersion);
        return logHeader != null ? logHeader.getStartPosition() : new LogPosition(nextVersion, 0);
    }

    @Override
    public LogPosition position() {
        if (beginning.equals(currentLogCommandBatchCursor.position())) {
            return lastReasonableLogPosition;
        }
        return currentLogCommandBatchCursor.position();
    }

    @Override
    public CommittedCommandBatch get() {
        return currentLogCommandBatchCursor.get();
    }
}
