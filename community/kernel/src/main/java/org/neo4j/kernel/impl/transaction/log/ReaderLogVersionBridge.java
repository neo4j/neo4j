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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import org.neo4j.kernel.impl.transaction.log.entry.IncompleteLogHeaderException;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;

/**
 * {@link LogVersionBridge} naturally transitioning from one {@link LogVersionedStoreChannel} to the next,
 * i.e. to log version with one higher version than the current.
 */
public abstract class ReaderLogVersionBridge implements LogVersionBridge {

    /**
     *
     * @param logFile the transaction log file(s) to read over
     * @return a bridge that can return the chain of transaction files in version order
     */
    public static LogVersionBridge forFile(LogFile logFile) {
        return (channel, raw) -> swap(channel, version -> logFile.openForVersion(version, raw));
    }

    /**
     *
     * @param logFile the checkpoint log file(s) to read over
     * @return a bridge that can return the chain of checkpoint files in version order
     */
    public static LogVersionBridge forFile(CheckpointFile logFile) {
        return (channel, raw) -> swap(channel, logFile::openForVersion);
    }

    @FunctionalInterface
    private interface ChannelProvider {
        LogVersionedStoreChannel get(long version) throws IOException;
    }

    private static LogVersionedStoreChannel swap(
            LogVersionedStoreChannel prevChannel, ChannelProvider nextChannelProvider) throws IOException {
        LogVersionedStoreChannel nextChannel;
        try {
            nextChannel = nextChannelProvider.get(prevChannel.getLogVersion() + 1);
        } catch (NoSuchFileException | IncompleteLogHeaderException e) {
            // See TransactionLogFile#rotate() for description as to why these exceptions are OK
            return prevChannel;
        }
        prevChannel.close();
        return nextChannel;
    }
}
