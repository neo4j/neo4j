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
package org.neo4j.wal;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.wal.checkpoint.CheckpointFile;
import org.neo4j.wal.enveloped.EnvelopeReadChannel;
import org.neo4j.wal.enveloped.EnvelopeWriteChannel;
import org.neo4j.wal.enveloped.LogFilesMetadata;
import org.neo4j.wal.enveloped.LogHeaderFactory;
import org.neo4j.wal.enveloped.StoreChannelsForTransfer;
import org.neo4j.wal.pruning.LogPruneThreshold;

/**
 * Main point of access to write-ahead logs.
 * Provide access to low level file based operations, log file headers, {@link LogFile}
 * and {@link PhysicalLogVersionedStoreChannel}
 */
public interface WALService extends Lifecycle {
    int MINIMUM_SEGMENTS = 2;

    CheckpointFile getCheckpointFile();

    LogFile getLogFile();

    Path logFilesDirectory();

    // Not available if full logfiles have not been built
    LogMetadataProvider logMetadataProvider();

    Path[] logFiles() throws IOException;

    boolean isLogFile(Path path);

    LogTailMetadata getTailMetadata();

    EnvelopeWriteChannel truncate(long fromIndex) throws IOException;

    long prune(long safeIndex) throws IOException;

    void clearCache();

    void populateCache() throws IOException;

    LogFilesMetadata logFilesMetadata() throws IOException;

    LogFilesMetadata logFilesMetadata(boolean reversed) throws IOException;

    EnvelopeReadChannel openReadChannel() throws IOException;

    EnvelopeReadChannel openReadChannel(long fileWithIndex) throws IOException;

    void remove() throws IOException; // ???

    void setPruneThreshold(LogPruneThreshold logPruneThreshold);

    StoreChannelsForTransfer entryStreamChannels(long fromIndex, long toIndex) throws IOException;

    long initialise() throws IOException;

    void forceRotate() throws IOException;

    LogHeaderFactory headerFactory();
}
