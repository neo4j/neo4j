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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.kernel.impl.transaction.log.StoreChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneThreshold;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;

public class EnvelopeLogFilesFactory {
    private final FileSystemAbstraction fileSystem;
    private final int defaultLogSegmentSize;
    private final int writeBufferBlocks;
    private final int segments;
    private final MemoryTracker memoryTracker;
    private final LogPruneThreshold pruneThreshold;
    private final StoreChannelNativeAccessor storeChannelNativeAccessor;
    private final InternalLogProvider logProvider;

    public EnvelopeLogFilesFactory(
            FileSystemAbstraction fileSystem,
            int defaultLogSegmentSize,
            int writeBufferBlocks,
            int segments,
            MemoryTracker memoryTracker,
            LogPruneThreshold pruneThreshold,
            StoreChannelNativeAccessor storeChannelNativeAccessor,
            InternalLogProvider logProvider) {
        this.fileSystem = fileSystem;
        this.defaultLogSegmentSize = defaultLogSegmentSize;
        this.writeBufferBlocks = writeBufferBlocks;
        this.segments = segments;
        this.memoryTracker = memoryTracker;
        this.pruneThreshold = pruneThreshold;
        this.storeChannelNativeAccessor = storeChannelNativeAccessor;
        this.logProvider = logProvider;
    }

    public EnvelopedLogFiles create(SequentialFileNameHelper fileNameHelper, LogHeaderFactory logHeaderFactory) {
        return new EnvelopedLogFiles(
                new LogsRepository(fileSystem, fileNameHelper),
                logHeaderFactory,
                defaultLogSegmentSize,
                writeBufferBlocks,
                segments,
                memoryTracker,
                pruneThreshold,
                storeChannelNativeAccessor,
                logProvider);
    }
}
