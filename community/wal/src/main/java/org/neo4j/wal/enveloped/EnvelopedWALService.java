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
package org.neo4j.wal.enveloped;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.io.fs.ChannelNativeAccessor;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.LogTailMetadata;
import org.neo4j.wal.LogTracers;
import org.neo4j.wal.WALService;
import org.neo4j.wal.checkpoint.CheckpointFile;
import org.neo4j.wal.pruning.LogPruneThreshold;

public class EnvelopedWALService implements WALService {
    private final EnvelopedLogFiles delegate;

    public EnvelopedWALService(
            LogsRepository logsRepository,
            LogHeaderFactory logHeaderFactory,
            int segmentBlockSize,
            int writerBufferedBlocks,
            int totalSegments,
            MemoryTracker memoryTracker,
            LogPruneThreshold pruneThreshold,
            ChannelNativeAccessor channelNativeAccessor,
            InternalLogProvider logProvider,
            LogTracers logTracers) {
        this(
                logsRepository,
                logHeaderFactory,
                segmentBlockSize,
                writerBufferedBlocks,
                totalSegments,
                memoryTracker,
                pruneThreshold,
                channelNativeAccessor,
                logProvider,
                logTracers,
                new EnvelopedLogHeaderCacheImpl());
    }

    public EnvelopedWALService(
            LogsRepository logsRepository,
            LogHeaderFactory logHeaderFactory,
            int segmentBlockSize,
            int writerBufferedBlocks,
            int totalSegments,
            MemoryTracker memoryTracker,
            LogPruneThreshold pruneThreshold,
            ChannelNativeAccessor channelNativeAccessor,
            InternalLogProvider logProvider) {
        this(
                logsRepository,
                logHeaderFactory,
                segmentBlockSize,
                writerBufferedBlocks,
                totalSegments,
                memoryTracker,
                pruneThreshold,
                channelNativeAccessor,
                logProvider,
                LogTracers.NULL, // TODO MERGELOGS LogTracers currently only used in tests
                new EnvelopedLogHeaderCacheImpl());
    }

    public EnvelopedWALService(
            LogsRepository logsRepository,
            LogHeaderFactory logHeaderFactory,
            int segmentBlockSize,
            int writerBufferedBlocks,
            int totalSegments,
            MemoryTracker memoryTracker,
            LogPruneThreshold pruneThreshold,
            ChannelNativeAccessor channelNativeAccessor,
            InternalLogProvider logProvider,
            LogTracers logTracers,
            EnvelopedLogHeaderCache logHeaderCache) {
        this.delegate = new EnvelopedLogFiles(
                logsRepository,
                logHeaderFactory,
                segmentBlockSize,
                writerBufferedBlocks,
                totalSegments,
                memoryTracker,
                pruneThreshold,
                channelNativeAccessor,
                logProvider,
                logTracers,
                logHeaderCache);
    }

    @Override
    public CheckpointFile getCheckpointFile() {
        throw new UnsupportedOperationException("Not for enveloped log files");
    }

    @Override
    public LogFile getLogFile() {
        return delegate.getLogFile();
    }

    @Override
    public Path logFilesDirectory() {
        throw new UnsupportedOperationException("Not for enveloped log files");
    }

    @Override
    public LogMetadataProvider logMetadataProvider() {
        throw new UnsupportedOperationException("Not for enveloped log files");
    }

    @Override
    public Path[] logFiles() throws IOException {
        throw new UnsupportedOperationException("Not for enveloped log files");
    }

    @Override
    public boolean isLogFile(Path path) {
        throw new UnsupportedOperationException("Not for enveloped log files");
    }

    @Override
    public LogTailMetadata getTailMetadata() {
        throw new UnsupportedOperationException("Not for enveloped log files");
    }

    @Override
    public EnvelopeWriteChannel truncate(long fromIndex) throws IOException {
        return delegate.truncate(fromIndex);
    }

    @Override
    public long prune(long safeIndex) throws IOException {
        return delegate.prune(safeIndex);
    }

    @Override
    public void clearCache() {
        delegate.clearCache();
    }

    @Override
    public void populateCache() throws IOException {
        delegate.populateCache();
    }

    @Override
    public LogFilesMetadata logFilesMetadata() throws IOException {
        return delegate.logFilesMetadata();
    }

    @Override
    public LogFilesMetadata logFilesMetadata(boolean reversed) throws IOException {
        return delegate.logFilesMetadata(reversed);
    }

    @Override
    public EnvelopeReadChannel openReadChannel() throws IOException {
        return delegate.openReadChannel();
    }

    @Override
    public EnvelopeReadChannel openReadChannel(long fileWithIndex) throws IOException {
        return delegate.openReadChannel(fileWithIndex);
    }

    @Override
    public void remove() throws IOException {
        delegate.remove();
    }

    @Override
    public void setPruneThreshold(LogPruneThreshold logPruneThreshold) {
        delegate.setPruneThreshold(logPruneThreshold);
    }

    @Override
    public StoreChannelsForTransfer entryStreamChannels(long fromIndex, long toIndex) throws IOException {
        return delegate.entryStreamChannels(fromIndex, toIndex);
    }

    @Override
    public long initialise() throws IOException {
        return delegate.initialise();
    }

    @Override
    public void forceRotate() throws IOException {
        delegate.forceRotate();
    }

    @Override
    public LogHeaderFactory headerFactory() {
        return delegate.headerFactory();
    }

    @Override
    public void init() throws Exception {
        delegate.initialise();
    }

    @Override
    public void start() throws Exception {}

    @Override
    public void stop() throws Exception {}

    @Override
    public void shutdown() throws Exception {
        delegate.close();
    }
}
