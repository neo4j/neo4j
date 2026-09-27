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
package org.neo4j.kernel.impl.store.segment;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.neo4j.kernel.impl.store.segment.SegmentsMetadata.EMPTY_SEGMENTS_METADATA;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.segment.DatabaseSegmentTracker;
import org.neo4j.logging.InternalLog;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.StoreId;

public class SegmentedStoreMetadataService implements SegmentMetadataService {

    private static final long MAGIC = 0xCAFE_0FF1CEL;
    private static final int FORMAT_VERSION = 1;

    private final FileSystemAbstraction fs;
    private final DatabaseLayout databaseLayout;
    private final DatabaseSegmentTracker segmentTracker;
    private final StoreId storeId;
    private final MemoryTracker memoryTracker;
    private final DatabaseHealth databaseHealth;
    private final InternalLog log;

    public SegmentedStoreMetadataService(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            StoreId storeId,
            DatabaseSegmentTracker segmentTracker,
            MemoryTracker memoryTracker,
            DatabaseHealth databaseHealth,
            InternalLog log) {
        this.fs = fs;
        this.databaseLayout = databaseLayout;
        this.segmentTracker = segmentTracker;
        this.storeId = storeId;
        this.memoryTracker = memoryTracker;
        this.databaseHealth = databaseHealth;
        this.log = log;
    }

    @Override
    public void storeMetadata(long checkpointAppendIndex) {
        try {
            persist(load().combine(checkpointAppendIndex, segmentTracker.segmentMetadata()));
        } catch (Exception e) {
            log.warn("Error on store metadata update. ", e);
            try {
                deleteMetadataFile();
            } catch (IOException ex) {
                log.warn("Fail to cleanup legacy segment file. ", e);
                databaseHealth.panic(e);
            }
        }
    }

    @Override
    public SegmentsMetadata segmentsMetadata() {
        return load();
    }

    @Override
    public boolean trackedSegmentedStore() {
        return true;
    }

    private SegmentsMetadata load() {
        Path metadataFile = databaseLayout.segmentsMetadata();
        if (!fs.fileExists(metadataFile)) {
            return EMPTY_SEGMENTS_METADATA;
        }
        try (var storeChannel = fs.read(metadataFile);
                var channel = new ReadAheadChannel<>(storeChannel, memoryTracker)) {
            channel.beginChecksum();
            long magic = channel.getLong();
            if (magic != MAGIC) {
                throw new IOException("Unexpected segments metadata magic: " + magic);
            }
            int formatVersion = channel.getInt();
            if (formatVersion != FORMAT_VERSION) {
                throw new IOException("Unsupported segments metadata format version: " + formatVersion);
            }
            StoreId metadataStoreId = StoreId.deserialize(channel);
            if (!storeId.equals(metadataStoreId)) {
                throw new IOException(
                        "Segments metadata store id " + metadataStoreId + " does not match store id " + storeId);
            }
            SegmentsMetadata metadata = SegmentsMetadataCodec.read(channel);
            channel.endChecksumAndValidate();
            return metadata;
        } catch (Exception e) {
            log.warn(
                    "Error on loading of segments metadata file: " + metadataFile + ". Empty metadata will be used.",
                    e);
            return EMPTY_SEGMENTS_METADATA;
        }
    }

    private void deleteMetadataFile() throws IOException {
        fs.deleteFile(databaseLayout.segmentsMetadata());
    }

    private void persist(SegmentsMetadata metadata) throws IOException {
        Path segmentsMetadata = databaseLayout.segmentsMetadata();
        Path tempFile = tempMetadataFile(segmentsMetadata);
        if (fs.fileExists(tempFile)) {
            fs.deleteFile(tempFile);
        }
        try (var storeChannel = fs.write(tempFile)) {
            try (var channel = new PhysicalFlushableChannel(storeChannel, memoryTracker)) {
                channel.beginChecksumForWriting();
                channel.putLong(MAGIC);
                channel.putInt(FORMAT_VERSION);
                storeId.serialize(channel);
                SegmentsMetadataCodec.write(channel, metadata);
                channel.putChecksum();
                channel.prepareForFlush().flush();
                storeChannel.force(false);
            }
        }
        fs.renameFile(tempFile, segmentsMetadata, ATOMIC_MOVE, REPLACE_EXISTING);
    }

    private static Path tempMetadataFile(Path segmentsMetadata) {
        return segmentsMetadata.resolveSibling(segmentsMetadata.getFileName() + ".tmp");
    }
}
