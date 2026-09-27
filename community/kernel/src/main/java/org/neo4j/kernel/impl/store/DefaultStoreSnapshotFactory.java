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
package org.neo4j.kernel.impl.store;

import static org.neo4j.kernel.impl.store.segment.SegmentsMetadata.EMPTY_SEGMENTS_METADATA;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.store.segment.SegmentMetadataService;
import org.neo4j.kernel.impl.store.segment.SegmentsMetadata;
import org.neo4j.logging.InternalLog;
import org.neo4j.storageengine.api.SegmentsFilter;
import org.neo4j.storageengine.api.StoreResource;
import org.neo4j.storageengine.api.StoreSnapshot;
import org.neo4j.wal.LogFiles;
import org.neo4j.wal.checkpoint.CheckPointer;
import org.neo4j.wal.checkpoint.SimpleTriggerInfo;
import org.neo4j.wal.entry.LogFormat;

public class DefaultStoreSnapshotFactory implements StoreSnapshot.Factory {
    private final Database database;
    private final FileSystemAbstraction fs;
    private final SegmentMetadataService segmentMetadataService;
    private final InternalLog log;

    public DefaultStoreSnapshotFactory(Database database, FileSystemAbstraction fs) {
        this(database, fs, SegmentMetadataService.EMPTY_METADATA_SERVICE);
    }

    public DefaultStoreSnapshotFactory(
            Database database, FileSystemAbstraction fs, SegmentMetadataService segmentMetadataService) {
        this.database = database;
        this.fs = fs;
        this.segmentMetadataService = segmentMetadataService;
        this.log = database.getInternalLogProvider().getLog(getClass());
    }

    @Override
    public Optional<StoreSnapshot> createStoreSnapshot() throws IOException {
        return createStoreSnapshot(EMPTY_SEGMENTS_METADATA);
    }

    public Optional<StoreSnapshot> createStoreSnapshot(SegmentsMetadata requestedSegmentState) throws IOException {
        log.debug("Starting creating store snapshot");
        if (!database.getDatabaseAvailabilityGuard().isAvailable()) {
            log.warn("Unable to prepare a store snapshot because database '"
                    + database.getNamedDatabaseId().name() + "' is unavailable");
            return Optional.empty();
        }

        // Checkpoint must happen before listing files, otherwise we might miss
        // updates that happen between listing and checkpoint since the listing
        // is a sort of point in time snapshot of the state.
        var checkPointer = database.getDependencyResolver().resolveDependency(CheckPointer.class);
        var checkpointMutex = tryCheckpointAndAcquireMutex(checkPointer);
        boolean success = false;
        Stream<StoreResource> unrecoverableFiles = null;
        try {
            var latestCheckpointInfo = checkPointer.latestCheckPointInfo();
            var kernelVersion = latestCheckpointInfo.kernelVersion();
            var lastCommittedTransactionId = latestCheckpointInfo.highestObservedClosedTransactionId();
            long appendIndex = latestCheckpointInfo.appendIndex();

            LogFiles logFiles = database.getDependencyResolver().resolveDependency(LogFiles.class);
            LogFormat logFormatAtCheckpoint = logFiles.getLogFile()
                    .extractHeader(
                            latestCheckpointInfo.checkpointedLogPosition().getLogVersion())
                    .getLogFormatVersion();

            unrecoverableFiles = unrecoverableFiles(database);
            var recoverableFiles = recoverableFiles(database, requestedSegmentState);
            var snapshot = new StoreSnapshot(
                    unrecoverableFiles,
                    recoverableFiles,
                    kernelVersion,
                    lastCommittedTransactionId,
                    appendIndex,
                    database.getStoreId(),
                    checkpointMutex,
                    logFormatAtCheckpoint);
            var result = Optional.of(snapshot);
            success = true;
            return result;
        } finally {
            if (!success) {
                IOUtils.closeAll(unrecoverableFiles, checkpointMutex);
            }
            log.debug("Creating store snapshot complete, success=%s", success);
        }
    }

    /**
     * Stream of store files whose contents are *not* updated via transactional commands.
     *
     * As these files may not be recovered later, they must be included in full in the {@link StoreSnapshot}.
     * We intentionally return an *un-closed* {@code Stream<StoreResource>}, which is then closed
     * as part of the wrapping StoreSnapshot when a caller is finished with it.
     */
    protected Stream<StoreResource> unrecoverableFiles(Database database) throws IOException {
        var databaseDirectory = database.getDatabaseLayout().databaseDirectory();
        return database
                .getStoreFileListing()
                .builder()
                .excludeAll()
                .includeAtomicStorageFiles()
                .includeAdditionalProviders()
                .includeSchemaIndexStoreFiles()
                .includeIdFiles()
                .build()
                .stream()
                .map(metadata -> toStoreResource(databaseDirectory, metadata));
    }

    /**
     * Collection of paths for store files whose contents are updated via transactional commands.
     *
     * These files may be reconstructed via recovery. As a result, they do not need to be included in full in a
     * {@link StoreSnapshot}. Instead, a {@code Path[]} is returned, so that subsequent requests may fetch each
     * store file, regardless of what state they may be in.
     */
    private Path[] recoverableFiles(Database database, SegmentsMetadata requestedSegmentState) throws IOException {
        try (var recoverableFiles = database.getStoreFileListing()
                .builder()
                .excludeAll()
                .includeReplayableStorageFiles(segmentsFilter(requestedSegmentState))
                .build()) {
            return recoverableFiles.stream().toArray(Path[]::new);
        }
    }

    private SegmentsFilter segmentsFilter(SegmentsMetadata requestedSegmentState) {
        return segmentMetadataService
                .segmentsMetadata()
                .differenceFilter(requestedSegmentState, database.getDatabaseLayout());
    }

    /**
     * Before we return a store snapshot we perform a checkpoint and acquire a mutex to prevent other checkpoints from
     * occurring until we have streamed all the *unrecoverable* files.
     */
    private Resource tryCheckpointAndAcquireMutex(CheckPointer checkPointer) throws IOException {
        return database.getStoreCopyCheckPointMutex().storeCopy(() -> {
            checkPointer.tryCheckPoint(new SimpleTriggerInfo("Store copy"));
        });
    }

    private StoreResource toStoreResource(Path databaseDirectory, Path storeFile) {
        var relativePath = databaseDirectory.relativize(storeFile).toString();
        return new StoreResource(storeFile, relativePath, fs);
    }
}
