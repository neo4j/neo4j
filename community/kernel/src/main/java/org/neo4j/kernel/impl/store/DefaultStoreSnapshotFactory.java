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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;
import org.neo4j.graphdb.Resource;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.logging.InternalLog;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreResource;
import org.neo4j.storageengine.api.StoreSnapshot;

public class DefaultStoreSnapshotFactory implements StoreSnapshot.Factory {
    private final Database database;
    private final FileSystemAbstraction fs;
    private final InternalLog log;

    public DefaultStoreSnapshotFactory(Database database, FileSystemAbstraction fs) {
        this.database = database;
        this.fs = fs;
        this.log = database.getInternalLogProvider().getLog(getClass());
    }

    @Override
    public Optional<StoreSnapshot> createStoreSnapshot() throws IOException {
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
            var lastCommittedTransactionId = latestCheckpointInfo.highestObservedClosedTransactionId();
            long appendIndex = latestCheckpointInfo.appendIndex();

            unrecoverableFiles = unrecoverableFiles(database);
            var recoverableFiles = recoverableFiles(database);
            var snapshot = new StoreSnapshot(
                    unrecoverableFiles,
                    recoverableFiles,
                    lastCommittedTransactionId,
                    appendIndex,
                    database.getStoreId(),
                    checkpointMutex);
            var result = Optional.of(snapshot);
            success = true;
            return result;
        } finally {
            if (!success) {
                IOUtils.closeAll(unrecoverableFiles, checkpointMutex);
            }
        }
    }

    /**
     * Stream of store files whose contents are *not* updated via transactional commands.
     *
     * As these files may not be recovered later, they must be included in full in the {@link StoreSnapshot}.
     * We intentionally return an *un-closed* {@code Stream<StoreResource>}, which is then closed
     * as part of the wrapping StoreSnapshot when a caller is finished with it.
     */
    private Stream<StoreResource> unrecoverableFiles(Database database) throws IOException {
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
    private Path[] recoverableFiles(Database database) throws IOException {
        try (var recoverableFiles = database.getStoreFileListing()
                .builder()
                .excludeAll()
                .includeReplayableStorageFiles()
                .build()) {
            return recoverableFiles.stream().map(StoreFileMetadata::path).toArray(Path[]::new);
        }
    }

    /**
     * Before we return a store snapshot we perform a checkpoint and acquire a mutex to prevent other checkpoints from
     * occurring until we have streamed all the *unrecoverable* files.
     */
    private Resource tryCheckpointAndAcquireMutex(CheckPointer checkPointer) throws IOException {
        return database.getStoreCopyCheckPointMutex()
                .storeCopy(() -> checkPointer.tryCheckPoint(new SimpleTriggerInfo("Store copy")));
    }

    private StoreResource toStoreResource(Path databaseDirectory, StoreFileMetadata storeFileMetadata) {
        var file = storeFileMetadata.path();
        var relativePath = databaseDirectory.relativize(file).toString();
        return new StoreResource(file, relativePath, fs);
    }
}
