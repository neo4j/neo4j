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
package org.neo4j.kernel.impl.storemigration;

import static org.neo4j.storageengine.migration.StoreMigrationParticipant.NOT_PARTICIPATING;
import static org.neo4j.util.Preconditions.checkState;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.MigrationStoreVersionCheck;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;

public class StorageEngineMigrationAbstraction {
    private final StorageEngineFactory storageEngineFactory;
    private final StorageEngineFactory targetStorageEngineFactory;
    private final boolean migrationAcrossEngine;

    StorageEngineMigrationAbstraction(
            StorageEngineFactory storageEngineFactory, StorageEngineFactory targetStorageEngineFactory) {
        this.storageEngineFactory = storageEngineFactory;
        this.targetStorageEngineFactory = targetStorageEngineFactory;
        this.migrationAcrossEngine = storageEngineFactory.id() != targetStorageEngineFactory.id();
    }

    StorageEngineFactory getStorageEngineFactory() {
        return storageEngineFactory;
    }

    StorageEngineFactory getTargetStorageEngineFactory() {
        return targetStorageEngineFactory;
    }

    MigrationStoreVersionCheck getStoreVersionCheck(
            FileSystemAbstraction fs,
            PageCache pageCache,
            DatabaseLayout databaseLayout,
            Config config,
            LogService logService,
            CursorContextFactory contextFactory) {

        return migrationAcrossEngine
                ? new AcrossEngineVersionCheck(
                        fs,
                        pageCache,
                        databaseLayout,
                        config,
                        logService,
                        contextFactory,
                        storageEngineFactory,
                        targetStorageEngineFactory)
                : storageEngineFactory.versionCheck(fs, databaseLayout, config, pageCache, logService, contextFactory);
    }

    List<StoreMigrationParticipant> getMigrationParticipants(
            boolean forceBtreeIndexesToRange,
            boolean keepNodeIds,
            FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            LogService logService,
            JobScheduler jobScheduler,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            IndexProviderMap indexProviderMap) {
        List<StoreMigrationParticipant> storeParticipants = new ArrayList<>();
        if (migrationAcrossEngine) {
            // One participant that copies over the data and schema. It doesn't care about any other
            // participants that the storage engines might have since those don't understand going between engines.
            storeParticipants.add(new AcrossEngineMigrationParticipant(
                    fs,
                    pageCache,
                    pageCacheTracer,
                    config,
                    logService,
                    jobScheduler,
                    contextFactory,
                    memoryTracker,
                    storageEngineFactory,
                    targetStorageEngineFactory,
                    forceBtreeIndexesToRange,
                    keepNodeIds));
        } else {
            // Get all the participants from the storage engine and add them where they want to be
            storeParticipants.addAll(storageEngineFactory.migrationParticipants(
                    fs,
                    config,
                    pageCache,
                    jobScheduler,
                    logService,
                    memoryTracker,
                    pageCacheTracer,
                    contextFactory,
                    forceBtreeIndexesToRange));

            // Do individual index provider migration last because they may delete files that we need in earlier steps.
            indexProviderMap.accept(provider -> storeParticipants.add(provider.storeMigrationParticipant(
                    fs, pageCache, pageCacheTracer, storageEngineFactory, contextFactory)));
        }

        Set<String> participantNames = new HashSet<>();
        storeParticipants.forEach(participant -> {
            if (!NOT_PARTICIPATING.equals(participant)) {
                var newParticipantName = participant.getName();
                checkState(
                        !participantNames.contains(newParticipantName),
                        "Migration participants should have unique names. Participant with name: '%s' is already registered.",
                        newParticipantName);
                participantNames.add(newParticipantName);
            }
        });

        return storeParticipants;
    }
}
