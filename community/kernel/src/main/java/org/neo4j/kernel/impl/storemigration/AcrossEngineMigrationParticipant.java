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

import static org.neo4j.kernel.impl.storemigration.FileOperation.DELETE_INCLUDING_DIRS;
import static org.neo4j.kernel.impl.storemigration.FileOperation.MOVE;
import static org.neo4j.kernel.impl.storemigration.StoreMigratorFileOperation.fileOperation;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Set;
import org.eclipse.collections.api.factory.Sets;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.IndexConfig;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.batchimport.Monitor;
import org.neo4j.internal.batchimport.ReadBehaviour;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.format.Index44Compatibility;
import org.neo4j.storageengine.migration.AbstractStoreMigrationParticipant;

/**
 * Migrates a store from one storage engine to another by doing something close to what store copy does
 *
 * All tokens aren't necessarily migrated, only the ones referenced in the data will be included.
 */
public class AcrossEngineMigrationParticipant extends AbstractStoreMigrationParticipant {
    public static final String NAME = "Store files";

    private final Config config;
    private final LogService logService;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final PageCacheTracer pageCacheTracer;
    private final JobScheduler jobScheduler;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;
    private final StorageEngineFactory srcStorageEngine;
    private final StorageEngineFactory targetStorageEngine;
    private final boolean forceBtreeIndexesToRange;

    public AcrossEngineMigrationParticipant(
            FileSystemAbstraction fileSystem,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            LogService logService,
            JobScheduler jobScheduler,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            StorageEngineFactory srcStorageEngine,
            StorageEngineFactory targetStorageEngine,
            boolean forceBtreeIndexesToRange) {
        super(NAME);
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.config = config;
        this.logService = logService;
        this.jobScheduler = jobScheduler;
        this.pageCacheTracer = pageCacheTracer;
        this.contextFactory = contextFactory;
        this.memoryTracker = memoryTracker;
        this.srcStorageEngine = srcStorageEngine;
        this.targetStorageEngine = targetStorageEngine;
        this.forceBtreeIndexesToRange = forceBtreeIndexesToRange;
    }

    @Override
    public void migrate(
            DatabaseLayout directoryLayoutArg,
            DatabaseLayout migrationLayoutArg,
            ProgressListener progressListener,
            StoreVersion fromVersion,
            StoreVersion toVersion,
            IndexImporterFactory indexImporterFactory,
            LogTailMetadata tailMetadata)
            throws IOException, KernelException {
        Config localConfig = Config.newBuilder()
                .fromConfig(config)
                .set(GraphDatabaseSettings.db_format, toVersion.formatName())
                .build();

        // Use the ids from the old logTail. This means that the importer will end up on the
        // same tx id as the logs migration
        AdditionalInitialIds additionalInitialIds = getInitialIds(tailMetadata);

        BatchImporter importer = targetStorageEngine.batchImporter(
                migrationLayoutArg,
                fileSystem,
                pageCacheTracer,
                // Creating both indexes here. The existing store we are migrating doesn't necessarily
                // have both, and we could take that into account, but it is easiest to just assume
                // everyone wants them and create both.
                new Configuration.Overridden(Configuration.defaultConfiguration()) {
                    @Override
                    public IndexConfig indexConfig() {
                        return IndexConfig.create().withLabelIndex().withRelationshipTypeIndex();
                    }
                },
                logService,
                // No progress printing or updating progressReporter right now. Probably should be..
                new PrintStream(OutputStream.nullOutputStream()),
                false,
                additionalInitialIds,
                localConfig,
                Monitor.NO_MONITOR,
                jobScheduler,
                Collector.EMPTY,
                TransactionLogInitializer.getLogFilesInitializer(),
                indexImporterFactory,
                memoryTracker,
                contextFactory);

        // Do the copy
        try (Input fromInput = srcStorageEngine.asBatchImporterInput(
                directoryLayoutArg,
                fileSystem,
                pageCache,
                pageCacheTracer,
                localConfig,
                memoryTracker,
                ReadBehaviour.INCLUSIVE_STRICT,
                false,
                contextFactory,
                tailMetadata)) {
            importer.doImport(fromInput);
        }

        SchemaMigrator.migrateSchemaRules(
                srcStorageEngine,
                targetStorageEngine,
                fileSystem,
                pageCache,
                pageCacheTracer,
                localConfig,
                directoryLayoutArg,
                migrationLayoutArg,
                fromVersion.hasCapability(Index44Compatibility.INSTANCE),
                contextFactory,
                tailMetadata,
                forceBtreeIndexesToRange);
    }

    @Override
    public void moveMigratedFiles(
            DatabaseLayout migrationLayoutArg,
            DatabaseLayout directoryLayoutArg,
            StoreVersion versionToUpgradeFrom,
            StoreVersion versionToUpgradeTo)
            throws IOException {
        DatabaseLayout mig = targetStorageEngine.formatSpecificDatabaseLayout(migrationLayoutArg);
        DatabaseLayout dir = srcStorageEngine.formatSpecificDatabaseLayout(directoryLayoutArg);

        // Delete all old store files, indexes, profiles that belonged to the old store since the
        // engine probably has different files and move won't replace all
        Path indexFolder = IndexDirectoryStructure.baseSchemaIndexFolder(dir.databaseDirectory());
        Path toplevelIndexFolder = indexFolder;
        while (!toplevelIndexFolder.getParent().equals(dir.databaseDirectory())) {
            toplevelIndexFolder = toplevelIndexFolder.getParent();
        }
        Path profiles = dir.databaseDirectory().resolve("profiles");
        Set<Path> storeFiles = Sets.mutable.of(dir.storeFiles().toArray(new Path[] {}));
        Set<Path> idFiles = dir.idFiles();
        storeFiles.addAll(idFiles);
        storeFiles.add(toplevelIndexFolder);
        storeFiles.add(profiles);
        fileOperation(
                DELETE_INCLUDING_DIRS,
                fileSystem,
                dir,
                mig,
                storeFiles.toArray(new Path[] {}),
                true, // allow to skip non-existent source files
                ExistingTargetStrategy.OVERWRITE);

        // Move the migrated ones into the store directory
        Path migIndexFolder = IndexDirectoryStructure.baseSchemaIndexFolder(mig.databaseDirectory());
        storeFiles = Sets.mutable.of(mig.storeFiles().toArray(new Path[] {}));
        idFiles = mig.idFiles();
        storeFiles.addAll(idFiles);
        fileOperation(
                MOVE,
                fileSystem,
                mig,
                dir,
                storeFiles.toArray(new Path[] {}),
                true, // allow to skip non-existent source files
                ExistingTargetStrategy.OVERWRITE);

        // Move the token indexes that were built in migrate, so they don't have to rebuild on start-up
        fileSystem.moveToDirectory(migIndexFolder, indexFolder.getParent());
    }

    @Override
    public void postMigration(
            DatabaseLayout databaseLayout, StoreVersion toVersion, long txIdBeforeMigration, long txIdAfterMigration)
            throws IOException {
        // No need for updating the latest count stores tx-id here. Logs migration will end up on same id as
        // the batchimporter.
    }

    @Override
    public void cleanup(DatabaseLayout migrationLayout) throws IOException {}

    private static AdditionalInitialIds getInitialIds(LogTailMetadata tailMetadata) {
        return new AdditionalInitialIds() {

            @Override
            public long lastCommittedTransactionId() {
                return tailMetadata.getLastCommittedTransaction().transactionId();
            }

            @Override
            public int lastCommittedTransactionChecksum() {
                return tailMetadata.getLastCommittedTransaction().checksum();
            }

            @Override
            public long lastCommittedTransactionLogVersion() {
                return tailMetadata.getLastTransactionLogPosition().getLogVersion();
            }

            @Override
            public long lastCommittedTransactionLogByteOffset() {
                return tailMetadata.getLastTransactionLogPosition().getByteOffset();
            }

            @Override
            public long checkpointLogVersion() {
                return tailMetadata.getCheckpointLogVersion();
            }
        };
    }
}
