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
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;
import org.apache.logging.log4j.core.util.NullOutputStream;
import org.neo4j.batchimport.api.AdditionalInitialIds;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.BatchImporter.HardwareValidation;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.IndexConfig;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.ReadBehaviour;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.ExternallyManagedPageCache;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.DatabaseCreationOptions;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.kernel.impl.index.schema.DefaultIndexProvidersAccess;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.LogTailMetadataFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.format.Index44Compatibility;
import org.neo4j.storageengine.migration.AbstractStoreMigrationParticipant;
import org.neo4j.storageengine.migration.TokenIndexMigrator;

/**
 * Migrates a store from one storage engine to another by doing something close to what store copy does
 * <br>
 * All tokens aren't necessarily migrated, only the ones referenced in the data will be included.
 */
public class AcrossEngineMigrationParticipant extends AbstractStoreMigrationParticipant {
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
    private final boolean keepNodeIds;
    private final long maxOffHeapMemory;
    private final PrintStream verboseProgressOutput;
    private final boolean verboseOutput;

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
            boolean forceBtreeIndexesToRange,
            boolean keepNodeIds,
            long maxOffHeapMemory,
            PrintStream verboseProgressOutput,
            boolean verboseOutput) {
        super(STORE_FILES_MIGRATOR_NAME);
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
        this.keepNodeIds = keepNodeIds;
        this.maxOffHeapMemory = maxOffHeapMemory;
        this.verboseProgressOutput = verboseProgressOutput;
        this.verboseOutput = verboseOutput;
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
        Supplier<IndexProvidersAccess> indexProviders = () -> new DefaultIndexProvidersAccess(
                targetStorageEngine, fileSystem, config, jobScheduler, logService, pageCacheTracer, contextFactory);

        // The default progress output is a condensed and consolidated 0..100% progress,
        // which (probably for legacy reasons) is done via the special VisibleMigrationProgressMonitorFactory.
        // However, for greater insight into what goes on during migration across formats
        // (which may be a large undertaking) then skip that condensed progress and instead show the real progress
        // from the importer which contains a lot more details.
        Monitor progressTrackingMonitor;
        PrintStream progressOutput;
        if (verboseOutput) {
            progressTrackingMonitor = Monitor.NO_MONITOR;
            progressOutput = verboseProgressOutput;
        } else {
            progressTrackingMonitor = progressTrackingMonitor(progressListener);
            progressOutput = new PrintStream(NullOutputStream.nullOutputStream());
        }

        BatchImporter importer = targetStorageEngine.batchImporter(
                migrationLayoutArg,
                fileSystem,
                false,
                pageCacheTracer,
                // Creating both indexes here. The existing store we are migrating doesn't necessarily
                // have both, and we could take that into account, but it is easiest to just assume
                // everyone wants them and create both.
                new Configuration.Overridden(Configuration.defaultConfiguration()) {
                    @Override
                    public IndexConfig indexConfig() {
                        return IndexConfig.create().withLabelIndex().withRelationshipTypeIndex();
                    }

                    // These two methods below has logic for accommodating both the current behavior of
                    // only providing max-off-heap-memory, and also the legacy behavior of passing in an
                    // external page cache (which is mostly due the existence of migrate command --pagecache option).

                    @Override
                    public ExternallyManagedPageCache providedPageCache() {
                        return maxOffHeapMemory == UNSPECIFIED_MAX_OFF_HEAP_MEMORY
                                ? new ExternallyManagedPageCache(pageCache)
                                : null;
                    }

                    @Override
                    public long maxOffHeapMemory() {
                        return maxOffHeapMemory == UNSPECIFIED_MAX_OFF_HEAP_MEMORY
                                ? super.maxOffHeapMemory()
                                : maxOffHeapMemory;
                    }

                    @Override
                    public boolean enableInstrumentation() {
                        return false;
                    }
                },
                logService,
                progressOutput,
                verboseOutput,
                additionalInitialIds,
                new LogTailMetadataFactoryImpl(fileSystem),
                localConfig,
                progressTrackingMonitor,
                jobScheduler,
                Collector.EMPTY,
                TransactionLogInitializer.getLogFilesInitializer(),
                indexImporterFactory,
                memoryTracker,
                contextFactory,
                indexProviders,
                0,
                null,
                DatabaseCreationOptions.EMPTY_CREATION_OPTIONS,
                HardwareValidation.NONE);

        // Do the copy
        try (Input fromInput = srcStorageEngine.asBatchImporterInput(
                directoryLayoutArg,
                fileSystem,
                pageCache,
                pageCacheTracer,
                config,
                memoryTracker,
                ReadBehaviour.INCLUSIVE_STRICT,
                !keepNodeIds,
                contextFactory,
                tailMetadata)) {
            if (!targetStorageEngine.supportsVectorData() && fromInput.containsVectorData()) {
                throw new UnsupportedOperationException("Provided input is known to contain vector value data, "
                        + "which is not supported by the target storage engine.");
            }
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
                forceBtreeIndexesToRange,
                ReadBehaviour.INCLUSIVE_STRICT,
                memoryTracker);
    }

    private Monitor progressTrackingMonitor(ProgressListener progressReporter) {
        return new Monitor() {
            private int lastReportedPercentage;

            @Override
            public void percentageCompleted(int percentage) {
                int diff = percentage - lastReportedPercentage;
                if (diff > 0) {
                    progressReporter.add(diff);
                    lastReportedPercentage = percentage;
                }
            }
        };
    }

    @Override
    public void moveMigratedFiles(
            DatabaseLayout migrationLayoutArg,
            DatabaseLayout directoryLayoutArg,
            StoreVersion versionToUpgradeFrom,
            StoreVersion versionToUpgradeTo,
            MemoryTracker memoryTracker)
            throws IOException {
        DatabaseLayout migrationDatabaseLayout = targetStorageEngine.formatSpecificDatabaseLayout(migrationLayoutArg);
        DatabaseLayout sourceDatabaseLayout = srcStorageEngine.formatSpecificDatabaseLayout(directoryLayoutArg);

        // Delete all old store files, indexes, profiles that belonged to the old store since the
        // engine probably has different files and move won't replace all
        Path indexFolder = IndexDirectoryStructure.baseSchemaIndexFolder(sourceDatabaseLayout.databaseDirectory());
        Path toplevelIndexFolder = indexFolder;
        while (!toplevelIndexFolder.getParent().equals(sourceDatabaseLayout.databaseDirectory())) {
            toplevelIndexFolder = toplevelIndexFolder.getParent();
        }
        Path profiles = sourceDatabaseLayout.databaseDirectory().resolve("profiles");
        Path vectors = sourceDatabaseLayout.vectorStoresDirectory();
        Collection<Path> storeFiles =
                new ArrayList<>(srcStorageEngine.listStorageFiles(fileSystem, sourceDatabaseLayout));
        storeFiles.add(toplevelIndexFolder);
        storeFiles.add(vectors);
        storeFiles.add(profiles);
        // If migrating from <5 the legacy token indexes are not in the index folder
        storeFiles.addAll(sourceDatabaseLayout
                .file(TokenIndexMigrator.LEGACY_LABEL_INDEX_STORE)
                .allSegments(fileSystem));
        storeFiles.addAll(sourceDatabaseLayout
                .file(TokenIndexMigrator.LEGACY_RELATIONSHIP_TYPE_INDEX_STORE)
                .allSegments(fileSystem));
        fileOperation(
                DELETE_INCLUDING_DIRS,
                fileSystem,
                sourceDatabaseLayout,
                migrationDatabaseLayout,
                storeFiles.toArray(new Path[] {}),
                true, // allow to skip non-existent source files
                ExistingTargetStrategy.OVERWRITE);

        // Move the migrated ones into the store directory
        Path migIndexFolder =
                IndexDirectoryStructure.baseSchemaIndexFolder(migrationDatabaseLayout.databaseDirectory());
        Path vectorIndexFolder = migrationDatabaseLayout.vectorStoresDirectory();
        storeFiles = targetStorageEngine.listStorageFiles(fileSystem, migrationDatabaseLayout);
        fileOperation(
                MOVE,
                fileSystem,
                migrationDatabaseLayout,
                sourceDatabaseLayout,
                storeFiles.toArray(new Path[] {}),
                true, // allow to skip non-existent source files
                ExistingTargetStrategy.OVERWRITE);

        // Move the token indexes that were built in migrate, so they don't have to rebuild on start-up
        fileSystem.moveToDirectory(migIndexFolder, indexFolder.getParent());
        // move vector files
        if (fileSystem.fileExists(vectorIndexFolder)) {
            fileSystem.moveToDirectory(vectorIndexFolder, sourceDatabaseLayout.databaseDirectory());
        }
    }

    @Override
    public void postMigration(
            DatabaseLayout databaseLayout, StoreVersion toVersion, long txIdBeforeMigration, long txIdAfterMigration) {
        // No need for updating the latest count stores tx-id here. Logs migration will end up on same id as
        // the batchimporter.
    }

    @Override
    public void cleanup(DatabaseLayout migrationLayout) throws IOException {}

    private static AdditionalInitialIds getInitialIds(LogTailMetadata tailMetadata) {
        return new AdditionalInitialIds() {

            @Override
            public long lastCommittedTransactionId() {
                return tailMetadata.getLastCommittedTransaction().id();
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

            @Override
            public long lastAppendIndex() {
                return tailMetadata.getLastCheckpointedAppendIndex();
            }

            @Override
            public long lastCommittedTransactionAppendIndex() {
                return tailMetadata.getLastCommittedTransaction().appendIndex();
            }
        };
    }
}
