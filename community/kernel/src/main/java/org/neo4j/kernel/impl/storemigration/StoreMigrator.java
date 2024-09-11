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

import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_ID;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Suppliers;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.FixedVersionContext;
import org.neo4j.io.pagecache.context.FixedVersionContextSupplier;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.recovery.LogTailExtractor;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.MigrationStoreVersionCheck;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.StoreVersionIdentifier;
import org.neo4j.storageengine.migration.MigrationProgressMonitor;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;

/**
 * A process for doing store upgrade and migration.
 * <p>
 * MIGRATION vs UPGRADE
 * Store upgrade and store migration are two modification of store that we distinguish between.
 * Store migration is generally a process that migrates a store from one version to another
 * or between store formats.
 * Store upgrade can be seen as a lightweight migration in the sense that only a subset of operations
 * that are permitted as part of a migration are permitted as part of an upgrade.
 * Upgrade has to conform to the following requirements:
 * <ul>
 *     <li>It takes a small and constant amount of time.</li>
 *     <li>It is rollable. Since cluster members perform upgrade of their store independently of each other,
 *     this requirement means that after each cluster member upgrades its own store, all of the cluster member have to end up with the same store</li>
 * </ul>
 * There is another important difference in usage. Upgrade is an implicit action that happens on database start up, but migration is an explicit action
 * that needs to be requested by a user.
 * <p>
 * Since upgrade is a special case of migration, most of the code is shared between the two processes.
 * <p>
 * A migration process consists of invocation of registered {@link StoreMigrationParticipant migration participants}.
 * Participants will be notified when it's time for migration.
 * The migration will happen to a separate, isolated directory so that an incomplete migration will not affect
 * the original database. Only when a successful migration has taken place the migrated store will replace
 * the original database.
 * <p>
 * Migration process at a glance:
 * <ol>
 * <li>Participants are asked whether or not there's a need for migration</li>
 * <li>Those that need are asked to migrate into a separate /migrate directory. Regardless of who actually
 * performs migration all participants are asked to satisfy dependencies for downstream participants</li>
 * <li>Migration is marked as migrated</li>
 * <li>Participants are asked to move their migrated files into the source directory,
 * replacing only the existing files, so that if only some store files needed migration the others are left intact</li>
 * <li>Migration is completed and participant resources are closed</li>
 * </ol>
 * <p>
 *
 * @see StoreMigrationParticipant
 */
public class StoreMigrator {
    private static final String STORE_UPGRADE_TAG = "storeMigrate";
    public static final String MIGRATION_DIRECTORY = "migrate";
    private static final String MIGRATION_STATUS_FILE = "_status";

    private final CursorContextFactory contextFactory;
    private final DatabaseTracers databaseTracers;
    private final DatabaseLayout databaseLayout;
    private final FileSystemAbstraction fs;
    private final Config config;
    private final PageCache pageCache;
    private final LogService logService;
    private final JobScheduler jobScheduler;
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;
    private final IndexProviderMap indexProviderMap;
    private final StorageEngineFactory storageEngineFactory;
    private final InternalLog internalLog;
    private Supplier<LogTailMetadata> logTailSupplier;
    private final StorageEngineMigrationAbstraction storageEngineMigrationAbstraction;

    public StoreMigrator(
            FileSystemAbstraction fs,
            Config config,
            LogService logService,
            PageCache pageCache,
            DatabaseTracers databaseTracers,
            JobScheduler jobScheduler,
            DatabaseLayout databaseLayout,
            StorageEngineFactory storageEngineFactory,
            StorageEngineFactory targetStorageEngineFactory,
            IndexProviderMap indexProviderMap,
            MemoryTracker memoryTracker,
            Supplier<LogTailMetadata> logTailSupplier) {
        this.fs = fs;
        this.config = config;
        this.logService = logService;
        this.pageCache = pageCache;
        this.databaseLayout = databaseLayout;
        this.storageEngineFactory = storageEngineFactory;
        this.jobScheduler = jobScheduler;
        this.databaseTracers = databaseTracers;
        this.pageCacheTracer = databaseTracers.getPageCacheTracer();
        this.memoryTracker = memoryTracker;
        this.indexProviderMap = indexProviderMap;

        this.internalLog = logService.getInternalLog(getClass());
        this.logTailSupplier = logTailSupplier;
        this.contextFactory =
                new CursorContextFactory(databaseTracers.getPageCacheTracer(), new LazyVersionContextSupplier());
        this.storageEngineMigrationAbstraction =
                new StorageEngineMigrationAbstraction(storageEngineFactory, targetStorageEngineFactory);
    }

    public void migrateIfNeeded(String formatToMigrateTo, boolean forceBtreeIndexesToRange, boolean keepNodeIds)
            throws UnableToMigrateException, IOException {
        checkStoreExists();

        try (var cursorContext = contextFactory.create(STORE_UPGRADE_TAG)) {
            MigrationStructures migrationStructures = getMigrationStructures();

            finishInterruptedMigration(migrationStructures);

            var checkResult = doMigrationCheck(formatToMigrateTo, cursorContext);

            internalLog.info("'" + checkResult.versionToMigrateFrom().getStoreVersionUserString()
                    + "' has been identified as the current version of the store");
            internalLog.info("'" + checkResult.versionToMigrateTo().getStoreVersionUserString()
                    + "' has been identified as the target version of the store migration");

            if (checkResult.onRequestedVersion()) {
                // Sure store version is good, but what about kernel version?
                var logsMigrator = new LogsMigrator(
                        fs,
                        storageEngineFactory,
                        databaseLayout,
                        pageCache,
                        config,
                        contextFactory,
                        logTailSupplier,
                        pageCacheTracer);
                if (logTailSupplier.get().kernelVersion().isLessThan(KernelVersion.getLatestVersion(config))) {
                    // The kernel version is read from the log tail on start up - migrate the logs to get to the
                    // latest kernel version
                    var logsCheckResult = logsMigrator.assertCleanlyShutDown();
                    doOnlyLogsMigration(logsCheckResult, checkResult.versionToMigrateTo);
                } else {
                    internalLog.info(
                            "The current store version and the migration target version are the same, so there is nothing to do.");
                }

                return;
            }

            doMigrate(
                    migrationStructures,
                    MigrationStatus.MigrationState.migrating,
                    checkResult.versionToMigrateFrom(),
                    checkResult.versionToMigrateTo(),
                    VisibleMigrationProgressMonitorFactory.forMigration(internalLog),
                    LogsMigrator.CheckResult::migrate,
                    forceBtreeIndexesToRange,
                    storageEngineMigrationAbstraction,
                    keepNodeIds);
        }
    }

    private void doOnlyLogsMigration(
            LogsMigrator.CheckResult logsCheckResult, StoreVersionIdentifier versionToMigrateTo) {
        internalLog.info("'" + logTailSupplier.get().kernelVersion()
                + "' has been identified as the current kernel version of the store.");
        internalLog.info("'" + KernelVersion.getLatestVersion(config)
                + "' has been identified as the target kernel version of the store migration.");

        StoreVersion toVersion =
                storageEngineFactory.versionInformation(versionToMigrateTo).orElseThrow();

        // This is not the most beautiful thing in the world...
        // We only want to get a new kernel version, for that we only need to update logs so there
        // is no reason for a migration directory and copying around store files.
        // BUT the count store and degree store will rebuild if they aren't pointing to the
        // latest transaction id, therefore we need to do the postMigration step that updates that.
        MigrationProgressMonitor progressMonitor = VisibleMigrationProgressMonitorFactory.forMigration(internalLog);
        progressMonitor.started(0);
        progressMonitor.startTransactionLogsMigration();
        LogsMigrator.MigrationTransactionIds txIds = logsCheckResult.migrate();
        progressMonitor.completeTransactionLogsMigration();

        // We are only changing logs, but we need to do the postMigration step since the last tx id has changed
        var participants = getStoreMigrationParticipants(storageEngineMigrationAbstraction, false, false);
        postMigration(participants, toVersion, txIds.txIdBeforeMigration(), txIds.txIdAfterMigration());

        progressMonitor.completed();
    }

    public void upgradeIfNeeded() throws UnableToMigrateException, IOException {
        if (!storageEngineFactory.storageExists(fs, databaseLayout)) {
            // upgrade is invoked on database start up and before new databases are initialised,
            // so the database store not existing is a perfectly valid scenario.
            return;
        }

        try (var cursorContext = contextFactory.create(STORE_UPGRADE_TAG)) {
            MigrationStructures migrationStructures = getMigrationStructures();

            finishInterruptedUpgrade(cursorContext, migrationStructures);

            var checkResult = doUpgradeCheck(cursorContext);
            if (checkResult.onRequestedVersion()) {
                // Unlike in the case migration which is an explicit action and the store being up to date is a
                // situation worth notifying the user about,
                // this is the most common outcome of the upgrade process as it is an implicit process invoked every
                // time a database starts up.
                return;
            }

            internalLog.info("'" + checkResult.versionToMigrateFrom().getStoreVersionUserString()
                    + "' has been identified as the current version of the store");
            internalLog.info("'" + checkResult.versionToMigrateTo().getStoreVersionUserString()
                    + "' has been identified as the target version of the store upgrade");

            doMigrate(
                    migrationStructures,
                    MigrationStatus.MigrationState.migrating,
                    checkResult.versionToMigrateFrom(),
                    checkResult.versionToMigrateTo(),
                    VisibleMigrationProgressMonitorFactory.forUpgrade(internalLog),
                    LogsMigrator.CheckResult::upgrade,
                    false,
                    storageEngineMigrationAbstraction,
                    false);
        }
    }

    private MigrationStructures getMigrationStructures() {
        DatabaseLayout migrationStructure = DatabaseLayout.ofFlat(databaseLayout.file(MIGRATION_DIRECTORY));
        return new MigrationStructures(migrationStructure, migrationStructure.file(MIGRATION_STATUS_FILE));
    }

    private void doMigrate(
            MigrationStructures migrationStructures,
            MigrationStatus.MigrationState migrationState,
            StoreVersionIdentifier versionToMigrateFrom,
            StoreVersionIdentifier versionToMigrateTo,
            MigrationProgressMonitor progressMonitor,
            LogsAction logsAction,
            boolean forceBtreeIndexesToRange,
            StorageEngineMigrationAbstraction storageEngineMigrationAbstraction,
            boolean keepNodeIds)
            throws IOException {
        var participants =
                getStoreMigrationParticipants(storageEngineMigrationAbstraction, forceBtreeIndexesToRange, keepNodeIds);
        // One or more participants would like to do migration
        progressMonitor.started(participants.size());

        var logsMigrator = new LogsMigrator(
                fs,
                storageEngineMigrationAbstraction.getTargetStorageEngineFactory(),
                databaseLayout,
                pageCache,
                config,
                contextFactory,
                logTailSupplier,
                pageCacheTracer);
        var logsCheckResult = logsMigrator.assertCleanlyShutDown();
        StoreVersion fromVersion = storageEngineMigrationAbstraction
                .getStorageEngineFactory()
                .versionInformation(versionToMigrateFrom)
                .orElseThrow();
        StoreVersion toVersion = storageEngineMigrationAbstraction
                .getTargetStorageEngineFactory()
                .versionInformation(versionToMigrateTo)
                .orElseThrow();

        // We don't need to migrate if we're at the phase where we have migrated successfully
        // and it's just a matter of moving over the files to the storeDir.
        if (MigrationStatus.MigrationState.migrating.isNeededFor(migrationState)) {
            cleanMigrationDirectory(migrationStructures.migrationLayout.databaseDirectory());
            MigrationStatus.MigrationState.migrating.setMigrationStatus(
                    fs,
                    migrationStructures.migrationStateFile,
                    versionToMigrateFrom,
                    versionToMigrateTo,
                    memoryTracker);
            migrateToIsolatedDirectory(
                    participants,
                    databaseLayout,
                    migrationStructures.migrationLayout,
                    fromVersion,
                    toVersion,
                    progressMonitor);
            MigrationStatus.MigrationState.moving.setMigrationStatus(
                    fs,
                    migrationStructures.migrationStateFile,
                    versionToMigrateFrom,
                    versionToMigrateTo,
                    memoryTracker);
        }

        if (MigrationStatus.MigrationState.moving.isNeededFor(migrationState)) {
            moveMigratedFilesToStoreDirectory(
                    participants,
                    migrationStructures.migrationLayout,
                    databaseLayout,
                    fromVersion,
                    toVersion,
                    memoryTracker);
        }

        progressMonitor.startTransactionLogsMigration();
        var txIds = logsAction.handleLogs(logsCheckResult);
        progressMonitor.completeTransactionLogsMigration();

        postMigration(participants, toVersion, txIds.txIdBeforeMigration(), txIds.txIdAfterMigration());

        cleanup(participants, migrationStructures.migrationLayout);

        progressMonitor.completed();
    }

    private CheckResult doMigrationCheck(String formatToMigrateTo, CursorContext cursorContext) {
        MigrationStoreVersionCheck storeVersionCheck = storageEngineMigrationAbstraction.getStoreVersionCheck(
                fs, pageCache, databaseLayout, config, logService, contextFactory);

        var checkResult = storeVersionCheck.getAndCheckMigrationTargetVersion(formatToMigrateTo, cursorContext);
        var fromVersion = checkResult.versionToMigrateFrom();
        var toVersion = checkResult.versionToMigrateTo();
        return switch (checkResult.outcome()) {
            case MIGRATION_POSSIBLE -> new CheckResult(false, fromVersion, toVersion);
            case NO_OP -> new CheckResult(true, fromVersion, toVersion);
            case STORE_VERSION_RETRIEVAL_FAILURE -> throw new UnableToMigrateException(
                    "Failed to read current store version. This usually indicate a store corruption",
                    checkResult.cause());
            case UNSUPPORTED_MIGRATION_PATH -> throw new UnableToMigrateException(String.format(
                    "Store migration from '%s' to '%s' not supported",
                    fromVersion.getStoreVersionUserString(),
                    toVersion != null ? toVersion.getStoreVersionUserString() : formatToMigrateTo));
            case UNSUPPORTED_TARGET_VERSION -> throw new UnableToMigrateException(
                    "The current store version is not supported. " + "Please migrate the store to be able to continue");
            case UNSUPPORTED_MIGRATION_LIMITS -> throw new UnableToMigrateException(
                    String.format(
                            "Store migration from '%s' to '%s' not supported for this store",
                            fromVersion.getStoreVersionUserString(),
                            toVersion != null ? toVersion.getStoreVersionUserString() : formatToMigrateTo),
                    checkResult.cause());
        };
    }

    private CheckResult doUpgradeCheck(CursorContext cursorContext) {
        StoreVersionCheck storeVersionCheck =
                storageEngineFactory.versionCheck(fs, databaseLayout, config, pageCache, logService, contextFactory);
        var checkResult = storeVersionCheck.getAndCheckUpgradeTargetVersion(cursorContext);
        return switch (checkResult.outcome()) {
            case UPGRADE_POSSIBLE -> new CheckResult(
                    false, checkResult.versionToUpgradeFrom(), checkResult.versionToUpgradeTo());
            case NO_OP -> new CheckResult(true, checkResult.versionToUpgradeFrom(), checkResult.versionToUpgradeTo());
                // since an upgrade is an implicit action we need to be a bit careful about the error given
                // when the retrieval of the current store version fails
            case STORE_VERSION_RETRIEVAL_FAILURE -> throw new IllegalStateException(
                    "Failed to read current store version.", checkResult.cause());
            case UNSUPPORTED_TARGET_VERSION -> throw new UnableToMigrateException(String.format(
                    "The selected target store format '%s' (introduced in %s) is no longer supported",
                    checkResult.versionToUpgradeTo().getStoreVersionUserString(),
                    storeVersionCheck.getIntroductionVersionFromVersion(checkResult.versionToUpgradeTo())));
        };
    }

    private void finishInterruptedMigration(MigrationStructures migrationStructures) throws IOException {
        MigrationStatus migrationStatus =
                MigrationStatus.readMigrationStatus(fs, migrationStructures.migrationStateFile, memoryTracker);
        if (migrationStatus.migrationInProgress()) {
            MigrationStatus.MigrationState state = migrationStatus.state();
            if (state == MigrationStatus.MigrationState.moving) {
                // If a previous migration was interrupted in the moving phase that move must be completed to have a
                // consistent store again.
                internalLog.info("Resuming migration in progress to '"
                        + migrationStatus.versionToMigrateTo().getStoreVersionUserString() + "'");

                // The interrupted migration is not necessarily to the same version - it could belong to a different
                // storage engine
                StoreVersionIdentifier versionToMigrateTo = migrationStatus.versionToMigrateTo();
                Config localConfig = Config.newBuilder()
                        .fromConfig(config)
                        .set(GraphDatabaseSettings.db_format, versionToMigrateTo.getFormatName())
                        .build();
                StorageEngineFactory targetStorageEngineFactory = StorageEngineFactory.selectStorageEngine(localConfig);
                StorageEngineMigrationAbstraction storageEngineMigrationAbstraction =
                        new StorageEngineMigrationAbstraction(storageEngineFactory, targetStorageEngineFactory);
                doMigrate(
                        migrationStructures,
                        MigrationStatus.MigrationState.moving,
                        migrationStatus.versionToMigrateFrom(),
                        versionToMigrateTo,
                        VisibleMigrationProgressMonitorFactory.forMigration(internalLog),
                        // Since we are doing a migration it should be safe to use the LogsMigrator#migrate
                        LogsMigrator.CheckResult::migrate,
                        false,
                        storageEngineMigrationAbstraction,
                        false);

                // Have new logTail now, use that one instead
                logTailSupplier = getLogTailSupplier(targetStorageEngineFactory);
            } else {
                removeOldMigrationDir(migrationStructures.migrationLayout.databaseDirectory());
            }
        }
    }

    private void finishInterruptedUpgrade(CursorContext cursorContext, MigrationStructures migrationStructures)
            throws IOException {
        MigrationStatus migrationStatus =
                MigrationStatus.readMigrationStatus(fs, migrationStructures.migrationStateFile, memoryTracker);
        if (migrationStatus.migrationInProgress()) {
            MigrationStatus.MigrationState state = migrationStatus.state();
            if (state == MigrationStatus.MigrationState.moving) {
                var checkResult = doUpgradeCheck(cursorContext);
                if (!migrationStatus.expectedMigration(checkResult.versionToMigrateTo)) {
                    throw new UnableToMigrateException("A partially complete migration to "
                            + migrationStatus.versionToMigrateTo().getStoreVersionUserString()
                            + " found when trying to migrate to "
                            + checkResult.versionToMigrateTo.getStoreVersionUserString()
                            + ". Complete that migration before continuing. "
                            + "This can be done by running the migration tool");
                }

                internalLog.info("Resuming upgrade in progress to '" + checkResult.versionToMigrateTo + "'");
                doMigrate(
                        migrationStructures,
                        MigrationStatus.MigrationState.moving,
                        migrationStatus.versionToMigrateFrom(),
                        migrationStatus.versionToMigrateTo(),
                        VisibleMigrationProgressMonitorFactory.forUpgrade(internalLog),
                        LogsMigrator.CheckResult::upgrade,
                        false,
                        storageEngineMigrationAbstraction,
                        false);

                // Could have new logTail now, use that one instead
                logTailSupplier = getLogTailSupplier(storageEngineFactory);
            } else {
                removeOldMigrationDir(migrationStructures.migrationLayout.databaseDirectory());
            }
        }
    }

    private List<StoreMigrationParticipant> getStoreMigrationParticipants(
            StorageEngineMigrationAbstraction storageEngineMigrationAbstraction,
            boolean forceBtreeIndexesToRange,
            boolean keepNodeIds) {
        return storageEngineMigrationAbstraction.getMigrationParticipants(
                forceBtreeIndexesToRange,
                keepNodeIds,
                fs,
                pageCache,
                pageCacheTracer,
                config,
                logService,
                jobScheduler,
                contextFactory,
                memoryTracker,
                indexProviderMap);
    }

    private void migrateToIsolatedDirectory(
            List<StoreMigrationParticipant> participants,
            DatabaseLayout directoryLayout,
            DatabaseLayout migrationLayout,
            StoreVersion fromVersion,
            StoreVersion toVersion,
            MigrationProgressMonitor progressMonitor) {
        try {
            for (StoreMigrationParticipant participant : participants) {
                try (ProgressListener processListener = progressMonitor.startSection(participant.getName())) {
                    IndexImporterFactory indexImporterFactory = new IndexImporterFactoryImpl();
                    participant.migrate(
                            directoryLayout,
                            migrationLayout,
                            processListener,
                            fromVersion,
                            toVersion,
                            indexImporterFactory,
                            logTailSupplier.get());
                }
            }
        } catch (IOException | UncheckedIOException | KernelException e) {
            throw new UnableToMigrateException("A critical failure during migration has occurred", e);
        }
    }

    private static void moveMigratedFilesToStoreDirectory(
            Iterable<StoreMigrationParticipant> participants,
            DatabaseLayout migrationLayout,
            DatabaseLayout directoryLayout,
            StoreVersion versionToMigrateFrom,
            StoreVersion versionToMigrateTo,
            MemoryTracker memoryTracker) {
        try {
            for (StoreMigrationParticipant participant : participants) {
                participant.moveMigratedFiles(
                        migrationLayout, directoryLayout, versionToMigrateFrom, versionToMigrateTo, memoryTracker);
            }
        } catch (IOException e) {
            throw new UnableToMigrateException(
                    "A critical failure during migration has occurred. Failed to move migrated files into place", e);
        }
    }

    private void checkStoreExists() {
        if (!storageEngineFactory.storageExists(fs, databaseLayout)) {
            throw new UnableToMigrateException("Database '" + databaseLayout.getDatabaseName()
                    + "' either does not exists or it has not been initialised");
        }
    }

    private void postMigration(
            Iterable<StoreMigrationParticipant> participants,
            StoreVersion toVersion,
            long txIdBeforeMigration,
            long txIdAfterMigration) {
        try {
            for (var participant : participants) {
                participant.postMigration(databaseLayout, toVersion, txIdBeforeMigration, txIdAfterMigration);
            }
        } catch (IOException e) {
            throw new UnableToMigrateException(
                    "A critical failure during migration has occurred. Unable to do post-migration step", e);
        }
    }

    private void cleanup(Iterable<StoreMigrationParticipant> participants, DatabaseLayout migrationStructure) {
        try {
            for (StoreMigrationParticipant participant : participants) {
                participant.cleanup(migrationStructure);
            }
        } catch (IOException e) {
            throw new UnableToMigrateException(
                    "A critical failure during store migration has occurred. Failed to clean up after migration", e);
        }
        removeOldMigrationDir(migrationStructure.databaseDirectory());
    }

    private void cleanMigrationDirectory(Path migrationDirectory) {
        removeOldMigrationDir(migrationDirectory);
        try {
            fs.mkdir(migrationDirectory);
        } catch (IOException e) {
            throw new UnableToMigrateException(
                    "A critical failure during migration has occurred. Failed to create a migration directory "
                            + migrationDirectory,
                    e);
        }
    }

    private void removeOldMigrationDir(Path migrationDirectory) {
        try {
            if (fs.fileExists(migrationDirectory)) {
                fs.deleteRecursively(migrationDirectory);
            }
        } catch (IOException | UncheckedIOException e) {
            throw new UnableToMigrateException(
                    "A critical failure during migration has occurred. Failed to delete a migration directory "
                            + migrationDirectory,
                    e);
        }
    }

    private Suppliers.Lazy<LogTailMetadata> getLogTailSupplier(StorageEngineFactory storageEngineFactory) {
        return Suppliers.lazySingleton(() -> {
            try {
                // If empty tx logs are allowed, and we don't have tx logs we fall back to the latest kernel version.
                // That should be safe since we are trying to migrate to that version anyway.
                return new LogTailExtractor(fs, config, storageEngineFactory, databaseTracers)
                        .getTailMetadata(databaseLayout, memoryTracker, () -> KernelVersion.getLatestVersion(config));
            } catch (Exception e) {
                throw new UnableToMigrateException("Fail to load log tail during migration.", e);
            }
        });
    }

    private record MigrationStructures(DatabaseLayout migrationLayout, Path migrationStateFile) {}

    private record CheckResult(
            boolean onRequestedVersion,
            StoreVersionIdentifier versionToMigrateFrom,
            StoreVersionIdentifier versionToMigrateTo) {}

    private interface LogsAction {
        LogsMigrator.MigrationTransactionIds handleLogs(LogsMigrator.CheckResult checkResult);
    }

    private class LazyVersionContextSupplier extends FixedVersionContextSupplier {
        public LazyVersionContextSupplier() {
            super(UNKNOWN_TX_ID);
        }

        @Override
        public VersionContext createVersionContext() {
            return new LazyTailVersionContext(logTailSupplier);
        }
    }

    private static class LazyTailVersionContext extends FixedVersionContext {
        private final Supplier<LogTailMetadata> logTailSupplier;

        public LazyTailVersionContext(Supplier<LogTailMetadata> logTailSupplier) {
            super(UNKNOWN_TX_ID);
            this.logTailSupplier = logTailSupplier;
        }

        @Override
        public long committingTransactionId() {
            return logTailSupplier.get().getLastCommittedTransaction().id();
        }
    }
}
