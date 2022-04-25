/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.storemigration;

import static org.neo4j.storageengine.migration.StoreMigrationParticipant.NOT_PARTICIPATING;
import static org.neo4j.util.Preconditions.checkState;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Suppliers;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.recovery.LogTailExtractor;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.migration.MigrationProgressMonitor;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;

/**
 * A process for doing store upgrade and migration.
 * <p>
 * MIGRATION vs UPGRADE
 * Store upgrade and store migration are two modification of store that we distinguish between.
 * Store migration is generally a process that migrates a store from one version to another
 * or between store formats.
 * Store upgrade can be seen as a ligthweight migration in the sense that only a subset of operations
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

    public StoreMigrator(
            FileSystemAbstraction fs,
            Config config,
            LogService logService,
            PageCache pageCache,
            DatabaseTracers databaseTracers,
            JobScheduler jobScheduler,
            DatabaseLayout databaseLayout,
            StorageEngineFactory storageEngineFactory,
            IndexProviderMap indexProviderMap,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            Supplier<LogTailMetadata> logTailSupplier) {
        this.fs = fs;
        this.config = config;
        this.logService = logService;
        this.pageCache = pageCache;
        this.databaseLayout = databaseLayout;
        this.storageEngineFactory = storageEngineFactory;
        this.contextFactory = contextFactory;
        this.jobScheduler = jobScheduler;
        this.databaseTracers = databaseTracers;
        this.pageCacheTracer = databaseTracers.getPageCacheTracer();
        this.memoryTracker = memoryTracker;
        this.indexProviderMap = indexProviderMap;

        this.internalLog = logService.getInternalLog(getClass());
        this.logTailSupplier = logTailSupplier;
    }

    public void migrateIfNeeded(String formatFamily) throws UnableToMigrateException {
        checkStoreExists();

        try (var cursorContext = contextFactory.create(STORE_UPGRADE_TAG)) {
            MigrationStructures migrationStructures = getMigrationStructures();

            finishInterruptedMigration(migrationStructures);

            var checkResult = doMigrationCheck(formatFamily, cursorContext);

            internalLog.info("'" + checkResult.versionToMigrateFrom()
                    + "' has been identified as the current version of the store");
            internalLog.info("'" + checkResult.versionToMigrateTo()
                    + "' has been identified as the target version of the store migration");

            if (checkResult.onRequestedVersion()) {
                internalLog.info(
                        "The current store version and the migration target version are the same, so there is nothing to do.");
                return;
            }

            doMigrate(
                    migrationStructures,
                    MigrationStatus.MigrationState.migrating,
                    checkResult.versionToMigrateFrom(),
                    checkResult.versionToMigrateTo(),
                    VisibleMigrationProgressMonitorFactory.forMigration(internalLog),
                    LogsMigrator.CheckResult::migrate);
        }
    }

    public void upgradeIfNeeded() throws UnableToMigrateException {
        if (!storageEngineFactory.storageExists(fs, databaseLayout, pageCache)) {
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

            internalLog.info("'" + checkResult.versionToMigrateFrom()
                    + "' has been identified as the current version of the store");
            internalLog.info("'" + checkResult.versionToMigrateTo()
                    + "' has been identified as the target version of the store upgrade");

            doMigrate(
                    migrationStructures,
                    MigrationStatus.MigrationState.migrating,
                    checkResult.versionToMigrateFrom(),
                    checkResult.versionToMigrateTo(),
                    VisibleMigrationProgressMonitorFactory.forUpgrade(internalLog),
                    LogsMigrator.CheckResult::upgrade);
        }
    }

    private MigrationStructures getMigrationStructures() {
        DatabaseLayout migrationStructure = DatabaseLayout.ofFlat(databaseLayout.file(MIGRATION_DIRECTORY));
        return new MigrationStructures(migrationStructure, migrationStructure.file(MIGRATION_STATUS_FILE));
    }

    private void doMigrate(
            MigrationStructures migrationStructures,
            MigrationStatus.MigrationState migrationState,
            String versionToMigrateFrom,
            String versionToMigrateTo,
            MigrationProgressMonitor progressMonitor,
            LogsAction logsAction) {
        var participants = getStoreMigrationParticipants();
        // One or more participants would like to do migration
        progressMonitor.started(participants.size());

        var logsMigrator = new LogsMigrator(
                fs, storageEngineFactory, databaseLayout, pageCache, config, contextFactory, logTailSupplier);
        var logsCheckResult = logsMigrator.assertCleanlyShutDown();

        // We don't need to migrate if we're at the phase where we have migrated successfully
        // and it's just a matter of moving over the files to the storeDir.
        if (MigrationStatus.MigrationState.migrating.isNeededFor(migrationState)) {
            StoreVersion fromVersion = storageEngineFactory.versionInformation(versionToMigrateFrom);
            StoreVersion toVersion = storageEngineFactory.versionInformation(versionToMigrateTo);
            cleanMigrationDirectory(migrationStructures.migrationLayout.databaseDirectory());
            MigrationStatus.MigrationState.migrating.setMigrationStatus(
                    fs, migrationStructures.migrationStateFile, versionToMigrateFrom, versionToMigrateTo);
            migrateToIsolatedDirectory(
                    participants,
                    databaseLayout,
                    migrationStructures.migrationLayout,
                    fromVersion,
                    toVersion,
                    progressMonitor);
            MigrationStatus.MigrationState.moving.setMigrationStatus(
                    fs, migrationStructures.migrationStateFile, versionToMigrateFrom, versionToMigrateTo);
        }

        if (MigrationStatus.MigrationState.moving.isNeededFor(migrationState)) {
            moveMigratedFilesToStoreDirectory(
                    participants,
                    migrationStructures.migrationLayout,
                    databaseLayout,
                    versionToMigrateFrom,
                    versionToMigrateTo);
        }

        progressMonitor.startTransactionLogsMigration();
        logsAction.handleLogs(logsCheckResult);
        progressMonitor.completeTransactionLogsMigration();

        cleanup(participants, migrationStructures.migrationLayout);

        progressMonitor.completed();
    }

    private CheckResult doMigrationCheck(String formatFamily, CursorContext cursorContext) {
        StoreVersionCheck storeVersionCheck =
                storageEngineFactory.versionCheck(fs, databaseLayout, config, pageCache, logService, contextFactory);
        var checkResult = storeVersionCheck.getAndCheckMigrationTargetVersion(formatFamily, cursorContext);
        return switch (checkResult.outcome()) {
            case MIGRATION_POSSIBLE -> new CheckResult(
                    false, checkResult.versionToMigrateFrom(), checkResult.versionToMigrateTo());
            case NO_OP -> new CheckResult(true, checkResult.versionToMigrateFrom(), checkResult.versionToMigrateTo());
            case STORE_VERSION_RETRIEVAL_FAILURE -> throw new UnableToMigrateException(
                    "Failed to read current store version. This usually indicate a store corruption",
                    checkResult.cause());
            case UNSUPPORTED_MIGRATION_PATH -> throw new UnableToMigrateException(String.format(
                    "Store migration from '%s' to '%s' not supported",
                    checkResult.versionToMigrateFrom(), checkResult.versionToMigrateTo()));
            case UNSUPPORTED_TARGET_VERSION -> throw new UnableToMigrateException(
                    "The current store version is not supported. " + "Please migrate the store to be able to continue");
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
                    "The selected target store format '%s' is no longer supported", checkResult.versionToUpgradeTo()));
        };
    }

    private void finishInterruptedMigration(MigrationStructures migrationStructures) {
        MigrationStatus migrationStatus =
                MigrationStatus.readMigrationStatus(fs, migrationStructures.migrationStateFile);
        if (migrationStatus.migrationInProgress()) {
            MigrationStatus.MigrationState state = migrationStatus.state();
            if (state == MigrationStatus.MigrationState.moving) {
                // If a previous migration was interrupted in the moving phase that move must be completed to have a
                // consistent store again.
                internalLog.info("Resuming migration in progress to '" + migrationStatus.versionToMigrateTo() + "'");
                doMigrate(
                        migrationStructures,
                        MigrationStatus.MigrationState.moving,
                        migrationStatus.versionToMigrateFrom(),
                        migrationStatus.versionToMigrateTo(),
                        VisibleMigrationProgressMonitorFactory.forMigration(internalLog),
                        LogsMigrator.CheckResult
                                ::migrate); // Since we are doing a migration it should be safe to use the
                // LogsMigrator#migrate

                // Have new logTail now, use that one instead
                logTailSupplier = getLogTailSupplier();
            } else {
                removeOldMigrationDir(migrationStructures.migrationLayout.databaseDirectory());
            }
        }
    }

    private void finishInterruptedUpgrade(CursorContext cursorContext, MigrationStructures migrationStructures) {
        MigrationStatus migrationStatus =
                MigrationStatus.readMigrationStatus(fs, migrationStructures.migrationStateFile);
        if (migrationStatus.migrationInProgress()) {
            MigrationStatus.MigrationState state = migrationStatus.state();
            if (state == MigrationStatus.MigrationState.moving) {
                var checkResult = doUpgradeCheck(cursorContext);
                if (!migrationStatus.expectedMigration(checkResult.versionToMigrateTo)) {
                    throw new UnableToMigrateException("A partially complete migration to "
                            + migrationStatus.versionToMigrateTo() + " found when trying to " + "migrate to "
                            + checkResult.versionToMigrateTo + ". Complete that migration before continuing. "
                            + "This can be done by running the migration tool");
                }

                internalLog.info("Resuming upgrade in progress to '" + checkResult.versionToMigrateTo + "'");
                doMigrate(
                        migrationStructures,
                        MigrationStatus.MigrationState.moving,
                        migrationStatus.versionToMigrateFrom(),
                        migrationStatus.versionToMigrateTo(),
                        VisibleMigrationProgressMonitorFactory.forUpgrade(internalLog),
                        LogsMigrator.CheckResult::upgrade);

                // Could have new logTail now, use that one instead
                logTailSupplier = getLogTailSupplier();
            } else {
                removeOldMigrationDir(migrationStructures.migrationLayout.databaseDirectory());
            }
        }
    }

    private List<StoreMigrationParticipant> getStoreMigrationParticipants() {
        // Get all the participants from the storage engine and add them where they want to be
        var storeParticipants = storageEngineFactory.migrationParticipants(
                fs, config, pageCache, jobScheduler, logService, memoryTracker, pageCacheTracer, contextFactory);
        List<StoreMigrationParticipant> participants = new ArrayList<>(storeParticipants);

        // Do individual index provider migration last because they may delete files that we need in earlier steps.
        indexProviderMap.accept(provider -> participants.add(
                provider.storeMigrationParticipant(fs, pageCache, storageEngineFactory, contextFactory)));

        Set<String> participantNames = new HashSet<>();
        participants.forEach(participant -> {
            if (!NOT_PARTICIPATING.equals(participant)) {
                var newParticipantName = participant.getName();
                checkState(
                        !participantNames.contains(newParticipantName),
                        "Migration participants should have unique names. Participant with name: '%s' is already registered.",
                        newParticipantName);
                participantNames.add(newParticipantName);
            }
        });

        return participants;
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
                ProgressReporter progressReporter = progressMonitor.startSection(participant.getName());
                IndexImporterFactory indexImporterFactory = new IndexImporterFactoryImpl();
                participant.migrate(
                        directoryLayout,
                        migrationLayout,
                        progressReporter,
                        fromVersion,
                        toVersion,
                        indexImporterFactory,
                        logTailSupplier.get());
                progressReporter.completed();
            }
        } catch (IOException | UncheckedIOException | KernelException e) {
            throw new UnableToMigrateException("A critical failure during migration has occurred", e);
        }
    }

    private static void moveMigratedFilesToStoreDirectory(
            Iterable<StoreMigrationParticipant> participants,
            DatabaseLayout migrationLayout,
            DatabaseLayout directoryLayout,
            String versionToMigrateFrom,
            String versionToMigrateTo) {
        try {
            for (StoreMigrationParticipant participant : participants) {
                participant.moveMigratedFiles(
                        migrationLayout, directoryLayout, versionToMigrateFrom, versionToMigrateTo);
            }
        } catch (IOException e) {
            throw new UnableToMigrateException(
                    "A critical failure during migration has occurred. Failed to move migrated files into place", e);
        }
    }

    private void checkStoreExists() {
        if (!storageEngineFactory.storageExists(fs, databaseLayout, pageCache)) {
            throw new UnableToMigrateException("Database '" + databaseLayout.getDatabaseName()
                    + "' either does not exists or it has not been initialised");
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

    private Suppliers.Lazy<LogTailMetadata> getLogTailSupplier() {
        return Suppliers.lazySingleton(() -> {
            try {
                return new LogTailExtractor(fs, pageCache, config, storageEngineFactory, databaseTracers)
                        .getTailMetadata(databaseLayout, memoryTracker);
            } catch (Exception e) {
                throw new UnableToMigrateException("Fail to load log tail during migration.", e);
            }
        });
    }

    private record MigrationStructures(DatabaseLayout migrationLayout, Path migrationStateFile) {}

    private record CheckResult(boolean onRequestedVersion, String versionToMigrateFrom, String versionToMigrateTo) {}

    private interface LogsAction {

        void handleLogs(LogsMigrator.CheckResult checkResult);
    }
}
