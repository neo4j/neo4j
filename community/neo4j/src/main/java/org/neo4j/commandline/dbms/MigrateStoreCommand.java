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
package org.neo4j.commandline.dbms;

import static java.lang.String.format;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory.createPageCache;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import org.neo4j.cli.AbstractAdminCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.graphdb.facade.SystemDbUpgrader;
import org.neo4j.graphdb.factory.module.edition.migration.MigrationEditionModuleFactory;
import org.neo4j.graphdb.factory.module.edition.migration.SystemDatabaseMigrator;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.locker.FileLockException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.FixedVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionFailureStrategies;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.UnableToMigrateException;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMap;
import org.neo4j.kernel.impl.transaction.state.StaticIndexProviderMapFactory;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.recovery.LogTailExtractor;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Level;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.Services;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.token.TokenHolders;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(
        name = "migrate",
        header = "Migrate a database",
        description = "Migrates a database from one format to another or between versions of the same format. "
                + "It always migrates the database to the latest combination of major and minor "
                + "version of the target format.")
public class MigrateStoreCommand extends AbstractAdminCommand {
    @Parameters(
            arity = "1",
            paramLabel = "<database>",
            description = "Name of the database to migrate. Can contain * and ? for globbing. "
                    + "Note that * and ? have special meaning in some shells "
                    + "and might need to be escaped or used with quotes.",
            converter = Converters.DatabaseNamePatternConverter.class)
    private DatabaseNamePattern database;

    @Option(
            names = "--to-format",
            paramLabel = "standard|high_limit|aligned|block",
            description =
                    "Name of the format to migrate the store to. "
                            + "If the format is specified, the target database is migrated to the latest known combination of MAJOR and MINOR versions of the specified format. "
                            + "If not specified, the tool migrates the target database to the latest known combination of MAJOR and MINOR versions of the current format.")
    private String formatToMigrateTo;

    @Option(
            names = "--pagecache",
            paramLabel = "<size>",
            description = "The size of the page cache to use for the migration process. "
                    + "The general rule is that values up to the size of the database proportionally increase "
                    + "performance.")
    private String pagecacheMemory;

    @Option(
            names = "--force-btree-indexes-to-range",
            fallbackValue = "true",
            description = "Special option for automatically turning all BTREE indexes/constraints into RANGE. "
                    + "Be aware that RANGE indexes are not always the optimal replacement of BTREEs "
                    + "and performance may be affected while the new indexes are populated. "
                    + "See the Neo4j v5 migration guide online for more information. "
                    + "The newly created indexes will be populated in the background on the first database start up "
                    + "following the migration and users should monitor the successful completion of that process.")
    private boolean forceBtreeToRange;

    @Option(
            names = "--force-system-database",
            hidden = true,
            fallbackValue = "true",
            description = "A special option for forcing migration of Enterprise System database.")
    protected boolean forceSystemDatabase;

    @Option(
            names = "--keep-node-ids",
            hidden = true,
            fallbackValue = "true",
            description = "If the node id space should not be compacted during a migration that switches format. "
                    + "Only applies when changing storage engine.")
    private boolean keepNodeIds;

    public MigrateStoreCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    protected Optional<String> commandConfigName() {
        return Optional.of("database-migrate");
    }

    @Override
    protected void execute() {
        Config config = buildConfig();
        try (Log4jLogProvider logProvider = new Log4jLogProvider(ctx.out(), verbose ? Level.DEBUG : Level.INFO)) {
            migrateStore(config, logProvider);
        }
    }

    protected void checkAllowedToMigrateSystemDb(
            StorageEngineFactory storageEngineFactory,
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            PageCache pageCache,
            CursorContextFactory contextFactory)
            throws UnableToMigrateException {}

    private void migrateStore(Config config, Log4jLogProvider logProvider) {
        var databaseTracers = DatabaseTracers.EMPTY;
        var pageCacheTracer = PageCacheTracer.NULL;
        var memoryTracker = EmptyMemoryTracker.INSTANCE;
        var contextFactory =
                new CursorContextFactory(PageCacheTracer.NULL, new FixedVersionContextSupplier(BASE_TX_ID));

        List<FailedMigration> failedMigrations = new ArrayList<>();

        InternalLog resultLog = logProvider.getLog(getClass());
        try (FileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            Set<String> dbNames = getDbNames(config, fs, database);

            for (String dbName : dbNames) {

                resultLog.info("Starting migration for database '" + dbName + "'");

                LifeSupport life = new LifeSupport();
                String formatForDb = formatToMigrateTo;

                try (JobScheduler jobScheduler = life.add(JobSchedulerFactory.createInitialisedScheduler());
                        PageCache pageCache = createPageCache(fs, config, jobScheduler, pageCacheTracer)) {

                    DatabaseLayout databaseLayout = Neo4jLayout.of(config).databaseLayout(dbName);
                    checkDatabaseExistence(databaseLayout);

                    try (Closeable ignored = LockChecker.checkDatabaseLock(databaseLayout)) {
                        SimpleLogService logService = new SimpleLogService(logProvider);

                        StorageEngineFactory currentStorageEngineFactory =
                                getCurrentStorageEngineFactory(fs, databaseLayout);

                        if (SYSTEM_DATABASE_NAME.equals(dbName)) {
                            formatForDb = "aligned";

                            checkAllowedToMigrateSystemDb(
                                    currentStorageEngineFactory, fs, databaseLayout, pageCache, contextFactory);
                        }

                        StorageEngineFactory targetStorageEngineFactory = formatForDb == null
                                ? currentStorageEngineFactory
                                : StorageEngineFactory.selectStorageEngine(Config.newBuilder()
                                        .fromConfig(config)
                                        .set(GraphDatabaseSettings.db_format, formatForDb)
                                        .build());

                        var indexProviderMap = getIndexProviderMap(
                                fs,
                                databaseLayout,
                                config,
                                logService,
                                pageCache,
                                jobScheduler,
                                life,
                                currentStorageEngineFactory,
                                pageCacheTracer,
                                contextFactory,
                                memoryTracker);

                        // Add the kernel store migrator
                        life.start();

                        StoreMigrator storeMigrator = new StoreMigrator(
                                fs,
                                config,
                                logService,
                                pageCache,
                                databaseTracers,
                                jobScheduler,
                                databaseLayout,
                                currentStorageEngineFactory,
                                targetStorageEngineFactory,
                                indexProviderMap,
                                memoryTracker,
                                Suppliers.lazySingleton(() -> loadLogTail(
                                        fs,
                                        config,
                                        currentStorageEngineFactory,
                                        DatabaseTracers.EMPTY,
                                        databaseLayout,
                                        memoryTracker)));

                        storeMigrator.migrateIfNeeded(formatForDb, forceBtreeToRange, keepNodeIds);
                    } catch (FileLockException e) {
                        throw new CommandFailedException(
                                "The database is in use. Stop database '" + dbName + "' and try again.", e);
                    }
                } catch (Exception e) {
                    resultLog.error("Failed to migrate database '" + dbName + "': " + e.getMessage());
                    failedMigrations.add(new FailedMigration(dbName, e));
                } finally {
                    life.shutdown();
                }
            }
        } catch (IOException e) {
            throw new CommandFailedException(
                    format("Failed to migrate database(s): %s: %s", e.getClass().getSimpleName(), e.getMessage()), e);
        }

        if (database.matches(SYSTEM_DATABASE_NAME)
                && failedMigrations.stream()
                        .noneMatch(failedMigration -> SYSTEM_DATABASE_NAME.equals(failedMigration.dbName))) {
            try (Log4jLogProvider systemDbStartupLogProvider =
                    new Log4jLogProvider(ctx.out(), verbose ? Level.DEBUG : Level.ERROR)) {
                upgradeSystemDb(config, logProvider, systemDbStartupLogProvider);
            } catch (Exception e) {
                resultLog.error("Failed to migrate database '" + SYSTEM_DATABASE_NAME + "': " + e.getMessage());
                failedMigrations.add(new FailedMigration(SYSTEM_DATABASE_NAME, e));
            }
        }

        if (failedMigrations.isEmpty()) {
            resultLog.info("Database migration completed successfully");
        } else {
            StringJoiner failedDbs = new StringJoiner("', '", "Migration failed for databases: '", "'");
            Exception exceptions = null;
            for (FailedMigration failedMigration : failedMigrations) {
                failedDbs.add(failedMigration.dbName);
                exceptions = Exceptions.chain(exceptions, failedMigration.e);
            }
            resultLog.error(failedDbs.toString());
            throw new CommandFailedException(failedDbs.toString(), exceptions);
        }
    }

    record FailedMigration(String dbName, Exception e) {}

    private static void upgradeSystemDb(
            Config config, Log4jLogProvider logProvider, Log4jLogProvider systemDbStartupLogProvider) {
        try {
            var editionModuleFactory = loadEditionModuleFactory();
            var systemDatabaseMigrator = loadSystemDatabaseMigrator();
            SystemDbUpgrader.upgrade(
                    editionModuleFactory,
                    systemDatabaseMigrator,
                    config,
                    logProvider,
                    systemDbStartupLogProvider,
                    new DatabaseEventListenerAdapter());
        } catch (Exception e) {
            throw new CommandFailedException(e.getMessage(), e);
        }
    }

    private static MigrationEditionModuleFactory loadEditionModuleFactory() {
        var editionModuleFactory = Services.loadByPriority(MigrationEditionModuleFactory.class);
        return editionModuleFactory.orElseThrow(() -> new IllegalStateException(
                "Could not find any implementations of " + MigrationEditionModuleFactory.class));
    }

    private static SystemDatabaseMigrator loadSystemDatabaseMigrator() {
        var systemDatabaseMigrator = Services.loadByPriority(SystemDatabaseMigrator.class);
        return systemDatabaseMigrator.orElseThrow(() ->
                new IllegalStateException("Could not find any implementations of " + SystemDatabaseMigrator.class));
    }

    private LogTailMetadata loadLogTail(
            FileSystemAbstraction fs,
            Config config,
            StorageEngineFactory engineFactory,
            DatabaseTracers databaseTracers,
            DatabaseLayout layout,
            MemoryTracker memoryTracker) {
        try {
            // If empty tx logs are allowed, and we don't have tx logs we fall back to the latest kernel version.
            // That should be safe since we are trying to migrate to that version anyway.
            return new LogTailExtractor(fs, config, engineFactory, databaseTracers)
                    .getTailMetadata(layout, memoryTracker, () -> KernelVersion.getLatestVersion(config));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void checkDatabaseExistence(DatabaseLayout databaseLayout) {
        try {
            Validators.CONTAINS_EXISTING_DATABASE.validate(databaseLayout.databaseDirectory());
        } catch (IllegalArgumentException e) {
            throw new CommandFailedException("Database '" + databaseLayout.getDatabaseName() + "' does not exist", e);
        }
    }

    private StorageEngineFactory getCurrentStorageEngineFactory(
            FileSystemAbstraction fs, DatabaseLayout databaseLayout) {
        return StorageEngineFactory.selectStorageEngine(fs, databaseLayout)
                .orElseThrow(() -> new CommandFailedException(
                        "Current store format has not been recognised by any of the available storage engines"));
    }

    private Config buildConfig() {
        try {
            var builder = createPrefilledConfigBuilder();
            if (pagecacheMemory != null) {
                builder.set(pagecache_memory, ByteUnit.parse(pagecacheMemory));
            }
            return builder.build();
        } catch (Exception e) {
            throw new CommandFailedException(e.getMessage(), e);
        }
    }

    private static StaticIndexProviderMap getIndexProviderMap(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config config,
            LogService logService,
            PageCache pageCache,
            JobScheduler jobScheduler,
            LifeSupport life,
            StorageEngineFactory storageEngineFactory,
            PageCacheTracer pageCacheTracer,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        var recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.ignore();
        var monitors = new Monitors();
        var tokenHolders = storageEngineFactory.loadReadOnlyTokens(
                fs, databaseLayout, config, pageCache, pageCacheTracer, true, contextFactory, memoryTracker);
        var extensions = life.add(instantiateExtensions(
                fs,
                databaseLayout,
                config,
                logService,
                pageCache,
                jobScheduler,
                recoveryCleanupWorkCollector,
                // We use TOOL context because it's true, and also because it uses the 'single' operational mode, which
                // is important.
                monitors,
                tokenHolders,
                pageCacheTracer,
                readOnly()));
        return life.add(StaticIndexProviderMapFactory.create(
                life,
                config,
                pageCache,
                fs,
                logService,
                monitors,
                readOnly(),
                HostedOnMode.SINGLE,
                recoveryCleanupWorkCollector,
                databaseLayout,
                tokenHolders,
                jobScheduler,
                contextFactory,
                pageCacheTracer,
                extensions));
    }

    private static DatabaseExtensions instantiateExtensions(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config config,
            LogService logService,
            PageCache pageCache,
            JobScheduler jobScheduler,
            RecoveryCleanupWorkCollector recoveryCollector,
            Monitors monitors,
            TokenHolders tokenHolders,
            PageCacheTracer pageCacheTracer,
            DatabaseReadOnlyChecker readOnlyChecker) {
        var deps = dependenciesOf(
                fs,
                config,
                logService,
                pageCache,
                recoveryCollector,
                monitors,
                jobScheduler,
                tokenHolders,
                pageCacheTracer,
                databaseLayout,
                readOnlyChecker);
        @SuppressWarnings("rawtypes")
        Iterable extensions = Services.loadAll(ExtensionFactory.class);
        DatabaseExtensionContext extensionContext = new DatabaseExtensionContext(databaseLayout, DbmsInfo.TOOL, deps);
        return new DatabaseExtensions(extensionContext, extensions, deps, ExtensionFailureStrategies.ignore());
    }
}
