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
package org.neo4j.commandline.dbms;

import static java.lang.String.format;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.io.pagecache.context.EmptyVersionContextSupplier.EMPTY;
import static org.neo4j.kernel.impl.factory.DbmsInfo.TOOL;
import static org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory.createPageCache;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import org.eclipse.collections.impl.set.mutable.MutableSetFactoryImpl;
import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.cli.Converters;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.collection.Dependencies;
import org.neo4j.commandline.Util;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.DatabaseNamePattern;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.graphdb.facade.SystemDbUpgrader;
import org.neo4j.graphdb.factory.module.edition.migration.MigrationEditionModuleFactory;
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
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionFailureStrategies;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
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

// TODO: let's wait with better description until the discussion with the PM is over
@Command(name = "migrate-store", header = "Migrate database store", description = "Migrate database store")
public class MigrateStoreCommand extends AbstractCommand {
    @Option(
            names = "--database",
            paramLabel = "<database>",
            defaultValue = DEFAULT_DATABASE_NAME,
            description = "Name of the database whose store to migrate. Can contain * and ? for globbing.",
            converter = Converters.DatabaseNamePatternConverter.class)
    private DatabaseNamePattern database;

    //    @Option( names = "--storage-engine", paramLabel = "<storage engine>", description = "Storage engine to migrate
    // the store to" )
    //    private String storageEngine;

    @Option(
            names = "--format-family",
            paramLabel = "<format family>",
            description = "Format family to migrate the store to. "
                    + "This option is supported only in combination with --storage-engine RECORD")
    private String formatFamily;

    @Option(
            names = "--pagecache",
            paramLabel = "<size>",
            defaultValue = "8m",
            description = "The size of the page cache to use for the backup process.")
    private String pagecacheMemory;

    @Option(
            names = "--additional-config",
            paramLabel = "<path>",
            description = "Configuration file to supply additional configuration in.")
    private Path additionalConfig;

    @Option(
            names = "--forceBtreeIndexesToRange",
            hidden = true,
            description = "Special option for turning all btree indexes/constraints into range")
    private boolean forceBtreeToRange;

    public MigrateStoreCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    protected void execute() {
        Config config = buildConfig(ctx, allowCommandExpansion);
        try (Log4jLogProvider logProvider = Util.configuredLogProvider(config, ctx.out());
                Log4jLogProvider systemDbStartupLogProvider =
                        createSystemDbStartupLogProvider(config, ctx, logProvider)) {
            migrateStore(config, logProvider);
            if (SYSTEM_DATABASE_NAME.equals(database.getDatabaseName())) {
                upgradeSystemDb(config, logProvider, systemDbStartupLogProvider);
            }
        }
    }

    private void migrateStore(Config config, Log4jLogProvider logProvider) {
        var databaseTracers = DatabaseTracers.EMPTY;
        var pageCacheTracer = PageCacheTracer.NULL;
        var memoryTracker = EmptyMemoryTracker.INSTANCE;
        var contextFactory = new CursorContextFactory(PageCacheTracer.NULL, EMPTY);

        List<FailedMigration> failedMigrations = new ArrayList<>();

        InternalLog resultLog = logProvider.getLog(getClass());
        try (FileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            Set<String> dbNames = getDbNames(config, fs);

            for (String dbName : dbNames) {

                resultLog.info("Starting migration for database '" + dbName + "'");

                LifeSupport life = new LifeSupport();
                String formatFamilyForDb = formatFamily;

                try (JobScheduler jobScheduler = life.add(JobSchedulerFactory.createInitialisedScheduler());
                        PageCache pageCache = createPageCache(fs, config, jobScheduler, pageCacheTracer)) {

                    DatabaseLayout databaseLayout = Neo4jLayout.of(config).databaseLayout(dbName);
                    checkDatabaseExistence(databaseLayout);

                    if (SYSTEM_DATABASE_NAME.equals(dbName)) {
                        formatFamilyForDb = GraphDatabaseSettings.DatabaseRecordFormat.aligned.name();
                    }

                    try (Closeable ignored = LockChecker.checkDatabaseLock(databaseLayout)) {
                        SimpleLogService logService = new SimpleLogService(logProvider);

                        StorageEngineFactory currentStorageEngineFactory =
                                getCurrentStorageEngineFactory(fs, databaseLayout, pageCache);
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
                                contextFactory);

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
                                indexProviderMap,
                                contextFactory,
                                memoryTracker,
                                Suppliers.lazySingleton(() -> loadLogTail(
                                        fs,
                                        pageCache,
                                        config,
                                        currentStorageEngineFactory,
                                        DatabaseTracers.EMPTY,
                                        databaseLayout,
                                        memoryTracker)));

                        storeMigrator.migrateIfNeeded(formatFamilyForDb, forceBtreeToRange);
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

        if (failedMigrations.isEmpty()) {
            resultLog.info("Migrate-store completed successfully");
        } else {
            StringJoiner failedDbs = new StringJoiner("', '", "Migrate-store failed for databases: '", "'");
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

    private Set<String> getDbNames(Config config, FileSystemAbstraction fs) {
        if (!database.containsPattern()) {
            return Set.of(database.getDatabaseName());
        } else {
            Set<String> dbNames = MutableSetFactoryImpl.INSTANCE.empty();
            Path databasesDir = Neo4jLayout.of(config).databasesDirectory();
            try {
                for (Path path : fs.listFiles(databasesDir)) {
                    if (fs.isDirectory(path)) {
                        String name = path.getFileName().toString();
                        if (database.matches(name)) {
                            dbNames.add(name);
                        }
                    }
                }
            } catch (IOException e) {
                throw new CommandFailedException(
                        format("Failed to list databases: %s: %s", e.getClass().getSimpleName(), e.getMessage()), e);
            }
            return dbNames;
        }
    }

    private static void upgradeSystemDb(
            Config config, Log4jLogProvider logProvider, Log4jLogProvider systemDbStartupLogProvider) {
        try {
            var editionModuleFactory = loadEditionModuleFactory();
            SystemDbUpgrader.upgrade(
                    editionModuleFactory,
                    config,
                    logProvider,
                    systemDbStartupLogProvider,
                    new DatabaseEventListenerAdapter());
        } catch (Exception e) {
            throw new CommandFailedException(e.getMessage(), e);
        }
    }

    private static Log4jLogProvider createSystemDbStartupLogProvider(
            Config config, ExecutionContext ctx, Log4jLogProvider logProvider) {
        if (config.get(GraphDatabaseSettings.store_internal_log_level) == Level.DEBUG) {
            return logProvider;
        }
        var errorLevelConfig = Config.newBuilder()
                .fromConfig(config)
                .set(GraphDatabaseSettings.store_internal_log_level, Level.ERROR)
                .build();
        return Util.configuredLogProvider(errorLevelConfig, ctx.out());
    }

    private static MigrationEditionModuleFactory loadEditionModuleFactory() {
        var editionModuleFactory = Services.loadByPriority(MigrationEditionModuleFactory.class);
        return editionModuleFactory.orElseThrow(() -> new IllegalStateException(
                "Could not find any implementations of " + MigrationEditionModuleFactory.class));
    }

    private LogTailMetadata loadLogTail(
            FileSystemAbstraction fs,
            PageCache pageCache,
            Config config,
            StorageEngineFactory engineFactory,
            DatabaseTracers databaseTracers,
            DatabaseLayout layout,
            MemoryTracker memoryTracker) {
        try {
            return new LogTailExtractor(fs, pageCache, config, engineFactory, databaseTracers)
                    .getTailMetadata(layout, memoryTracker);
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
            FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache) {
        return StorageEngineFactory.selectStorageEngine(fs, databaseLayout, pageCache)
                .orElseThrow(() -> new CommandFailedException(
                        "Current store format has not been recognised by any of the available storage engines"));
    }

    private Config buildConfig(ExecutionContext ctx, boolean allowCommandExpansion) {
        var configBuilder = Config.newBuilder()
                .fromFileNoThrow(ctx.confDir().resolve(Config.DEFAULT_CONFIG_FILE_NAME))
                .fromFileNoThrow(additionalConfig)
                .commandExpansion(allowCommandExpansion)
                .set(GraphDatabaseSettings.neo4j_home, ctx.homeDir())
                .set(pagecache_memory, ByteUnit.parse(pagecacheMemory))
                .set(GraphDatabaseSettings.read_only_database_default, true);

        if (verbose) {
            configBuilder.set(GraphDatabaseSettings.store_internal_log_level, Level.DEBUG);
        }

        Config cfg = configBuilder.build();
        ConfigUtils.disableAllConnectors(cfg);

        return cfg;
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
            CursorContextFactory contextFactory) {
        var recoveryCleanupWorkCollector = RecoveryCleanupWorkCollector.ignore();
        var monitors = new Monitors();
        var tokenHolders =
                storageEngineFactory.loadReadOnlyTokens(fs, databaseLayout, config, pageCache, true, contextFactory);
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
                TOOL,
                recoveryCleanupWorkCollector,
                databaseLayout,
                tokenHolders,
                jobScheduler,
                contextFactory,
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
        Dependencies deps = new Dependencies();
        deps.satisfyDependencies(
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
