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
package org.neo4j.storageengine.api;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.annotations.service.Service;
import org.neo4j.batchimport.api.AdditionalInitialIds;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.IncrementalBatchImporter;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.ReadBehaviour;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.block.BlockDatabaseExistMarker;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersionRepository;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.lock.LockService;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.Services;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccessExtended;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;

/**
 * Represents a type of storage engine and is capable of creating/instantiating {@link StorageEngine storage engines}, where each such
 * {@link StorageEngine} instance represents one storage engine running underneath a particular database. This factory also allows
 * access to recovery status, migration logic and access to meta-data about a storage engine.
 */
@Service
public interface StorageEngineFactory {
    /**
     * @return the name of this storage engine, which will be used in e.g. storage engine selection and settings.
     */
    String name();

    /**
     * @return the unique id of this storage engine which can be used in e.g. storage engine selection
     */
    byte id();

    /**
     * Retrieves the store ID of the store represented by the submitted layout.
     */
    StoreId retrieveStoreId(
            FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext)
            throws IOException;

    /**
     * Returns a {@link StoreVersionCheck} which can provide both configured and existing store versions
     * and means of checking upgradability between them.
     * @return StoreVersionCheck to check store version as well as upgradability to other versions.
     */
    StoreVersionCheck versionCheck(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config config,
            PageCache pageCache,
            LogService logService,
            CursorContextFactory contextFactory);

    /**
     * Finds and returns a store version representation identified by the submitted identifier.
     * <p>
     * A store version does not need to be found if the submitted identifier
     * does not correspond to anything known to these binaries.
     * This can generally happen in cluster-related operations when store version identifiers are sent
     * between cluster members that can be on different versions of the binaries.
     */
    Optional<? extends StoreVersion> versionInformation(StoreVersionIdentifier storeVersionIdentifier);

    /**
     * Returns a {@link StoreMigrationParticipant} which will be able to participate in a store migration.
     * @return StoreMigrationParticipant for migration.
     */
    List<StoreMigrationParticipant> migrationParticipants(
            FileSystemAbstraction fs,
            Config config,
            PageCache pageCache,
            JobScheduler jobScheduler,
            LogService logService,
            MemoryTracker memoryTracker,
            PageCacheTracer pageCacheTracer,
            CursorContextFactory contextFactory,
            boolean forceBtreeIndexesToRange);

    /**
     * Instantiates a {@link StorageEngine} where all dependencies can be retrieved from the supplied {@code dependencyResolver}.
     *
     * @return the instantiated {@link StorageEngine}.
     */
    StorageEngine instantiate(
            FileSystemAbstraction fs,
            Clock clock,
            DatabaseLayout databaseLayout,
            Config config,
            PageCache pageCache,
            TokenHolders tokenHolders,
            SchemaState schemaState,
            ConstraintRuleAccessor constraintSemantics,
            IndexConfigCompleter indexConfigCompleter,
            LockService lockService,
            IdGeneratorFactory idGeneratorFactory,
            DatabaseHealth databaseHealth,
            InternalLogProvider internalLogProvider,
            InternalLogProvider userLogProvider,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            LogTailMetadata logTailMetadata,
            KernelVersionRepository kernelVersionRepository,
            MemoryTracker memoryTracker,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            VersionStorage versionStorage)
            throws IOException;

    /**
     * Lists files of a specific storage location.
     * @param fileSystem {@link FileSystemAbstraction} this storage is on.
     * @param databaseLayout {@link DatabaseLayout} pointing out its location.
     * @return a {@link List} of {@link Path} instances for the storage files.
     * @throws IOException if there was no storage in this location.
     */
    List<Path> listStorageFiles(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout) throws IOException;

    /**
     * Check if a store described by provided database layout exists in provided file system
     * @param fileSystem store file system
     * @param databaseLayout store database layout
     * @return true if store exist, false otherwise
     */
    boolean storageExists(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout);

    /**
     * Check if a store described by provided Neo4j layout, and database name exists in provided file system
     * @param fileSystem store file system
     * @param neo4jLayout store layout
     * @param databaseName store name
     * @return true if store exist, false otherwise
     */
    default boolean storageExists(FileSystemAbstraction fileSystem, Neo4jLayout neo4jLayout, String databaseName) {
        return storageExists(fileSystem, databaseLayout(neo4jLayout, databaseName));
    }

    /**
     * Check if a format is supported by the factory
     *
     * @param format format to check if it is supported
     * @param includeFormatsUnderDevelopment true if this check should include formats under development
     * @return true if supported, false otherwise
     */
    default boolean supportedFormat(String format, boolean includeFormatsUnderDevelopment) {
        return supportedFormats(includeFormatsUnderDevelopment).contains(format);
    }

    /**
     * Get the name of all the supported formats
     * @param includeFormatsUnderDevelopment true if this check should include formats under development
     * @return a Set of format names
     */
    Set<String> supportedFormats(boolean includeFormatsUnderDevelopment);

    /**
     * Get the id limits supported by a format
     * @param formatName format to check limits for
     * @param includeFormatsUnderDevelopment true if this check should include formats under development
     * @return The limits for the format
     * @throws IllegalStateException on unsupported format for engine
     */
    StoreFormatLimits limitsForFormat(String formatName, boolean includeFormatsUnderDevelopment)
            throws IllegalStateException;

    /**
     * Check if the database fits within the provided id limits. This is best effort and not all limits are checked.
     * Assumes that the database exist - can return false for a non-existent/broken database even if it would fit.
     *
     * @param formatLimits   the limits to check against
     * @param databaseLayout layout pointing to the db in question
     * @param config a database config
     * @return Whether the database should fit within the limits.
     */
    boolean fitsWithinStoreFormatLimits(
            StoreFormatLimits formatLimits,
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fs,
            PageCache pageCache,
            Config config)
            throws IOException;

    /**
     * Instantiates a fully functional {@link MetadataProvider}, which is a union of {@link TransactionIdStore}
     * and {@link LogVersionRepository}.
     * @return a fully functional {@link MetadataProvider}.
     * @throws IOException on I/O error or if the store doesn't exist.
     */
    MetadataProvider transactionMetaDataStore(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config config,
            PageCache pageCache,
            DatabaseReadOnlyChecker readOnlyChecker,
            CursorContextFactory contextFactory,
            LogTailLogVersionsMetadata logTailMetadata,
            PageCacheTracer pageCacheTracer)
            throws IOException;

    void resetMetadata(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config config,
            PageCache pageCache,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            StoreId storeId,
            UUID externalStoreId)
            throws IOException;

    Optional<UUID> databaseIdUuid(
            FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext);

    /**
     * Reads schema rules from 4.4 schema store and ignores malformed rules while doing so.
     */
    List<SchemaRule44> load44SchemaRules(
            FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            DatabaseLayout databaseLayout,
            CursorContextFactory contextFactory,
            LogTailLogVersionsMetadata logTailMetadata,
            MemoryTracker memoryTracker);

    List<SchemaRule> loadSchemaRules(
            FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            DatabaseLayout databaseLayout,
            boolean lenient,
            Function<SchemaRule, SchemaRule> schemaRuleMigration,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker);

    TokenHolders loadReadOnlyTokens(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config config,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            boolean lenient,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker);

    SchemaRuleMigrationAccessExtended schemaRuleMigrationAccess(
            FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            DatabaseLayout databaseLayout,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            LogTailMetadata logTail)
            throws IOException;

    /**
     * Asks this storage engine about the state of a specific store before opening it. If this specific store is missing optional or
     * even perhaps mandatory files in order to properly open it, this is the place to report that.
     *
     * @param fs {@link FileSystemAbstraction} to use for file operations.
     * @param databaseLayout {@link DatabaseLayout} for the location of the database in the file system.
     * @param pageCache {@link PageCache} for any data reading needs.
     * @return the state of the storage files.
     */
    StorageFilesState checkStoreFileState(FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache);

    /**
     * @return a {@link CommandReaderFactory} capable of handing out {@link CommandReader} for specific versions. Generally kernel will take care
     * of most of the log entry parsing, i.e. the START, COMMIT, CHECKPOINT commands and their contents (they may be versioned). For COMMAND log entries
     * this returned factory will be used to parse the actual command contents, which are storage-specific. For maximum flexibility the structure should
     * be something like this:
     * <ol>
     *     <li>1B log entry version - managed by kernel</li>
     *     <li>1B log entry type - managed by kernel</li>
     *     <li>For COMMAND log entries: 1B command version - managed by storage</li>
     *     <li>For COMMAND log entries: 1B command type - managed by storage</li>
     *     <li>For COMMAND log entries: command data... - managed by storage</li>
     * </ol>
     *
     * Although currently it's more like this:
     *
     * <ol>
     *     <li>1B log entry version - dictating both log entry version AND command version</li>
     *     <li>1B log entry type - managed by kernel</li>
     *     <li>For COMMAND log entries: command data... - managed by storage, although versioned the same as log entry version</li>
     * </ol>
     */
    CommandReaderFactory commandReaderFactory();

    /**
     * Create a layout representing a database using this storage engine
     * @param neo4jLayout the layout of neo4j
     * @param databaseName the name of the database
     * @return the layout representing the database
     */
    DatabaseLayout databaseLayout(Neo4jLayout neo4jLayout, String databaseName);

    /**
     * Convert a plain database layout into the storage engine specific layout
     * @param plainLayout the layout of database
     * @return the format-specific layout representing the database
     */
    DatabaseLayout formatSpecificDatabaseLayout(DatabaseLayout plainLayout);

    BatchImporter batchImporter(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem,
            PageCacheTracer pageCacheTracer,
            org.neo4j.batchimport.api.Configuration config,
            LogService logService,
            PrintStream progressOutput,
            boolean verboseProgressOutput,
            AdditionalInitialIds additionalInitialIds,
            Config dbConfig,
            Monitor monitor,
            JobScheduler jobScheduler,
            Collector badCollector,
            LogFilesInitializer logFilesInitializer,
            IndexImporterFactory indexImporterFactory,
            MemoryTracker memoryTracker,
            CursorContextFactory contextFactory);

    Input asBatchImporterInput(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            MemoryTracker memoryTracker,
            ReadBehaviour readBehaviour,
            boolean compactNodeIdSpace,
            CursorContextFactory contextFactory,
            LogTailMetadata logTailMetadata);

    IncrementalBatchImporter incrementalBatchImporter(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem,
            PageCacheTracer pageCacheTracer,
            org.neo4j.batchimport.api.Configuration config,
            LogService logService,
            PrintStream progressOutput,
            boolean verboseProgressOutput,
            AdditionalInitialIds additionalInitialIds,
            ThrowingSupplier<LogTailMetadata, IOException> logTailMetadataSupplier,
            Config dbConfig,
            Monitor monitor,
            JobScheduler jobScheduler,
            Collector badCollector,
            LogFilesInitializer logFilesInitializer,
            IndexImporterFactory indexImporterFactory,
            MemoryTracker memoryTracker,
            CursorContextFactory contextFactory,
            IndexProvidersAccess indexProvidersAccess);

    LockManager createLockManager(Config config, SystemNanoClock clock);

    /**
     * Calculates the optimal amount of memory that this consistency checker would want to have to perform optimally in terms of fitting
     * off-heap caches while doing the check.
     *
     * @return the optimal amount of memory that should be available when running consistency checker for this provided db.
     */
    long optimalAvailableConsistencyCheckerMemory(
            FileSystemAbstraction fs, DatabaseLayout layout, Config config, PageCache pageCache);

    /**
     * Checks consistency of a store.
     *
     * @param fileSystem file system the store is on.
     * @param layout layout of the store.
     * @param config configuration to use.
     * @param pageCache page cache to load pages into.
     * @param indexProviders index providers for accessing indexes to check against the store.
     * @param reportLog to log inconsistencies/warnings to.
     * @param verboseLog to log verbose debug info
     * @param summary to update when finding inconsistencies.
     * @param numberOfThreads max number of threads to use.
     * @param maxOffHeapCachingMemory max amount of off-heap memory that the checker can allocate for caching data.
     * @param progressOutput output where progress of the check is printed, or {@code null} if no progress should be printed.
     * @param verbose whether or not to print verbose progress output.
     * @param flags which parts of the store/indexes to check.
     * @param contextFactory underlying page cursor context factory.
     * @param pageCacheTracer underlying page cache tracer
     * @param logTailMetadata meta data read from the tx log.
     * @param memoryTracker
     * @throws ConsistencyCheckIncompleteException on failure doing the consistency check.
     */
    void consistencyCheck(
            FileSystemAbstraction fileSystem,
            DatabaseLayout layout,
            Config config,
            PageCache pageCache,
            IndexProviderMap indexProviders,
            InternalLog reportLog,
            InternalLog verboseLog,
            ConsistencySummaryStatistics summary,
            int numberOfThreads,
            long maxOffHeapCachingMemory,
            OutputStream progressOutput,
            boolean verbose,
            ConsistencyFlags flags,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            LogTailMetadata logTailMetadata,
            MemoryTracker memoryTracker)
            throws ConsistencyCheckIncompleteException;

    /**
     * Detects open options for existing store such as endianness or version
     */
    ImmutableSet<OpenOption> getStoreOpenOptions(
            FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, CursorContextFactory contextFactory);

    /**
     * Return itself or original StorageEngineFactory if wraps one
     */
    StorageEngineFactory unwrap();

    /**
     * @return the default {@link StorageEngineFactory}.
     * @throws IllegalStateException if there were no storage engine factories to choose from.
     */
    static StorageEngineFactory defaultStorageEngine() {
        String name = "record";
        return allAvailableStorageEngines().stream()
                .filter(e -> e.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "No storage engine matching name '" + name + "'. Available storage engines are: "
                                + allAvailableStorageEngines().stream()
                                        .map(e -> "'" + e.name() + "'")
                                        .collect(Collectors.joining(", "))));
    }

    /**
     * @return all {@link StorageEngineFactory} instances that are available on the class path, loaded via {@link Service service loading}.
     */
    static Collection<StorageEngineFactory> allAvailableStorageEngines() {
        return StorageEngineFactoryHolder.ALL_ENGINE_FACTORIES;
    }

    /**
     * @return the first {@link StorageEngineFactory} that returns {@code true} when asked about
     * {@link StorageEngineFactory#storageExists(FileSystemAbstraction, DatabaseLayout)} for the given {@code databaseLayout}.
     */
    static Optional<StorageEngineFactory> selectStorageEngine(FileSystemAbstraction fs, DatabaseLayout databaseLayout) {
        return allAvailableStorageEngines().stream()
                .filter(engine -> engine.storageExists(fs, databaseLayout))
                .findFirst();
    }

    /**
     * @return the first {@link StorageEngineFactory} that returns {@code true} when asked about
     * {@link StorageEngineFactory#storageExists(FileSystemAbstraction, Neo4jLayout, String)} for the given
     * {@code neo4jLayout} and {@code databaseName}.
     */
    static Optional<StorageEngineFactory> selectStorageEngine(
            FileSystemAbstraction fs, Neo4jLayout neo4jLayout, String databaseName) {
        return allAvailableStorageEngines().stream()
                .filter(engine -> engine.storageExists(fs, neo4jLayout, databaseName))
                .findFirst();
    }

    /**
     * Selects storage engine which matches the store format found in the given {@link Configuration}, see {@link GraphDatabaseSettings#db_format}.
     *
     * @param configuration the {@link Configuration} to read the name from.
     * @return the {@link StorageEngineFactory} for this name.
     * @throws IllegalArgumentException if no storage engine matching the store format with the given name was found.
     */
    static StorageEngineFactory selectStorageEngine(Configuration configuration) {
        return findEngineForFormatOrThrow(configuration);
    }

    /**
     * Selects {@link StorageEngineFactory} first by looking at the store accessible in the provided file system at the given {@code databaseLayout},
     * and if a store exists there and is recognized by any of the available factories will return it. Otherwise the factory specified by the
     * {@code configuration} will be returned.
     *
     * @param configuration the {@link Configuration} to read the name from. This parameter can be {@code null}, which then means to select use the default.
     * @return the found {@link StorageEngineFactory}.
     */
    static StorageEngineFactory selectStorageEngine(
            FileSystemAbstraction fs, DatabaseLayout databaseLayout, Configuration configuration) {
        // - Does a store exist at this location? -> get the one able to open it
        // - Is there a specific name of a store format to look for? -> get the storage engine that recognizes it
        // (except for system that should use default)
        // - Use the default one
        return selectStorageEngine(fs, databaseLayout).orElseGet(() -> {
            validateNotKnownFormat(fs, databaseLayout);
            return configuration == null
                            || GraphDatabaseSettings.SYSTEM_DATABASE_NAME.equals(databaseLayout.getDatabaseName())
                    ? defaultStorageEngine()
                    : findEngineForFormatOrThrow(configuration);
        });
    }

    private static void validateNotKnownFormat(FileSystemAbstraction fs, DatabaseLayout databaseLayout) {
        if (fs.isDirectory(databaseLayout.databaseDirectory())) {
            assert selectStorageEngine(fs, databaseLayout).isEmpty();
            if (fs.fileExists(databaseLayout.file(BlockDatabaseExistMarker.INSTANCE))) {
                throw new IllegalArgumentException("Block format detected for database "
                        + databaseLayout.getDatabaseName() + " but unavailable in this edition.");
            }
        }
    }

    /**
     * @return the {@link StorageEngineFactory} with the corresponding ID
     * @throws IllegalArgumentException if no storage engine matching the id was found.
     */
    static StorageEngineFactory selectStorageEngine(byte id) {
        return allAvailableStorageEngines().stream()
                .filter(engine -> engine.id() == id)
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(
                        "No storage engine factory with id " + id + ". Available engines are: "
                                + allAvailableStorageEngines().stream()
                                        .map(e -> e.name() + ":" + e.id())
                                        .collect(Collectors.joining(", "))));
    }

    private static StorageEngineFactory findEngineForFormatOrThrow(Configuration configuration) {
        String name = configuration.get(GraphDatabaseSettings.db_format);
        boolean includeDevFormats = configuration.get(GraphDatabaseInternalSettings.include_versions_under_development);
        return allAvailableStorageEngines().stream()
                .filter(engine -> engine.supportedFormat(name, includeDevFormats))
                .findFirst()
                .orElseThrow(() -> {
                    String allFormats = allAvailableStorageEngines().stream()
                            .flatMap(sef -> sef.supportedFormats(includeDevFormats).stream())
                            .distinct()
                            .collect(Collectors.joining("', '", "'", "'"));
                    return new IllegalArgumentException(String.format(
                            "No supported database format '%s'. Available formats are: %s.", name, allFormats));
                });
    }

    @FunctionalInterface
    interface Selector {
        Optional<StorageEngineFactory> selectStorageEngine(FileSystemAbstraction fs, DatabaseLayout databaseLayout);
    }

    Selector SELECTOR = StorageEngineFactory::selectStorageEngine;

    final class StorageEngineFactoryHolder {
        static final Collection<StorageEngineFactory> ALL_ENGINE_FACTORIES = loadFactories();

        private static Collection<StorageEngineFactory> loadFactories() {
            return Services.loadAll(StorageEngineFactory.class);
        }
    }
}
