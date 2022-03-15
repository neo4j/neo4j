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
package org.neo4j.storageengine.api;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.batchimport.Monitor;
import org.neo4j.internal.batchimport.ReadBehaviour;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.lock.LockService;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.Services;
import org.neo4j.storageengine.migration.RollingUpgradeCompatibility;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.token.TokenHolders;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.storage_engine;
import static org.neo4j.internal.helpers.collection.Iterables.single;

/**
 * Represents a type of storage engine and is capable of creating/instantiating {@link StorageEngine storage engines}, where each such
 * {@link StorageEngine} instance represents one storage engine running underneath a particular database. This factory also allows
 * access to recovery status, migration logic and access to meta-data about a storage engine.
 */
@Service
public interface StorageEngineFactory
{
    /**
     * @return the name of this storage engine, which will be used in e.g. storage engine selection and settings.
     */
    String name();

    /**
     * Returns a {@link StoreVersionCheck} which can provide both configured and existing store versions
     * and means of checking upgradability between them.
     * @return StoreVersionCheck to check store version as well as upgradability to other versions.
     */
    StoreVersionCheck versionCheck( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, LogService logService,
            CursorContextFactory contextFactory );

    StoreVersion versionInformation( String storeVersion );

    StoreVersion versionInformation( StoreId storeId );

    StoreVersion defaultStoreVersion();

    RollingUpgradeCompatibility rollingUpgradeCompatibility();

    /**
     * Returns a {@link StoreMigrationParticipant} which will be able to participate in a store migration.
     * @return StoreMigrationParticipant for migration.
     */
    List<StoreMigrationParticipant> migrationParticipants( FileSystemAbstraction fs, Config config, PageCache pageCache,
            JobScheduler jobScheduler, LogService logService, MemoryTracker memoryTracker, PageCacheTracer pageCacheTracer,
            CursorContextFactory contextFactory );

    /**
     * Instantiates a {@link StorageEngine} where all dependencies can be retrieved from the supplied {@code dependencyResolver}.
     *
     * @return the instantiated {@link StorageEngine}.
     */
    StorageEngine instantiate( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, TokenHolders tokenHolders,
            SchemaState schemaState, ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter, LockService lockService,
            IdGeneratorFactory idGeneratorFactory, IdController idController, DatabaseHealth databaseHealth, InternalLogProvider internalLogProvider,
            InternalLogProvider userLogProvider, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            boolean createStoreIfNotExists, DatabaseReadOnlyChecker readOnlyChecker, LogTailMetadata logTailMetadata, MemoryTracker memoryTracker,
            CursorContextFactory contextFactory )
            throws IOException;

    /**
     * Lists files of a specific storage location.
     * @param fileSystem {@link FileSystemAbstraction} this storage is on.
     * @param databaseLayout {@link DatabaseLayout} pointing out its location.
     * @return a {@link List} of {@link Path} instances for the storage files.
     * @throws IOException if there was no storage in this location.
     */
    List<Path> listStorageFiles( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException;

    /**
     * Check if a store described by provided database layout exists in provided file system
     * @param fileSystem store file system
     * @param databaseLayout store database layout
     * @param pageCache page cache to open store with
     * @return true of store exist, false otherwise
     */
    boolean storageExists( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, PageCache pageCache );

    /**
     * Instantiates a read-only {@link TransactionIdStore} to be used outside of a {@link StorageEngine}.
     * @return the read-only {@link TransactionIdStore}.
     * @throws IOException on I/O error or if the store doesn't exist.
     */
    TransactionIdStore readOnlyTransactionIdStore( LogTailMetadata logTailMetadata ) throws IOException;

    /**
     * Instantiates a read-only {@link LogVersionRepository} to be used outside of a {@link StorageEngine}.
     * @return the read-only {@link LogVersionRepository}.
     */
    LogVersionRepository readOnlyLogVersionRepository( LogTailMetadata logTailMetadata );

    /**
     * Instantiates a fully functional {@link MetadataProvider}, which is a union of {@link TransactionIdStore}
     * and {@link LogVersionRepository}.
     * @return a fully functional {@link MetadataProvider}.
     * @throws IOException on I/O error or if the store doesn't exist.
     */
    MetadataProvider transactionMetaDataStore( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache,
            DatabaseReadOnlyChecker readOnlyChecker, CursorContextFactory contextFactory, LogTailMetadata logTailMetadata ) throws IOException;

    StoreId storeId( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext ) throws IOException;

    void resetMetadata( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, CursorContextFactory contextFactory,
            StoreId storeId, UUID externalStoreId ) throws IOException;

    Optional<UUID> databaseIdUuid( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext );

    SchemaRuleMigrationAccess schemaRuleMigrationAccess( FileSystemAbstraction fs, PageCache pageCache, Config config, DatabaseLayout databaseLayout,
            LogService logService, String recordFormats, CursorContextFactory contextFactory, MemoryTracker memoryTracker );

    /**
     * Reads schema rules from 4.4 schema store and ignores malformed rules while doing so.
     */
    List<SchemaRule44> load44SchemaRules( FileSystemAbstraction fs, PageCache pageCache, Config config, DatabaseLayout databaseLayout,
            CursorContextFactory contextFactory, LogTailMetadata logTailMetadata );

    List<SchemaRule> loadSchemaRules( FileSystemAbstraction fs, PageCache pageCache, Config config, DatabaseLayout databaseLayout, boolean lenient,
            Function<SchemaRule,SchemaRule> schemaRuleMigration, CursorContextFactory contextFactory );

    TokenHolders loadReadOnlyTokens( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, boolean lenient,
            CursorContextFactory contextFactory );

    /**
     * Asks this storage engine about the state of a specific store before opening it. If this specific store is missing optional or
     * even perhaps mandatory files in order to properly open it, this is the place to report that.
     *
     * @param fs {@link FileSystemAbstraction} to use for file operations.
     * @param databaseLayout {@link DatabaseLayout} for the location of the database in the file system.
     * @param pageCache {@link PageCache} for any data reading needs.
     * @return the state of the storage files.
     */
    StorageFilesState checkStoreFileState( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache );

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
    DatabaseLayout databaseLayout( Neo4jLayout neo4jLayout, String databaseName );

    BatchImporter batchImporter( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem,
            PageCacheTracer pageCacheTracer, org.neo4j.internal.batchimport.Configuration config, LogService logService,
            PrintStream progressOutput, boolean verboseProgressOutput,
            AdditionalInitialIds additionalInitialIds, Config dbConfig, Monitor monitor,
            JobScheduler jobScheduler, Collector badCollector, LogFilesInitializer logFilesInitializer,
            IndexImporterFactory indexImporterFactory, MemoryTracker memoryTracker, CursorContextFactory contextFactory );

    Input asBatchImporterInput( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem, PageCache pageCache,
            Config config, MemoryTracker memoryTracker, ReadBehaviour readBehaviour, boolean compactNodeIdSpace,
            CursorContextFactory contextFactory, LogTailMetadata logTailMetadata );

    /**
     * Calculates the optimal amount of memory that this consistency checker would want to have to perform optimally in terms of fitting
     * off-heap caches while doing the check.
     *
     * @return the optimal amount of memory that should be available when running consistency checker for this provided db.
     */
    long optimalAvailableConsistencyCheckerMemory( FileSystemAbstraction fs, DatabaseLayout layout, Config config, PageCache pageCache );

    /**
     * Checks consistency of a store.
     *
     * @param fileSystem file system the store is on.
     * @param layout layout of the store.
     * @param config configuration to use.
     * @param pageCache page cache to load pages into.
     * @param indexProviders index providers for accessing indexes to check against the store.
     * @param log to log inconsistencies/warnings to.
     * @param summary to update when finding inconsistencies.
     * @param numberOfThreads max number of threads to use.
     * @param memoryLimitLeewayFactor factor between 0..1 of available machine memory to use.
     * @param progressOutput output where progress of the check is printed, or {@code null} if no progress should be printed.
     * @param verbose whether or not to print verbose progress output.
     * @param flags which parts of the store/indexes to check.
     * @param contextFactory underlying page cursor context factory.
     * @throws ConsistencyCheckIncompleteException on failure doing the consistency check.
     */
    void consistencyCheck( FileSystemAbstraction fileSystem, DatabaseLayout layout, Config config, PageCache pageCache, IndexProviderMap indexProviders,
            InternalLog log, ConsistencySummaryStatistics summary, int numberOfThreads, double memoryLimitLeewayFactor, OutputStream progressOutput,
            boolean verbose, ConsistencyFlags flags, CursorContextFactory contextFactory ) throws ConsistencyCheckIncompleteException;

    /**
     * @return the default {@link StorageEngineFactory}.
     * @throws IllegalStateException if there were no storage engine factories to choose from.
     */
    static StorageEngineFactory defaultStorageEngine()
    {
        return selectStorageEngine( "record" );
    }

    /**
     * @return all {@link StorageEngineFactory} instances that are available on the class path, loaded via {@link Service service loading}.
     */
    static Collection<StorageEngineFactory> allAvailableStorageEngines()
    {
        return Services.loadAll( StorageEngineFactory.class );
    }

    /**
     * @return the first {@link StorageEngineFactory} that returns {@code true} when asked about
     * {@link StorageEngineFactory#storageExists(FileSystemAbstraction, DatabaseLayout, PageCache)} for the given {@code databaseLayout}.
     */
    static Optional<StorageEngineFactory> selectStorageEngine( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache )
    {
        Collection<StorageEngineFactory> storageEngineFactories = allAvailableStorageEngines();
        return storageEngineFactories.stream().filter( engine -> engine.storageExists( fs, databaseLayout, pageCache ) ).findFirst();
    }

    /**
     * @param name the name returned by {@link StorageEngineFactory#name()}.
     * @return the {@link StorageEngineFactory} that has the given {@code name}.
     * @throws IllegalArgumentException if the storage engine with the given {@code name} couldn't be found.
     */
    static StorageEngineFactory selectStorageEngine( String name )
    {
        Collection<StorageEngineFactory> storageEnginesWithThisName =
                allAvailableStorageEngines().stream().filter( e -> e.name().equals( name ) ).collect( Collectors.toList() );
        if ( storageEnginesWithThisName.isEmpty() )
        {
            throw new IllegalArgumentException( "No storage engine matching name '" + name + "'. Available storage engines are: " +
                    allAvailableStorageEngines().stream().map( e -> "'" + e.name() + "'" ).collect( Collectors.joining( ", " ) ) );
        }
        return single( storageEnginesWithThisName );
    }

    /**
     * Selects storage engine which has the name found in the given {@link Configuration}, see {@link GraphDatabaseInternalSettings#storage_engine}.
     *
     * @param configuration the {@link Configuration} to read the name from.
     * @return the {@link StorageEngineFactory} for this name.
     * @throws IllegalArgumentException if no storage engine with the given name was found.
     */
    static StorageEngineFactory selectStorageEngine( Configuration configuration )
    {
        return selectStorageEngine( configuration.get( storage_engine ) );
    }

    /**
     * Selects {@link StorageEngineFactory} first by looking at the store accessible in the provided file system at the given {@code databaseLayout},
     * and if a store exists there and is recognized by any of the available factories will return it. Otherwise the factory specified by the
     * {@code configuration} will be returned.
     *
     * @param configuration the {@link Configuration} to read the name from. This parameter can be {@code null}, which then means to select use the default.
     * @return the found {@link StorageEngineFactory}.
     */
    static StorageEngineFactory selectStorageEngine( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, Configuration configuration )
    {
        return selectStorageEngine( fs, databaseLayout, pageCache, configuration != null ? configuration.get( storage_engine ) : null );
    }

    /**
     * Selects {@link StorageEngineFactory} first by looking at the store accessible in the provided file system at the given {@code databaseLayout},
     * and if a store exists there and is recognized by any of the available factories will return it. Otherwise the factory specified by the
     * {@code configuration} will be returned.
     *
     * @param specificNameOrNull the {@link StorageEngineFactory} name to default to if no store exists and can be recognized by any available factory.
     * This parameter can be {@code null}, which then means to select use the default.
     * @return the found {@link StorageEngineFactory}.
     */
    static StorageEngineFactory selectStorageEngine( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, String specificNameOrNull )
    {
        // - Does a store exist at this location? -> get the one able to open it
        // - Is there a specific name of a storage engine to look for? -> get that one
        // - Use the default one
        Optional<StorageEngineFactory> forExistingStore = StorageEngineFactory.selectStorageEngine( fs, databaseLayout, pageCache );
        if ( forExistingStore.isPresent() )
        {
            return forExistingStore.get();
        }

        if ( isNotEmpty( specificNameOrNull ) )
        {
            return StorageEngineFactory.selectStorageEngine( specificNameOrNull );
        }

        return defaultStorageEngine();
    }

    @FunctionalInterface
    interface Selector
    {
        Optional<StorageEngineFactory> selectStorageEngine( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache );
    }

    Selector SELECTOR = StorageEngineFactory::selectStorageEngine;
}
