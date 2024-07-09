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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.batchimport.api.AdditionalInitialIds;
import org.neo4j.batchimport.api.BatchImporter;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.batchimport.api.IncrementalBatchImporter;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.batchimport.api.Monitor;
import org.neo4j.batchimport.api.ReadBehaviour;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
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
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccessExtended;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;

public class DelegatingStorageEngineFactory implements StorageEngineFactory {
    private final StorageEngineFactory delegate;

    public DelegatingStorageEngineFactory(StorageEngineFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public byte id() {
        return delegate.id();
    }

    @Override
    public StoreId retrieveStoreId(
            FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext)
            throws IOException {
        return delegate.retrieveStoreId(fs, databaseLayout, pageCache, cursorContext);
    }

    @Override
    public StoreVersionCheck versionCheck(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config config,
            PageCache pageCache,
            LogService logService,
            CursorContextFactory contextFactory) {
        return delegate.versionCheck(fs, databaseLayout, config, pageCache, logService, contextFactory);
    }

    @Override
    public Optional<? extends StoreVersion> versionInformation(StoreVersionIdentifier storeVersionIdentifier) {
        return delegate.versionInformation(storeVersionIdentifier);
    }

    @Override
    public List<StoreMigrationParticipant> migrationParticipants(
            FileSystemAbstraction fs,
            Config config,
            PageCache pageCache,
            JobScheduler jobScheduler,
            LogService logService,
            MemoryTracker memoryTracker,
            PageCacheTracer pageCacheTracer,
            CursorContextFactory contextFactory,
            boolean forceBtreeIndexesToRange) {
        return delegate.migrationParticipants(
                fs,
                config,
                pageCache,
                jobScheduler,
                logService,
                memoryTracker,
                pageCacheTracer,
                contextFactory,
                forceBtreeIndexesToRange);
    }

    @Override
    public StorageEngine instantiate(
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
            throws IOException {
        return delegate.instantiate(
                fs,
                clock,
                databaseLayout,
                config,
                pageCache,
                tokenHolders,
                schemaState,
                constraintSemantics,
                indexConfigCompleter,
                lockService,
                idGeneratorFactory,
                databaseHealth,
                internalLogProvider,
                userLogProvider,
                recoveryCleanupWorkCollector,
                logTailMetadata,
                kernelVersionRepository,
                memoryTracker,
                contextFactory,
                pageCacheTracer,
                versionStorage);
    }

    @Override
    public List<Path> listStorageFiles(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout)
            throws IOException {
        return delegate.listStorageFiles(fileSystem, databaseLayout);
    }

    @Override
    public boolean storageExists(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout) {
        return delegate.storageExists(fileSystem, databaseLayout);
    }

    @Override
    public boolean storageExists(FileSystemAbstraction fileSystem, Neo4jLayout neo4jLayout, String databaseName) {
        return delegate.storageExists(fileSystem, neo4jLayout, databaseName);
    }

    @Override
    public boolean supportedFormat(String format, boolean includeFormatsUnderDevelopment) {
        return delegate.supportedFormat(format, includeFormatsUnderDevelopment);
    }

    @Override
    public Set<String> supportedFormats(boolean includeFormatsUnderDevelopment) {
        return delegate.supportedFormats(includeFormatsUnderDevelopment);
    }

    @Override
    public StoreFormatLimits limitsForFormat(String formatName, boolean includeFormatsUnderDevelopment)
            throws IllegalStateException {
        return delegate.limitsForFormat(formatName, includeFormatsUnderDevelopment);
    }

    @Override
    public boolean fitsWithinStoreFormatLimits(
            StoreFormatLimits formatLimits,
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fs,
            PageCache pageCache,
            Config config)
            throws IOException {
        return delegate.fitsWithinStoreFormatLimits(formatLimits, databaseLayout, fs, pageCache, config);
    }

    @Override
    public MetadataProvider transactionMetaDataStore(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config config,
            PageCache pageCache,
            DatabaseReadOnlyChecker readOnlyChecker,
            CursorContextFactory contextFactory,
            LogTailLogVersionsMetadata logTailMetadata,
            PageCacheTracer pageCacheTracer)
            throws IOException {
        return delegate.transactionMetaDataStore(
                fs,
                databaseLayout,
                config,
                pageCache,
                readOnlyChecker,
                contextFactory,
                logTailMetadata,
                pageCacheTracer);
    }

    @Override
    public void resetMetadata(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config config,
            PageCache pageCache,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            StoreId storeId,
            UUID externalStoreId)
            throws IOException {
        delegate.resetMetadata(
                fs, databaseLayout, config, pageCache, contextFactory, pageCacheTracer, storeId, externalStoreId);
    }

    @Override
    public Optional<UUID> databaseIdUuid(
            FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext) {
        return delegate.databaseIdUuid(fs, databaseLayout, pageCache, cursorContext);
    }

    @Override
    public List<SchemaRule44> load44SchemaRules(
            FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            DatabaseLayout databaseLayout,
            CursorContextFactory contextFactory,
            LogTailLogVersionsMetadata logTailMetadata) {
        return delegate.load44SchemaRules(
                fs, pageCache, pageCacheTracer, config, databaseLayout, contextFactory, logTailMetadata);
    }

    @Override
    public List<org.neo4j.internal.schema.SchemaRule> loadSchemaRules(
            FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            DatabaseLayout databaseLayout,
            boolean lenient,
            Function<org.neo4j.internal.schema.SchemaRule, org.neo4j.internal.schema.SchemaRule> schemaRuleMigration,
            CursorContextFactory contextFactory) {
        return delegate.loadSchemaRules(
                fs, pageCache, pageCacheTracer, config, databaseLayout, lenient, schemaRuleMigration, contextFactory);
    }

    @Override
    public TokenHolders loadReadOnlyTokens(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config config,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            boolean lenient,
            CursorContextFactory contextFactory) {
        return delegate.loadReadOnlyTokens(
                fs, databaseLayout, config, pageCache, pageCacheTracer, lenient, contextFactory);
    }

    @Override
    public SchemaRuleMigrationAccessExtended schemaRuleMigrationAccess(
            FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            DatabaseLayout databaseLayout,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            LogTailMetadata logTail)
            throws IOException {
        return delegate.schemaRuleMigrationAccess(
                fs, pageCache, pageCacheTracer, config, databaseLayout, contextFactory, memoryTracker, logTail);
    }

    @Override
    public StorageFilesState checkStoreFileState(
            FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache) {
        return delegate.checkStoreFileState(fs, databaseLayout, pageCache);
    }

    @Override
    public CommandReaderFactory commandReaderFactory() {
        return delegate.commandReaderFactory();
    }

    @Override
    public DatabaseLayout databaseLayout(Neo4jLayout neo4jLayout, String databaseName) {
        return delegate.databaseLayout(neo4jLayout, databaseName);
    }

    @Override
    public DatabaseLayout formatSpecificDatabaseLayout(DatabaseLayout plainLayout) {
        return delegate.formatSpecificDatabaseLayout(plainLayout);
    }

    @Override
    public BatchImporter batchImporter(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem,
            PageCacheTracer pageCacheTracer,
            Configuration config,
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
            CursorContextFactory contextFactory) {
        return delegate.batchImporter(
                databaseLayout,
                fileSystem,
                pageCacheTracer,
                config,
                logService,
                progressOutput,
                verboseProgressOutput,
                additionalInitialIds,
                dbConfig,
                monitor,
                jobScheduler,
                badCollector,
                logFilesInitializer,
                indexImporterFactory,
                memoryTracker,
                contextFactory);
    }

    @Override
    public Input asBatchImporterInput(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            MemoryTracker memoryTracker,
            ReadBehaviour readBehaviour,
            boolean compactNodeIdSpace,
            CursorContextFactory contextFactory,
            LogTailMetadata logTailMetadata) {
        return delegate.asBatchImporterInput(
                databaseLayout,
                fileSystem,
                pageCache,
                pageCacheTracer,
                config,
                memoryTracker,
                readBehaviour,
                compactNodeIdSpace,
                contextFactory,
                logTailMetadata);
    }

    @Override
    public IncrementalBatchImporter incrementalBatchImporter(
            DatabaseLayout databaseLayout,
            FileSystemAbstraction fileSystem,
            PageCacheTracer pageCacheTracer,
            Configuration config,
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
            IndexProvidersAccess indexProvidersAccess) {
        return delegate.incrementalBatchImporter(
                databaseLayout,
                fileSystem,
                pageCacheTracer,
                config,
                logService,
                progressOutput,
                verboseProgressOutput,
                additionalInitialIds,
                logTailMetadataSupplier,
                dbConfig,
                monitor,
                jobScheduler,
                badCollector,
                logFilesInitializer,
                indexImporterFactory,
                memoryTracker,
                contextFactory,
                indexProvidersAccess);
    }

    @Override
    public LockManager createLockManager(Config config, SystemNanoClock clock) {
        return delegate.createLockManager(config, clock);
    }

    @Override
    public long optimalAvailableConsistencyCheckerMemory(
            FileSystemAbstraction fs, DatabaseLayout layout, Config config, PageCache pageCache) {
        return delegate.optimalAvailableConsistencyCheckerMemory(fs, layout, config, pageCache);
    }

    @Override
    public void consistencyCheck(
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
            LogTailMetadata logTailMetadata)
            throws ConsistencyCheckIncompleteException {
        delegate.consistencyCheck(
                fileSystem,
                layout,
                config,
                pageCache,
                indexProviders,
                reportLog,
                verboseLog,
                summary,
                numberOfThreads,
                maxOffHeapCachingMemory,
                progressOutput,
                verbose,
                flags,
                contextFactory,
                pageCacheTracer,
                logTailMetadata);
    }

    @Override
    public ImmutableSet<OpenOption> getStoreOpenOptions(
            FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, CursorContextFactory contextFactory) {
        return delegate.getStoreOpenOptions(fs, pageCache, layout, contextFactory);
    }

    @Override
    public StorageEngineFactory unwrap() {
        return delegate.unwrap();
    }
}
