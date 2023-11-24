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
package org.neo4j.internal.recordstorage;

import static java.util.function.Predicate.not;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_LABEL_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_REL_TYPE_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.LABEL_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.REL_TYPE_TOKEN_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.kernel.impl.store.StoreType.LABEL_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.StoreType.META_DATA;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_KEY_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP_TYPE_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStore;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfigForNewDbs;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.checker.EntityBasedMemoryLimiter;
import org.neo4j.consistency.checker.RecordStorageConsistencyChecker;
import org.neo4j.consistency.checking.ByteArrayBitsManipulator;
import org.neo4j.consistency.checking.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.IncrementalBatchImporter;
import org.neo4j.internal.batchimport.IncrementalBatchImporterFactory;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.batchimport.Monitor;
import org.neo4j.internal.batchimport.ReadBehaviour;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.LenientStoreInput;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.internal.batchimport.staging.SpectrumExecutionMonitor;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.ScanOnOpenReadOnlyIdGeneratorFactory;
import org.neo4j.internal.id.SchemaIdType;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersionRepository;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.locking.forseti.ForsetiLockManager;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.LegacyMetadataHandler;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.PageCacheOptionsSelector;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.storemigration.RecordStorageMigrator;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersion;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.legacy.SchemaStore44Reader;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.lock.LockService;
import org.neo4j.lock.ResourceType;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.SchemaRule44;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageFilesState;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.StoreVersionIdentifier;
import org.neo4j.storageengine.api.cursor.CursorType;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.format.Index44Compatibility;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccessExtended;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.RegisteringCreatingTokenHolder;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokensLoader;

@ServiceProvider
public class RecordStorageEngineFactory implements StorageEngineFactory {
    public static final String NAME = "record";
    public static final byte ID = 1;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public StoreId retrieveStoreId(
            FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext)
            throws IOException {
        final var metaDataFile = formatSpecificDatabaseLayout(databaseLayout).metadataStore();
        final var databaseName = databaseLayout.getDatabaseName();
        MetaDataStore.FieldAccess fieldAccess =
                MetaDataStore.getFieldAccess(pageCache, metaDataFile, databaseName, cursorContext);
        if (fieldAccess.isLegacyFieldValid()) {
            return fieldAccess.readStoreId();
        }

        return LegacyMetadataHandler.readMetadata44FromStore(pageCache, metaDataFile, databaseName, cursorContext)
                .storeId();
    }

    @Override
    public StoreVersionCheck versionCheck(
            FileSystemAbstraction fs,
            DatabaseLayout databaseLayout,
            Config config,
            PageCache pageCache,
            LogService logService,
            CursorContextFactory contextFactory) {
        return new RecordStoreVersionCheck(pageCache, formatSpecificDatabaseLayout(databaseLayout), config);
    }

    @Override
    public Optional<? extends StoreVersion> versionInformation(StoreVersionIdentifier storeVersionIdentifier) {
        var maybeRecordFormat = RecordFormatSelector.selectForStoreVersionIdentifier(storeVersionIdentifier);
        return maybeRecordFormat.map(RecordStoreVersion::new);
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
        BatchImporterFactory batchImporterFactory = BatchImporterFactory.withHighestPriority();
        RecordStorageMigrator recordStorageMigrator = new RecordStorageMigrator(
                fs,
                pageCache,
                pageCacheTracer,
                config,
                logService,
                jobScheduler,
                contextFactory,
                batchImporterFactory,
                memoryTracker,
                forceBtreeIndexesToRange);
        return List.of(recordStorageMigrator);
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
            VersionStorage versionStorage) {
        return new RecordStorageEngine(
                formatSpecificDatabaseLayout(databaseLayout),
                config,
                pageCache,
                fs,
                internalLogProvider,
                userLogProvider,
                tokenHolders,
                schemaState,
                constraintSemantics,
                indexConfigCompleter,
                lockService,
                databaseHealth,
                idGeneratorFactory,
                recoveryCleanupWorkCollector,
                memoryTracker,
                logTailMetadata,
                kernelVersionRepository,
                LockVerificationFactory.select(config),
                contextFactory,
                pageCacheTracer,
                versionStorage);
    }

    @Override
    public List<Path> listStorageFiles(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout)
            throws IOException {
        if (!fileSystem.fileExists(formatSpecificDatabaseLayout(databaseLayout).metadataStore())) {
            throw new IOException("No storage present at " + databaseLayout + " on " + fileSystem);
        }

        return Arrays.stream(StoreType.STORE_TYPES)
                .map(t -> databaseLayout.file(t.getDatabaseFile()))
                .filter(fileSystem::fileExists)
                .toList();
    }

    @Override
    public boolean storageExists(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout) {
        return NeoStores.isStorePresent(fileSystem, formatSpecificDatabaseLayout(databaseLayout));
    }

    @Override
    public Set<String> supportedFormats(boolean includeFormatsUnderDevelopment) {
        return Iterables.stream(RecordFormatSelector.allFormats())
                .filter(f -> includeFormatsUnderDevelopment || !f.formatUnderDevelopment())
                .filter(not(RecordFormats::onlyForMigration))
                .map(RecordFormats::name)
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public MetadataProvider transactionMetaDataStore(
            FileSystemAbstraction fs,
            DatabaseLayout layout,
            Config config,
            PageCache pageCache,
            DatabaseReadOnlyChecker readOnlyChecker,
            CursorContextFactory contextFactory,
            LogTailLogVersionsMetadata logTailMetadata,
            PageCacheTracer pageCacheTracer) {
        RecordDatabaseLayout databaseLayout = formatSpecificDatabaseLayout(layout);
        RecordFormats recordFormats = selectForStoreOrConfigForNewDbs(
                config, databaseLayout, fs, pageCache, NullLogProvider.getInstance(), contextFactory);
        var idGeneratorFactory = readOnlyChecker.isReadOnly()
                ? new ScanOnOpenReadOnlyIdGeneratorFactory()
                : new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName());
        return new StoreFactory(
                        databaseLayout,
                        config,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        fs,
                        recordFormats,
                        NullLogProvider.getInstance(),
                        contextFactory,
                        readOnlyChecker.isReadOnly(),
                        logTailMetadata,
                        immutable.empty())
                .openNeoStores(META_DATA)
                .getMetaDataStore();
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
        try (var metadataProvider = transactionMetaDataStore(
                        fs,
                        databaseLayout,
                        config,
                        pageCache,
                        writable(),
                        contextFactory,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                        pageCacheTracer);
                var cursorContext = contextFactory.create("resetMetadata")) {
            metadataProvider.regenerateMetadata(storeId, externalStoreId, cursorContext);
        }
    }

    @Override
    public Optional<UUID> databaseIdUuid(
            FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext) {
        var fieldAccess = MetaDataStore.getFieldAccess(
                pageCache,
                formatSpecificDatabaseLayout(databaseLayout).metadataStore(),
                databaseLayout.getDatabaseName(),
                cursorContext);
        try {
            return fieldAccess.readDatabaseUUID();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<SchemaRule44> load44SchemaRules(
            FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            DatabaseLayout layout,
            CursorContextFactory contextFactory,
            LogTailLogVersionsMetadata logTailMetadata) {
        RecordDatabaseLayout recordDatabaseLayout = formatSpecificDatabaseLayout(layout);
        RecordFormats recordFormats = RecordFormatSelector.selectForStore(
                recordDatabaseLayout, fs, pageCache, NullLogProvider.getInstance(), contextFactory);
        if (recordFormats == null) {
            throw new IllegalStateException("Attempting to load 4.4 Schema rules from an empty store");
        }

        if (!recordFormats.hasCapability(Index44Compatibility.INSTANCE)) {
            throw new IllegalStateException("'" + recordFormats + "' is not a 4.4 store format");
        }

        IdGeneratorFactory idGeneratorFactory = new ScanOnOpenReadOnlyIdGeneratorFactory();
        StoreFactory factory = new StoreFactory(
                recordDatabaseLayout,
                config,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                fs,
                NullLogProvider.getInstance(),
                contextFactory,
                true,
                logTailMetadata);
        try (var cursorContext = contextFactory.create("loadSchemaRules");
                var stores = factory.openNeoStores(
                        StoreType.SCHEMA,
                        StoreType.PROPERTY,
                        StoreType.LABEL_TOKEN,
                        StoreType.RELATIONSHIP_TYPE_TOKEN,
                        StoreType.PROPERTY_KEY_TOKEN);
                var storeCursors = new CachedStoreCursors(stores, cursorContext)) {
            stores.start(cursorContext);
            TokenHolders tokenHolders = loadReadOnlyTokens(stores, true, contextFactory);

            var metadata = LegacyMetadataHandler.readMetadata44FromStore(
                    pageCache,
                    recordDatabaseLayout.metadataStore(),
                    recordDatabaseLayout.getDatabaseName(),
                    cursorContext);

            try (SchemaStore44Reader schemaStoreReader = new SchemaStore44Reader(
                    fs,
                    stores.getPropertyStore(),
                    tokenHolders,
                    metadata.kernelVersion(),
                    recordDatabaseLayout.schemaStore(),
                    recordDatabaseLayout.idSchemaStore(),
                    config,
                    SchemaIdType.SCHEMA,
                    idGeneratorFactory,
                    pageCache,
                    pageCacheTracer,
                    contextFactory,
                    NullLogProvider.getInstance(),
                    recordFormats,
                    recordDatabaseLayout.getDatabaseName(),
                    stores.getOpenOptions())) {

                return schemaStoreReader.loadAllSchemaRules(storeCursors);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<SchemaRule> loadSchemaRules(
            FileSystemAbstraction fs,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            Config config,
            DatabaseLayout layout,
            boolean lenient,
            Function<SchemaRule, SchemaRule> schemaRuleMigration,
            CursorContextFactory contextFactory) {
        RecordDatabaseLayout databaseLayout = formatSpecificDatabaseLayout(layout);
        StoreFactory factory = new StoreFactory(
                databaseLayout,
                config,
                new ScanOnOpenReadOnlyIdGeneratorFactory(),
                pageCache,
                pageCacheTracer,
                fs,
                NullLogProvider.getInstance(),
                contextFactory,
                true,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
        try (var cursorContext = contextFactory.create("loadSchemaRules");
                var stores = factory.openAllNeoStores();
                var storeCursors = new CachedStoreCursors(stores, cursorContext)) {
            stores.start(cursorContext);
            TokenHolders tokenHolders = loadReadOnlyTokens(stores, lenient, contextFactory);
            List<SchemaRule> rules = new ArrayList<>();
            SchemaStorage storage = new SchemaStorage(stores.getSchemaStore(), tokenHolders);
            if (lenient) {
                storage.getAllIgnoreMalformed(storeCursors).forEach(rules::add);
            } else {
                storage.getAll(storeCursors).forEach(rules::add);
            }
            return rules;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public TokenHolders loadReadOnlyTokens(
            FileSystemAbstraction fs,
            DatabaseLayout layout,
            Config config,
            PageCache pageCache,
            PageCacheTracer pageCacheTracer,
            boolean lenient,
            CursorContextFactory contextFactory) {
        RecordDatabaseLayout databaseLayout = formatSpecificDatabaseLayout(layout);
        StoreFactory factory = new StoreFactory(
                databaseLayout,
                config,
                new ScanOnOpenReadOnlyIdGeneratorFactory(),
                pageCache,
                pageCacheTracer,
                fs,
                NullLogProvider.getInstance(),
                contextFactory,
                true,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
        try (NeoStores stores = factory.openNeoStores(
                StoreType.PROPERTY_KEY_TOKEN,
                StoreType.PROPERTY_KEY_TOKEN_NAME,
                StoreType.LABEL_TOKEN,
                StoreType.LABEL_TOKEN_NAME,
                StoreType.RELATIONSHIP_TYPE_TOKEN,
                StoreType.RELATIONSHIP_TYPE_TOKEN_NAME)) {
            return loadReadOnlyTokens(stores, lenient, contextFactory);
        }
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
        DefaultIdGeneratorFactory idGeneratorFactory =
                new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName());
        StoreFactory dstFactory = new StoreFactory(
                databaseLayout,
                config,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                fs,
                NullLogProvider.getInstance(),
                contextFactory,
                false,
                logTail);

        CursorContext cursorContext = contextFactory.create("schemaStoreMigration");
        NeoStores dstStore = dstFactory.openNeoStores(
                StoreType.SCHEMA,
                StoreType.PROPERTY_KEY_TOKEN,
                StoreType.PROPERTY,
                StoreType.PROPERTY_KEY_TOKEN_NAME,
                StoreType.LABEL_TOKEN,
                StoreType.LABEL_TOKEN_NAME,
                StoreType.RELATIONSHIP_TYPE_TOKEN,
                StoreType.RELATIONSHIP_TYPE_TOKEN_NAME);
        dstStore.start(cursorContext);
        try {
            SchemaStore dstSchema = dstStore.getSchemaStore();
            var allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(dstStore);

            // Token holders that create (and add to cache) the tokens asked for if they don't already have them
            TokenHolders dstTokenHolders = new TokenHolders(
                    new RegisteringCreatingTokenHolder(
                            getPropertyTokenCreator(dstStore, contextFactory, memoryTracker, allocatorProvider),
                            TokenHolder.TYPE_PROPERTY_KEY),
                    new RegisteringCreatingTokenHolder(
                            getLabelTokenCreator(dstStore, contextFactory, memoryTracker, allocatorProvider),
                            TokenHolder.TYPE_LABEL),
                    new RegisteringCreatingTokenHolder(
                            getRelTypeTokenCreator(dstStore, contextFactory, memoryTracker, allocatorProvider),
                            TokenHolder.TYPE_RELATIONSHIP_TYPE));
            var storeCursors = new CachedStoreCursors(dstStore, cursorContext);
            dstTokenHolders
                    .propertyKeyTokens()
                    .setInitialTokens(dstStore.getPropertyKeyTokenStore().getTokens(storeCursors));
            dstTokenHolders
                    .labelTokens()
                    .setInitialTokens(dstStore.getLabelTokenStore().getTokens(storeCursors));
            dstTokenHolders
                    .relationshipTypeTokens()
                    .setInitialTokens(dstStore.getRelationshipTypeTokenStore().getTokens(storeCursors));

            return new SchemaRuleMigrationAccessImplExtended(
                    dstStore,
                    new SchemaStorage(dstSchema, dstTokenHolders),
                    allocatorProvider,
                    cursorContext,
                    memoryTracker,
                    storeCursors,
                    dstTokenHolders);
        } catch (Throwable e) {
            dstStore.close();
            throw e;
        }
    }

    private TokenHolders loadReadOnlyTokens(NeoStores stores, boolean lenient, CursorContextFactory contextFactory) {
        try (var cursorContext = contextFactory.create("loadReadOnlyTokens");
                var storeCursors = new CachedStoreCursors(stores, cursorContext)) {
            stores.start(cursorContext);
            TokensLoader loader = lenient ? StoreTokens.allReadableTokens(stores) : StoreTokens.allTokens(stores);
            TokenHolder propertyKeys =
                    new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TokenHolder.TYPE_PROPERTY_KEY);
            TokenHolder labels = new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TokenHolder.TYPE_LABEL);
            TokenHolder relationshipTypes =
                    new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, TokenHolder.TYPE_RELATIONSHIP_TYPE);

            propertyKeys.setInitialTokens(
                    lenient
                            ? unique(loader.getPropertyKeyTokens(storeCursors))
                            : loader.getPropertyKeyTokens(storeCursors));
            labels.setInitialTokens(
                    lenient ? unique(loader.getLabelTokens(storeCursors)) : loader.getLabelTokens(storeCursors));
            relationshipTypes.setInitialTokens(
                    lenient
                            ? unique(loader.getRelationshipTypeTokens(storeCursors))
                            : loader.getRelationshipTypeTokens(storeCursors));
            return new TokenHolders(propertyKeys, labels, relationshipTypes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<NamedToken> unique(List<NamedToken> tokens) {
        if (!tokens.isEmpty()) {
            Set<String> names = new HashSet<>(tokens.size());
            int i = 0;
            while (i < tokens.size()) {
                if (names.add(tokens.get(i).name())) {
                    i++;
                } else {
                    // Remove the token at the given index, by replacing it with the last token in the list.
                    // This changes the order of elements, but can be done in constant time instead of linear time.
                    int lastIndex = tokens.size() - 1;
                    NamedToken endToken = tokens.remove(lastIndex);
                    if (i < lastIndex) {
                        tokens.set(i, endToken);
                    }
                }
            }
        }
        return tokens;
    }

    @Override
    public CommandReaderFactory commandReaderFactory() {
        return RecordStorageCommandReaderFactory.INSTANCE;
    }

    @Override
    public RecordDatabaseLayout databaseLayout(Neo4jLayout neo4jLayout, String databaseName) {
        return RecordDatabaseLayout.of(neo4jLayout, databaseName);
    }

    @Override
    public RecordDatabaseLayout formatSpecificDatabaseLayout(DatabaseLayout plainLayout) {
        if (plainLayout instanceof RecordDatabaseLayout recordLayout) {
            return recordLayout;
        } else {
            return databaseLayout(plainLayout.getNeo4jLayout(), plainLayout.getDatabaseName());
        }
    }

    @Override
    public StorageFilesState checkStoreFileState(
            FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache) {
        RecordDatabaseLayout recordLayout = formatSpecificDatabaseLayout(databaseLayout);
        Set<Path> storeFiles = recordLayout.mandatoryStoreFiles();
        boolean allStoreFilesExist = storeFiles.stream().allMatch(fs::fileExists);
        if (!allStoreFilesExist) {
            return StorageFilesState.unrecoverableState(
                    storeFiles.stream().filter(file -> !fs.fileExists(file)).toList());
        }

        boolean allIdFilesExist = recordLayout.idFiles().stream().allMatch(fs::fileExists);
        if (!allIdFilesExist) {
            return StorageFilesState.recoverableState();
        }

        return StorageFilesState.recoveredState();
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
        ExecutionMonitor executionMonitor = progressOutput != null
                ? verboseProgressOutput
                        ? new SpectrumExecutionMonitor(progressOutput)
                        : ExecutionMonitors.defaultVisible(progressOutput, System.err)
                : ExecutionMonitor.INVISIBLE;
        return BatchImporterFactory.withHighestPriority()
                .instantiate(
                        databaseLayout,
                        fileSystem,
                        pageCacheTracer,
                        config,
                        logService,
                        executionMonitor,
                        additionalInitialIds,
                        new EmptyLogTailMetadata(dbConfig),
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
        var storesToOpen = Arrays.stream(StoreType.STORE_TYPES)
                .filter(storeType -> storeType != META_DATA)
                .toArray(StoreType[]::new);
        NeoStores neoStores = new StoreFactory(
                        databaseLayout,
                        config,
                        new ScanOnOpenReadOnlyIdGeneratorFactory(),
                        pageCache,
                        pageCacheTracer,
                        fileSystem,
                        NullLogProvider.getInstance(),
                        contextFactory,
                        true,
                        logTailMetadata)
                .openNeoStores(storesToOpen);
        return new LenientStoreInput(
                neoStores,
                readBehaviour.decorateTokenHolders(loadReadOnlyTokens(neoStores, true, contextFactory)),
                compactNodeIdSpace,
                contextFactory,
                readBehaviour);
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
        return IncrementalBatchImporterFactory.withHighestPriority()
                .instantiate(
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
        return new ForsetiLockManager(config, clock, ResourceType.values());
    }

    @Override
    public long optimalAvailableConsistencyCheckerMemory(
            FileSystemAbstraction fs, DatabaseLayout layout, Config config, PageCache pageCache) {
        RecordDatabaseLayout databaseLayout = formatSpecificDatabaseLayout(layout);
        CursorContextFactory contextFactory = NULL_CONTEXT_FACTORY;
        RecordFormats recordFormats =
                selectForStore(databaseLayout, fs, pageCache, NullLogProvider.getInstance(), contextFactory);
        var idGeneratorFactory = new DefaultIdGeneratorFactory(
                fs, immediate(), false, PageCacheTracer.NULL, layout.getDatabaseName(), true, true);
        try (NeoStores neoStores = new StoreFactory(
                        databaseLayout,
                        config,
                        idGeneratorFactory,
                        pageCache,
                        PageCacheTracer.NULL,
                        fs,
                        recordFormats,
                        NullLogProvider.getInstance(),
                        contextFactory,
                        true,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                        immutable.empty())
                .openNeoStores(StoreType.NODE_LABEL, StoreType.NODE, StoreType.RELATIONSHIP)) {
            var highId = Math.max(
                    neoStores.getNodeStore().getIdGenerator().getHighId(),
                    neoStores.getRelationshipStore().getIdGenerator().getHighId());
            return ByteArrayBitsManipulator.MAX_BYTES * highId;
        }
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
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory(
                fileSystem, RecoveryCleanupWorkCollector.ignore(), pageCacheTracer, layout.getDatabaseName());
        try (NeoStores neoStores = new StoreFactory(
                        layout,
                        config,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        fileSystem,
                        NullLogProvider.getInstance(),
                        contextFactory,
                        true,
                        logTailMetadata)
                .openAllNeoStores()) {
            neoStores.start(CursorContext.NULL_CONTEXT);
            ProgressMonitorFactory progressMonitorFactory = progressOutput != null
                    ? ProgressMonitorFactory.textual(progressOutput)
                    : ProgressMonitorFactory.NONE;
            try (RecordStorageConsistencyChecker checker = new RecordStorageConsistencyChecker(
                    fileSystem,
                    formatSpecificDatabaseLayout(layout),
                    pageCache,
                    neoStores,
                    indexProviders,
                    idGeneratorFactory,
                    summary,
                    progressMonitorFactory,
                    config,
                    numberOfThreads,
                    reportLog,
                    verboseLog,
                    verbose,
                    flags,
                    EntityBasedMemoryLimiter.defaultMemoryLimiter(maxOffHeapCachingMemory),
                    EmptyMemoryTracker.INSTANCE,
                    contextFactory,
                    pageCacheTracer)) {
                checker.check();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public ImmutableSet<OpenOption> getStoreOpenOptions(
            FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout layout, CursorContextFactory contextFactory) {
        RecordDatabaseLayout recordDatabaseLayout = formatSpecificDatabaseLayout(layout);
        RecordFormats recordFormats = RecordFormatSelector.selectForStore(
                recordDatabaseLayout, fs, pageCache, NullLogProvider.getInstance(), contextFactory);
        if (recordFormats == null) {
            throw new IllegalStateException("Can't detect open options for empty store");
        }

        return PageCacheOptionsSelector.select(recordFormats);
    }

    public static SchemaRuleMigrationAccess createMigrationTargetSchemaRuleAccess(
            NeoStores stores, CursorContextFactory contextFactory, MemoryTracker memoryTracker) {
        SchemaStore dstSchema = stores.getSchemaStore();
        var allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(stores);
        TokenCreator propertyKeyTokenCreator =
                getPropertyTokenCreator(stores, contextFactory, memoryTracker, allocatorProvider);
        var cursorContext = contextFactory.create("createMigrationTargetSchemaRuleAccess");
        var storeCursors = new CachedStoreCursors(stores, cursorContext);
        TokenHolders dstTokenHolders = loadTokenHolders(stores, propertyKeyTokenCreator, storeCursors);
        return new SchemaRuleMigrationAccessImpl(
                stores,
                new SchemaStorage(dstSchema, dstTokenHolders),
                allocatorProvider,
                cursorContext,
                memoryTracker,
                storeCursors);
    }

    private static TokenCreator getPropertyTokenCreator(
            NeoStores stores,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            DynamicAllocatorProvider allocatorProvider) {
        return getTokenCreator(
                stores,
                stores.getPropertyKeyTokenStore(),
                PROPERTY_KEY_TOKEN_NAME,
                DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR,
                PROPERTY_KEY_TOKEN_CURSOR,
                contextFactory,
                memoryTracker,
                allocatorProvider);
    }

    private static TokenCreator getLabelTokenCreator(
            NeoStores stores,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            DynamicAllocatorProvider allocatorProvider) {
        return getTokenCreator(
                stores,
                stores.getLabelTokenStore(),
                LABEL_TOKEN_NAME,
                DYNAMIC_LABEL_TOKEN_CURSOR,
                LABEL_TOKEN_CURSOR,
                contextFactory,
                memoryTracker,
                allocatorProvider);
    }

    private static TokenCreator getRelTypeTokenCreator(
            NeoStores stores,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            DynamicAllocatorProvider allocatorProvider) {
        return getTokenCreator(
                stores,
                stores.getRelationshipTypeTokenStore(),
                RELATIONSHIP_TYPE_TOKEN_NAME,
                DYNAMIC_REL_TYPE_TOKEN_CURSOR,
                REL_TYPE_TOKEN_CURSOR,
                contextFactory,
                memoryTracker,
                allocatorProvider);
    }

    private static <T extends TokenRecord, STORE extends TokenStore<T>> TokenCreator getTokenCreator(
            NeoStores stores,
            STORE tokenStore,
            StoreType nameStoreType,
            CursorType nameStoreCursorType,
            CursorType storeCursorType,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            DynamicAllocatorProvider allocatorProvider) {
        return (name, internal) -> {
            try (var cursorContext = contextFactory.create("createMigrationTargetSchemaRuleAccess");
                    var storeCursors = new CachedStoreCursors(stores, cursorContext)) {
                DynamicStringStore nameStore = tokenStore.getNameStore();
                byte[] bytes = PropertyStore.encodeString(name);
                List<DynamicRecord> nameRecords = new ArrayList<>();
                AbstractDynamicStore.allocateRecordsFromBytes(
                        nameRecords, bytes, allocatorProvider.allocator(nameStoreType), cursorContext, memoryTracker);
                nameRecords.forEach(
                        record -> nameStore.prepareForCommit(record, nameStore.getIdGenerator(), cursorContext));
                try (PageCursor cursor = storeCursors.writeCursor(nameStoreCursorType)) {
                    nameRecords.forEach(record -> nameStore.updateRecord(record, cursor, cursorContext, storeCursors));
                }
                nameRecords.forEach(record -> {
                    long highId = record.getId();
                    nameStore.getIdGenerator().setHighestPossibleIdInUse(highId);
                });
                int nameId = Iterables.first(nameRecords).getIntId();
                T keyTokenRecord = tokenStore.newRecord();
                long tokenId = tokenStore.getIdGenerator().nextId(cursorContext);
                keyTokenRecord.setId(tokenId);
                keyTokenRecord.initialize(true, nameId);
                keyTokenRecord.setInternal(internal);
                keyTokenRecord.setCreated();
                tokenStore.prepareForCommit(keyTokenRecord, tokenStore.getIdGenerator(), cursorContext);
                try (PageCursor pageCursor = storeCursors.writeCursor(storeCursorType)) {
                    tokenStore.updateRecord(keyTokenRecord, pageCursor, cursorContext, storeCursors);
                }
                long highId = keyTokenRecord.getId();
                tokenStore.getIdGenerator().setHighestPossibleIdInUse(highId);
                return Math.toIntExact(tokenId);
            }
        };
    }

    private static TokenHolders loadTokenHolders(
            NeoStores stores, TokenCreator propertyKeyTokenCreator, StoreCursors storeCursors) {
        TokenHolder propertyKeyTokens =
                new RegisteringCreatingTokenHolder(propertyKeyTokenCreator, TokenHolder.TYPE_PROPERTY_KEY);
        TokenHolders dstTokenHolders = new TokenHolders(
                propertyKeyTokens,
                StoreTokens.createReadOnlyTokenHolder(TokenHolder.TYPE_LABEL),
                StoreTokens.createReadOnlyTokenHolder(TokenHolder.TYPE_RELATIONSHIP_TYPE));
        dstTokenHolders
                .propertyKeyTokens()
                .setInitialTokens(stores.getPropertyKeyTokenStore().getTokens(storeCursors));
        return dstTokenHolders;
    }
}
