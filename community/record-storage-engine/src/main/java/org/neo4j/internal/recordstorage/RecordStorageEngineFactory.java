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
package org.neo4j.internal.recordstorage;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.checker.EntityBasedMemoryLimiter;
import org.neo4j.consistency.checker.RecordStorageConsistencyChecker;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.IndexConfig;
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
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.ScanOnOpenReadOnlyIdGeneratorFactory;
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
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.storemigration.RecordStorageMigrator;
import org.neo4j.kernel.impl.storemigration.RecordStoreRollingUpgradeCompatibility;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersion;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersionCheck;
import org.neo4j.lock.LockService;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.KernelVersionRepository;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageFilesState;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.migration.RollingUpgradeCompatibility;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokensLoader;

import static java.util.stream.Collectors.toList;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.io.layout.recordstorage.RecordDatabaseLayout.convert;
import static org.neo4j.kernel.impl.store.StoreType.META_DATA;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfig;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForVersion;

@ServiceProvider
public class RecordStorageEngineFactory implements StorageEngineFactory
{
    public static final String NAME = "record";

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public StoreVersionCheck versionCheck( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache,
            LogService logService, PageCacheTracer pageCacheTracer )
    {
        return new RecordStoreVersionCheck( fs, pageCache, convert( databaseLayout ), logService.getInternalLogProvider(), config,
                pageCacheTracer );
    }

    @Override
    public StoreVersion versionInformation( String storeVersion )
    {
        return new RecordStoreVersion( RecordFormatSelector.selectForVersion( storeVersion ) );
    }

    @Override
    public StoreVersion versionInformation( StoreId storeId )
    {
        return versionInformation( StoreVersion.versionLongToString( storeId.getStoreVersion() ) );
    }

    @Override
    public StoreVersion defaultStoreVersion()
    {
        return new RecordStoreVersion( defaultFormat() );
    }

    @Override
    public RollingUpgradeCompatibility rollingUpgradeCompatibility()
    {
        return new RecordStoreRollingUpgradeCompatibility( RecordFormatSelector.allFormats() );
    }

    @Override
    public List<StoreMigrationParticipant> migrationParticipants( FileSystemAbstraction fs, Config config, PageCache pageCache,
            JobScheduler jobScheduler, LogService logService, PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        BatchImporterFactory batchImporterFactory = BatchImporterFactory.withHighestPriority();
        RecordStorageMigrator recordStorageMigrator = new RecordStorageMigrator( fs, pageCache, config, logService, jobScheduler, cacheTracer,
                batchImporterFactory, memoryTracker );
        return List.of( recordStorageMigrator );
    }

    @Override
    public StorageEngine instantiate( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, TokenHolders tokenHolders,
            SchemaState schemaState, ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter, LockService lockService,
            IdGeneratorFactory idGeneratorFactory, IdController idController, DatabaseHealth databaseHealth, LogProvider internalLogProvider,
            LogProvider userLogProvider, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, PageCacheTracer cacheTracer, boolean createStoreIfNotExists,
            DatabaseReadOnlyChecker readOnlyChecker, MemoryTracker memoryTracker, CursorContext cursorContext )
    {
        return new RecordStorageEngine( convert( databaseLayout ), config, pageCache, fs, internalLogProvider, userLogProvider, tokenHolders, schemaState,
                constraintSemantics, indexConfigCompleter, lockService, databaseHealth, idGeneratorFactory, idController, recoveryCleanupWorkCollector,
                cacheTracer, createStoreIfNotExists, memoryTracker, readOnlyChecker, new CommandLockVerification.Factory.RealFactory( config ),
                LockVerificationMonitor.Factory.defaultFactory( config ), cursorContext );
    }

    @Override
    public List<Path> listStorageFiles( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException
    {
        if ( !fileSystem.fileExists( convert( databaseLayout ).metadataStore() ) )
        {
            throw new IOException( "No storage present at " + databaseLayout + " on " + fileSystem );
        }

        return Arrays.stream( StoreType.values() )
                     .map( t -> databaseLayout.file( t.getDatabaseFile() ) )
                     .filter( fileSystem::fileExists ).collect( toList() );
    }

    @Override
    public boolean storageExists( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, PageCache pageCache )
    {
        return NeoStores.isStorePresent( fileSystem, convert( databaseLayout ) );
    }

    @Override
    public TransactionIdStore readOnlyTransactionIdStore( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, PageCache pageCache,
            CursorContext cursorContext ) throws IOException
    {
        return new ReadOnlyTransactionIdStore( fileSystem, pageCache, convert( databaseLayout ), cursorContext );
    }

    @Override
    public LogVersionRepository readOnlyLogVersionRepository( DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext )
            throws IOException
    {
        return new ReadOnlyLogVersionRepository( pageCache, convert( databaseLayout ), cursorContext );
    }

    @Override
    public MetadataProvider transactionMetaDataStore( FileSystemAbstraction fs, DatabaseLayout layout, Config config, PageCache pageCache,
            PageCacheTracer cacheTracer, DatabaseReadOnlyChecker readOnlyChecker )
    {
        RecordDatabaseLayout databaseLayout = convert( layout );
        RecordFormats recordFormats = selectForStoreOrConfig( config, databaseLayout, fs, pageCache, NullLogProvider.getInstance(), cacheTracer );
        var idGeneratorFactory = readOnlyChecker.isReadOnly() ? new ScanOnOpenReadOnlyIdGeneratorFactory()
                                                              : new DefaultIdGeneratorFactory( fs, immediate(), databaseLayout.getDatabaseName() );
        return new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache, fs, recordFormats, NullLogProvider.getInstance(), cacheTracer,
                readOnlyChecker, immutable.empty() ).openNeoStores( META_DATA ).getMetaDataStore();
    }

    @Override
    public StoreId storeId( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext ) throws IOException
    {
        return MetaDataStore.getStoreId( pageCache, convert( databaseLayout ).metadataStore(), databaseLayout.getDatabaseName(), cursorContext );
    }

    @Override
    public void setStoreId( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext, StoreId storeId,
            long upgradeTxChecksum, long upgradeTxCommitTimestamp ) throws IOException
    {
        MetaDataStore.setStoreId( pageCache, convert( databaseLayout ).metadataStore(), storeId, upgradeTxChecksum, upgradeTxCommitTimestamp,
                databaseLayout.getDatabaseName(), cursorContext );
    }

    @Override
    public void setExternalStoreUUID( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext,
                               UUID externalStoreId ) throws IOException
    {
        MetaDataStore.setExternalStoreUUID( pageCache, convert( databaseLayout ).metadataStore(), externalStoreId, databaseLayout.getDatabaseName(),
                                            cursorContext );
    }

    @Override
    public Optional<UUID> databaseIdUuid( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext )
    {
        return MetaDataStore.getDatabaseIdUuid( pageCache, convert( databaseLayout ).metadataStore(), databaseLayout.getDatabaseName(), cursorContext );
    }

    @Override
    public SchemaRuleMigrationAccess schemaRuleMigrationAccess( FileSystemAbstraction fs, PageCache pageCache, Config config, DatabaseLayout layout,
            LogService logService, String recordFormats, PageCacheTracer cacheTracer, CursorContext cursorContext, MemoryTracker memoryTracker )
    {
        RecordDatabaseLayout databaseLayout = convert( layout );
        RecordFormats formats = selectForVersion( recordFormats );
        StoreFactory factory =
                new StoreFactory( databaseLayout, config, new DefaultIdGeneratorFactory( fs, immediate(), databaseLayout.getDatabaseName() ), pageCache, fs,
                        formats, logService.getInternalLogProvider(), cacheTracer, writable(), immutable.empty() );
        NeoStores stores = factory.openNeoStores( true, StoreType.SCHEMA, StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY );
        try
        {
            stores.start( cursorContext );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        return createMigrationTargetSchemaRuleAccess( stores, cursorContext, memoryTracker );
    }

    @Override
    public List<SchemaRule> loadSchemaRules( FileSystemAbstraction fs, PageCache pageCache, Config config, DatabaseLayout layout, boolean lenient,
            Function<SchemaRule,SchemaRule> schemaRuleMigration, PageCacheTracer pageCacheTracer )
    {
        RecordDatabaseLayout databaseLayout = convert( layout );
        StoreFactory factory =
                new StoreFactory( databaseLayout, config, new ScanOnOpenReadOnlyIdGeneratorFactory(), pageCache, fs,
                        NullLogProvider.getInstance(), pageCacheTracer, readOnly() );
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "loadSchemaRules" );
              var cursorContext = new CursorContext( cursorTracer );
              var stores = factory.openAllNeoStores();
              var storeCursors = new CachedStoreCursors( stores, cursorContext ) )
        {
            stores.start( cursorContext );
            TokenHolders tokenHolders = loadReadOnlyTokens( stores, lenient, pageCacheTracer );
            List<SchemaRule> rules = new ArrayList<>();
            SchemaStorage storage = new SchemaStorage( stores.getSchemaStore(), tokenHolders, () -> KernelVersion.LATEST );
            if ( lenient )
            {
                storage.getAllIgnoreMalformed( storeCursors ).forEach( rules::add );
            }
            else
            {
                storage.getAll( storeCursors ).forEach( rules::add );
            }
            return rules;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public TokenHolders loadReadOnlyTokens( FileSystemAbstraction fs, DatabaseLayout layout, Config config, PageCache pageCache, boolean lenient,
            PageCacheTracer pageCacheTracer )
    {
        RecordDatabaseLayout databaseLayout = convert( layout );
        StoreFactory factory =
                new StoreFactory( databaseLayout, config, new ScanOnOpenReadOnlyIdGeneratorFactory(), pageCache, fs,
                        NullLogProvider.getInstance(), pageCacheTracer, readOnly() );
        try ( NeoStores stores = factory.openNeoStores( false,
                StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY_KEY_TOKEN_NAME,
                StoreType.LABEL_TOKEN, StoreType.LABEL_TOKEN_NAME,
                StoreType.RELATIONSHIP_TYPE_TOKEN, StoreType.RELATIONSHIP_TYPE_TOKEN_NAME ) )
        {
            return loadReadOnlyTokens( stores, lenient, pageCacheTracer );
        }
    }

    private TokenHolders loadReadOnlyTokens( NeoStores stores, boolean lenient, PageCacheTracer pageCacheTracer )
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "loadReadOnlyTokens" );
                var cursorContext = new CursorContext( cursorTracer );
                var storeCursors = new CachedStoreCursors( stores, cursorContext ) )
        {
            stores.start( cursorContext );
            TokensLoader loader = lenient ? StoreTokens.allReadableTokens( stores ) : StoreTokens.allTokens( stores );
            TokenHolder propertyKeys = new DelegatingTokenHolder( ReadOnlyTokenCreator.READ_ONLY, TokenHolder.TYPE_PROPERTY_KEY );
            TokenHolder labels = new DelegatingTokenHolder( ReadOnlyTokenCreator.READ_ONLY, TokenHolder.TYPE_LABEL );
            TokenHolder relationshipTypes = new DelegatingTokenHolder( ReadOnlyTokenCreator.READ_ONLY, TokenHolder.TYPE_RELATIONSHIP_TYPE );

            propertyKeys.setInitialTokens( lenient ? unique( loader.getPropertyKeyTokens( storeCursors ) ) : loader.getPropertyKeyTokens( storeCursors ) );
            labels.setInitialTokens( lenient ? unique( loader.getLabelTokens( storeCursors ) ) : loader.getLabelTokens( storeCursors ) );
            relationshipTypes.setInitialTokens(
                    lenient ? unique( loader.getRelationshipTypeTokens( storeCursors ) ) : loader.getRelationshipTypeTokens( storeCursors ) );
            return new TokenHolders( propertyKeys, labels, relationshipTypes );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static List<NamedToken> unique( List<NamedToken> tokens )
    {
        if ( !tokens.isEmpty() )
        {
            Set<String> names = new HashSet<>( tokens.size() );
            int i = 0;
            while ( i < tokens.size() )
            {
                if ( names.add( tokens.get( i ).name() ) )
                {
                    i++;
                }
                else
                {
                    // Remove the token at the given index, by replacing it with the last token in the list.
                    // This changes the order of elements, but can be done in constant time instead of linear time.
                    int lastIndex = tokens.size() - 1;
                    NamedToken endToken = tokens.remove( lastIndex );
                    if ( i < lastIndex )
                    {
                        tokens.set( i, endToken );
                    }
                }
            }
        }
        return tokens;
    }

    @Override
    public CommandReaderFactory commandReaderFactory()
    {
        return RecordStorageCommandReaderFactory.INSTANCE;
    }

    @Override
    public RecordDatabaseLayout databaseLayout( Neo4jLayout neo4jLayout, String databaseName )
    {
        return RecordDatabaseLayout.of( neo4jLayout, databaseName);
    }

    @Override
    public StorageFilesState checkStoreFileState( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache )
    {
        RecordDatabaseLayout recordLayout = convert( databaseLayout );
        Set<Path> storeFiles = recordLayout.storeFiles();
        // count store, relationship group degrees store and index statistics are not mandatory stores to have since they can be automatically rebuilt
        storeFiles.remove( recordLayout.countStore() );
        storeFiles.remove( recordLayout.relationshipGroupDegreesStore() );
        storeFiles.remove( recordLayout.indexStatisticsStore() );
        boolean allStoreFilesExist = storeFiles.stream().allMatch( fs::fileExists );
        if ( !allStoreFilesExist )
        {
            return StorageFilesState.unrecoverableState( storeFiles.stream().filter( file -> !fs.fileExists( file ) ).collect( toList() ) );
        }

        boolean allIdFilesExist = recordLayout.idFiles().stream().allMatch( fs::fileExists );
        if ( !allIdFilesExist )
        {
            return StorageFilesState.recoverableState();
        }

        return StorageFilesState.recoveredState();
    }

    @Override
    public IndexConfig matchingBatchImportIndexConfiguration( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache )
    {
        try ( NeoStores neoStores = new StoreFactory( databaseLayout, Config.defaults(), new ScanOnOpenReadOnlyIdGeneratorFactory(), pageCache, fs,
                NullLogProvider.getInstance(), PageCacheTracer.NULL, DatabaseReadOnlyChecker.readOnly() ).openAllNeoStores();
                CachedStoreCursors storeCursors = new CachedStoreCursors( neoStores, CursorContext.NULL_CONTEXT ) )
        {
            // Injected NLI will be included if the store we're copying from is older than when token indexes were introduced.
            IndexConfig config = IndexConfig.create();
            SchemaRuleAccess schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess( neoStores.getSchemaStore(),
                    loadReadOnlyTokens( neoStores, true, PageCacheTracer.NULL ), neoStores.getMetaDataStore() );
            schemaRuleAccess.tokenIndexes( storeCursors ).forEachRemaining( index ->
            {
                if ( index.schema().entityType() == NODE )
                {
                    config.withLabelIndex( index.getName() );
                }
                if ( index.schema().entityType() == RELATIONSHIP )
                {
                    config.withRelationshipTypeIndex( index.getName() );
                }
            } );
            return config;
        }
    }

    @Override
    public BatchImporter batchImporter( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem, PageCacheTracer pageCacheTracer, Configuration config,
            LogService logService, PrintStream progressOutput, boolean verboseProgressOutput, AdditionalInitialIds additionalInitialIds, Config dbConfig,
            Monitor monitor, JobScheduler jobScheduler, Collector badCollector, LogFilesInitializer logFilesInitializer,
            IndexImporterFactory indexImporterFactory, MemoryTracker memoryTracker )
    {
        RecordFormats recordFormats = RecordFormatSelector.selectForConfig( dbConfig, logService.getInternalLogProvider() );
        ExecutionMonitor executionMonitor = progressOutput != null
                ? verboseProgressOutput ? new SpectrumExecutionMonitor( progressOutput ) : ExecutionMonitors.defaultVisible( progressOutput )
                : ExecutionMonitor.INVISIBLE;
        return BatchImporterFactory.withHighestPriority().instantiate( databaseLayout, fileSystem, pageCacheTracer, config, logService,
                executionMonitor, additionalInitialIds, dbConfig,
                recordFormats, monitor, jobScheduler, badCollector, logFilesInitializer,
                indexImporterFactory, memoryTracker );
    }

    @Override
    public Input asBatchImporterInput( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem, PageCache pageCache, PageCacheTracer pageCacheTracer,
            Config config, MemoryTracker memoryTracker, ReadBehaviour readBehaviour, boolean compactNodeIdSpace )
    {
        NeoStores neoStores =
                new StoreFactory( databaseLayout, config, new ScanOnOpenReadOnlyIdGeneratorFactory(), pageCache, fileSystem, NullLogProvider.getInstance(),
                        pageCacheTracer, readOnly() ).openAllNeoStores();
        return new LenientStoreInput( neoStores, readBehaviour.decorateTokenHolders( loadReadOnlyTokens( neoStores, true, PageCacheTracer.NULL ) ),
                compactNodeIdSpace, pageCacheTracer, readBehaviour );
    }

    /**
     * @return SchemaRuleMigrationAccess that uses the latest kernel version and therefore never includes an injected node label index.
     */
    public static SchemaRuleMigrationAccess createMigrationTargetSchemaRuleAccess( NeoStores stores, CursorContext cursorContext, MemoryTracker memoryTracker )
    {
        return createMigrationTargetSchemaRuleAccess( stores, cursorContext, memoryTracker, () -> KernelVersion.LATEST );
    }

    @Override
    public void consistencyCheck( FileSystemAbstraction fileSystem, DatabaseLayout layout, Config config, PageCache pageCache, IndexProviderMap indexProviders,
            Log log, ConsistencySummaryStatistics summary, int numberOfThreads, double memoryLimitLeewayFactor, OutputStream progressOutput, boolean verbose,
            ConsistencyFlags flags, PageCacheTracer pageCacheTracer )
            throws ConsistencyCheckIncompleteException
    {
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, RecoveryCleanupWorkCollector.ignore(), layout.getDatabaseName() );
        try ( NeoStores neoStores = new StoreFactory( layout, config,
                idGeneratorFactory, pageCache, fileSystem,
                NullLogProvider.getInstance(), pageCacheTracer, readOnly() ).openAllNeoStores() )
        {
            neoStores.start( CursorContext.NULL_CONTEXT );
            ProgressMonitorFactory progressMonitorFactory =
                    progressOutput != null ? ProgressMonitorFactory.textual( progressOutput ) : ProgressMonitorFactory.NONE;
            try ( RecordStorageConsistencyChecker checker = new RecordStorageConsistencyChecker( fileSystem, RecordDatabaseLayout.convert( layout ), pageCache,
                    neoStores, indexProviders, null, idGeneratorFactory, summary, progressMonitorFactory, config, numberOfThreads, log, verbose, flags,
                    EntityBasedMemoryLimiter.defaultWithLeeway( memoryLimitLeewayFactor ), pageCacheTracer, EmptyMemoryTracker.INSTANCE ) )
            {
                checker.check();
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    public static SchemaRuleMigrationAccess createMigrationTargetSchemaRuleAccess( NeoStores stores, CursorContext cursorContext, MemoryTracker memoryTracker,
            KernelVersionRepository kernelVersionRepository )
    {
        SchemaStore dstSchema = stores.getSchemaStore();
        TokenCreator propertyKeyTokenCreator = ( name, internal ) ->
        {
            try ( var storeCursors = new CachedStoreCursors( stores, cursorContext ) )
            {
                PropertyKeyTokenStore keyTokenStore = stores.getPropertyKeyTokenStore();
                DynamicStringStore nameStore = keyTokenStore.getNameStore();
                byte[] bytes = PropertyStore.encodeString( name );
                List<DynamicRecord> nameRecords = new ArrayList<>();
                AbstractDynamicStore.allocateRecordsFromBytes( nameRecords, bytes, nameStore, cursorContext, memoryTracker );
                nameRecords.forEach( record -> nameStore.prepareForCommit( record, cursorContext ) );
                try ( PageCursor cursor = storeCursors.writeCursor( DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR ) )
                {
                    nameRecords.forEach( record -> nameStore.updateRecord( record, cursor, cursorContext, storeCursors ) );
                }
                nameRecords.forEach( record -> nameStore.setHighestPossibleIdInUse( record.getId() ) );
                int nameId = Iterables.first( nameRecords ).getIntId();
                PropertyKeyTokenRecord keyTokenRecord = keyTokenStore.newRecord();
                long tokenId = keyTokenStore.nextId( cursorContext );
                keyTokenRecord.setId( tokenId );
                keyTokenRecord.initialize( true, nameId );
                keyTokenRecord.setInternal( internal );
                keyTokenRecord.setCreated();
                keyTokenStore.prepareForCommit( keyTokenRecord, cursorContext );
                try ( PageCursor pageCursor = storeCursors.writeCursor( PROPERTY_KEY_TOKEN_CURSOR ) )
                {
                    keyTokenStore.updateRecord( keyTokenRecord, pageCursor, cursorContext, storeCursors );
                }
                keyTokenStore.setHighestPossibleIdInUse( keyTokenRecord.getId() );
                return Math.toIntExact( tokenId );
            }
        };
        var storeCursors = new CachedStoreCursors( stores, cursorContext );
        TokenHolders dstTokenHolders = loadTokenHolders( stores, propertyKeyTokenCreator, storeCursors );
        return new SchemaRuleMigrationAccessImpl( stores, new SchemaStorage( dstSchema, dstTokenHolders, kernelVersionRepository ), cursorContext,
                memoryTracker, storeCursors );
    }

    private static TokenHolders loadTokenHolders( NeoStores stores, TokenCreator propertyKeyTokenCreator, StoreCursors storeCursors )
    {
        TokenHolder propertyKeyTokens = new DelegatingTokenHolder( propertyKeyTokenCreator, TokenHolder.TYPE_PROPERTY_KEY );
        TokenHolders dstTokenHolders = new TokenHolders( propertyKeyTokens, StoreTokens.createReadOnlyTokenHolder( TokenHolder.TYPE_LABEL ),
                StoreTokens.createReadOnlyTokenHolder( TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        dstTokenHolders.propertyKeyTokens().setInitialTokens( stores.getPropertyKeyTokenStore().getTokens( storeCursors ) );
        return dstTokenHolders;
    }
}
