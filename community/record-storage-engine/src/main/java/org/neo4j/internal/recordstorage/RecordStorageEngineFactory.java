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
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.helpers.collection.Iterables;
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
import org.neo4j.kernel.impl.storemigration.IdGeneratorMigrator;
import org.neo4j.kernel.impl.storemigration.RecordStorageMigrator;
import org.neo4j.kernel.impl.storemigration.RecordStoreRollingUpgradeCompatibility;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersion;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersionCheck;
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
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
import org.neo4j.token.api.TokenHolder;

import static java.util.stream.Collectors.toList;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.io.layout.recordstorage.RecordDatabaseLayout.convert;
import static org.neo4j.kernel.impl.store.StoreType.META_DATA;
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
        return versionInformation( MetaDataStore.versionLongToString( storeId.getStoreVersion() ) );
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
        IdGeneratorMigrator idGeneratorMigrator = new IdGeneratorMigrator( fs, pageCache, config, cacheTracer );
        return List.of( recordStorageMigrator, idGeneratorMigrator );
    }

    @Override
    public StorageEngine instantiate( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, TokenHolders tokenHolders,
            SchemaState schemaState, ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter, LockService lockService,
            IdGeneratorFactory idGeneratorFactory, IdController idController, DatabaseHealth databaseHealth, LogProvider internalLogProvider,
            LogProvider userLogProvider, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, PageCacheTracer cacheTracer, boolean createStoreIfNotExists,
            DatabaseReadOnlyChecker readOnlyChecker, MemoryTracker memoryTracker )
    {
        return new RecordStorageEngine( convert( databaseLayout ), config, pageCache, fs, internalLogProvider, userLogProvider, tokenHolders, schemaState,
                constraintSemantics, indexConfigCompleter, lockService, databaseHealth, idGeneratorFactory, idController, recoveryCleanupWorkCollector,
                cacheTracer, createStoreIfNotExists, memoryTracker, readOnlyChecker, new CommandLockVerification.Factory.RealFactory( config ),
                LockVerificationMonitor.Factory.defaultFactory( config ) );
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
    public List<SchemaRule> loadSchemaRules( FileSystemAbstraction fs, PageCache pageCache, Config config, DatabaseLayout layout,
            CursorContext cursorContext )
    {
        RecordDatabaseLayout databaseLayout = convert( layout );
        StoreFactory factory =
                new StoreFactory( databaseLayout, config, new DefaultIdGeneratorFactory( fs, immediate(), databaseLayout.getDatabaseName() ), pageCache, fs,
                        NullLogProvider.getInstance(), PageCacheTracer.NULL, readOnly() );
        try ( NeoStores stores = factory.openNeoStores( false, StoreType.SCHEMA, StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY );
              var storeCursors = new CachedStoreCursors( stores, cursorContext ) )
        {
            stores.start( cursorContext );
            TokenHolders tokenHolders = tokenHoldersForSchemaStore( stores, new ReadOnlyTokenCreator(), storeCursors );
            List<SchemaRule> rules = new ArrayList<>();
            new SchemaStorage( stores.getSchemaStore(), tokenHolders, () -> KernelVersion.LATEST ).getAll( storeCursors ).forEach( rules::add );
            return rules;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
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
        // count store, index statistics and label scan store are not mandatory stores to have since they can be automatically rebuilt
        storeFiles.remove( recordLayout.countStore() );
        storeFiles.remove( recordLayout.relationshipGroupDegreesStore() );
        storeFiles.remove( recordLayout.indexStatisticsStore() );
        storeFiles.remove( recordLayout.labelScanStore() );
        storeFiles.remove( recordLayout.relationshipTypeScanStore() );
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

    public static SchemaRuleMigrationAccess createMigrationTargetSchemaRuleAccess( NeoStores stores, CursorContext cursorContext, MemoryTracker memoryTracker )
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
        TokenHolders dstTokenHolders = tokenHoldersForSchemaStore( stores, propertyKeyTokenCreator, storeCursors );
        return new SchemaRuleMigrationAccessImpl( stores, new SchemaStorage( dstSchema, dstTokenHolders, () -> KernelVersion.LATEST ), cursorContext,
                memoryTracker, storeCursors );
    }

    private static TokenHolders tokenHoldersForSchemaStore( NeoStores stores, TokenCreator propertyKeyTokenCreator, StoreCursors storeCursors )
    {
        TokenHolder propertyKeyTokens = new DelegatingTokenHolder( propertyKeyTokenCreator, TokenHolder.TYPE_PROPERTY_KEY );
        TokenHolders dstTokenHolders = new TokenHolders( propertyKeyTokens, StoreTokens.createReadOnlyTokenHolder( TokenHolder.TYPE_LABEL ),
                StoreTokens.createReadOnlyTokenHolder( TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        dstTokenHolders.propertyKeyTokens().setInitialTokens( stores.getPropertyKeyTokenStore().getTokens( storeCursors ) );
        return dstTokenHolders;
    }
}
