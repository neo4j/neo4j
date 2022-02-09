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

import org.apache.commons.lang3.StringUtils;
import org.eclipse.collections.impl.factory.Sets;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;

import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.Monitor;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.Input.Estimates;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.ReadableGroups;
import org.neo4j.internal.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.ScanOnOpenOverwritingIdGeneratorFactory;
import org.neo4j.internal.id.ScanOnOpenReadOnlyIdGeneratorFactory;
import org.neo4j.internal.id.SchemaIdType;
import org.neo4j.internal.recordstorage.RecordNodeCursor;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.CommonDatabaseFile;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseFile;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.storemigration.legacy.SchemaStore44Reader;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.KernelVersionRepository;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.api.SchemaRule44;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.format.CapabilityType;
import org.neo4j.storageengine.migration.AbstractStoreMigrationParticipant;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.util.VisibleForTesting;

import static java.util.Arrays.asList;
import static org.apache.commons.io.IOUtils.lineIterator;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.batchimport.Configuration.defaultConfiguration;
import static org.neo4j.internal.recordstorage.RecordStorageEngineFactory.createMigrationTargetSchemaRuleAccess;
import static org.neo4j.internal.recordstorage.StoreTokens.allTokens;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForVersion;
import static org.neo4j.kernel.impl.store.format.StoreVersion.ALIGNED_V5_0;
import static org.neo4j.kernel.impl.store.format.StoreVersion.HIGH_LIMIT_V5_0;
import static org.neo4j.kernel.impl.store.format.StoreVersion.STANDARD_V5_0;
import static org.neo4j.kernel.impl.storemigration.FileOperation.COPY;
import static org.neo4j.kernel.impl.storemigration.FileOperation.DELETE;
import static org.neo4j.kernel.impl.storemigration.FileOperation.MOVE;
import static org.neo4j.kernel.impl.storemigration.StoreMigratorFileOperation.fileOperation;
import static org.neo4j.kernel.impl.transaction.log.LogTailMetadata.EMPTY_LOG_TAIL;

/**
 * Migrates a {@link RecordStorageEngine} store from one version to another.
 * <p>
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the migration code is specific for the current upgrade and changes with each store format version.
 * <p>
 * Just one out of many potential participants in a migration.
 */
public class RecordStorageMigrator extends AbstractStoreMigrationParticipant
{
    private static final char TX_LOG_COUNTERS_SEPARATOR = 'A';
    private static final String RECORD_STORAGE_MIGRATION_TAG = "recordStorageMigration";
    private static final String NODE_CHUNK_MIGRATION_TAG = "nodeChunkMigration";
    private static final String RELATIONSHIP_CHUNK_MIGRATION_TAG = "relationshipChunkMigration";

    private final Config config;
    private final LogService logService;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final PageCacheTracer pageCacheTracer;
    private final JobScheduler jobScheduler;
    private final CursorContextFactory contextFactory;
    private final BatchImporterFactory batchImporterFactory;
    private final MemoryTracker memoryTracker;

    public RecordStorageMigrator( FileSystemAbstraction fileSystem, PageCache pageCache, PageCacheTracer pageCacheTracer, Config config,
            LogService logService, JobScheduler jobScheduler,
            CursorContextFactory contextFactory,
            BatchImporterFactory batchImporterFactory, MemoryTracker memoryTracker )
    {
        super( "Store files" );
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.config = config;
        this.logService = logService;
        this.jobScheduler = jobScheduler;
        this.pageCacheTracer = pageCacheTracer;
        this.contextFactory = contextFactory;
        this.batchImporterFactory = batchImporterFactory;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public void migrate( DatabaseLayout directoryLayoutArg, DatabaseLayout migrationLayoutArg, ProgressReporter progressReporter, StoreVersion fromVersion,
            StoreVersion toVersion, IndexImporterFactory indexImporterFactory, LogTailMetadata tailMetadata ) throws IOException, KernelException
    {
        RecordDatabaseLayout directoryLayout = RecordDatabaseLayout.convert( directoryLayoutArg );
        RecordDatabaseLayout migrationLayout = RecordDatabaseLayout.convert( migrationLayoutArg );
        // Extract information about the last transaction from legacy neostore
        try ( var cursorContext = contextFactory.create( RECORD_STORAGE_MIGRATION_TAG ) )
        {
            long lastTxId = tailMetadata.getLastCommittedTransaction().transactionId();
            TransactionId lastTxInfo = tailMetadata.getLastCommittedTransaction();
            LogPosition lastTxLogPosition = tailMetadata.getLastTransactionLogPosition();
            long checkpointLogVersion = tailMetadata.getCheckpointLogVersion();
            // Write the tx checksum to file in migrationStructure, because we need it later when moving files into storeDir
            writeLastTxInformation( migrationLayout, lastTxInfo );
            writeLastTxLogPosition( migrationLayout, lastTxLogPosition );

            RecordFormats oldFormat = selectForVersion( fromVersion.storeVersion() );
            RecordFormats newFormat = selectForVersion( toVersion.storeVersion() );
            boolean requiresDynamicStoreMigration = !newFormat.dynamic().equals( oldFormat.dynamic() );
            boolean requiresPropertyMigration =
                    !newFormat.property().equals( oldFormat.property() ) || requiresDynamicStoreMigration;
            // The FORMAT capability also includes the format family so this comparison is enough
            if ( !oldFormat.hasCompatibleCapabilities( newFormat, CapabilityType.FORMAT ) )
            {
                // Some form of migration is required (a fallback/catch-all option)
                migrateWithBatchImporter(
                        directoryLayout, migrationLayout, lastTxId, lastTxInfo.checksum(), lastTxLogPosition.getLogVersion(),
                        lastTxLogPosition.getByteOffset(), checkpointLogVersion, progressReporter, oldFormat, newFormat,
                        requiresDynamicStoreMigration, requiresPropertyMigration, indexImporterFactory );
            }

            if ( requiresPropertyMigration )
            {
                // Migration with the batch importer would have copied the property, property key token, and property key name stores
                // into the migration directory, which is needed for the schema store migration. However, it might choose to skip
                // store files that it did not change, or didn't migrate. It could also be that we didn't do a normal store
                // format migration. Then those files will be missing and the schema store migration would create empty ones that
                // ended up overwriting the real ones. Those are then deleted by the migration afterwards, to avoid overwriting the
                // actual files in the final copy from the migration directory, to the real store directory. When do a schema store
                // migration, we will be reading and writing properties, and property key tokens, so we need those files.
                // To get them, we just copy them again with the SKIP strategy, so we avoid overwriting any files that might have
                // been migrated earlier.
                List<DatabaseFile> databaseFiles =
                        asList( RecordDatabaseFile.PROPERTY_STORE, RecordDatabaseFile.PROPERTY_ARRAY_STORE, RecordDatabaseFile.PROPERTY_STRING_STORE,
                                RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE, RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                                RecordDatabaseFile.LABEL_TOKEN_STORE, RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE,
                                RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE, RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE );
                fileOperation( COPY, fileSystem, directoryLayout, migrationLayout, databaseFiles, true, true,
                        ExistingTargetStrategy.SKIP );
                migrateSchemaStore( directoryLayout, migrationLayout, oldFormat, newFormat, cursorContext, memoryTracker );
            }

            // First migration in 5.0 - when we are ready to force migration to these formats we can instead do a check on if the
            // KernelVersion is before 5.0 (assuming we set the kernel version to 5.0 in this migration).
            if ( need50Migration( oldFormat, newFormat ) )
            {
                List<DatabaseFile> databaseFiles =
                        asList( RecordDatabaseFile.SCHEMA_STORE,
                                RecordDatabaseFile.PROPERTY_STORE, RecordDatabaseFile.PROPERTY_ARRAY_STORE, RecordDatabaseFile.PROPERTY_STRING_STORE,
                                RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE, RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                                RecordDatabaseFile.LABEL_TOKEN_STORE, RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE,
                                RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE, RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE );
                fileOperation( COPY, fileSystem, directoryLayout, migrationLayout, databaseFiles, true, true,
                        ExistingTargetStrategy.SKIP );

                IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate(), migrationLayout.getDatabaseName() );
                IdGeneratorFactory srcIdGeneratorFactory = new ScanOnOpenReadOnlyIdGeneratorFactory();

                StoreFactory dstFactory = createStoreFactory( migrationLayout, newFormat, idGeneratorFactory );
                StoreFactory srcFactory = createStoreFactory( directoryLayout, oldFormat, srcIdGeneratorFactory );

                StoreId storeId;
                UUID externalId;
                UUID databaseId;
                try ( NeoStores srcStore = srcFactory.openNeoStores( false, StoreType.META_DATA ) )
                {
                    MetaDataStore oldMetadataStore = srcStore.getMetaDataStore();

                    try ( PageCursor pageCursor = oldMetadataStore.openPageCursorForReading( 0, cursorContext ) )
                    {
                        MetaDataRecord record = new MetaDataRecord();

                        long creationTime = getValueOrDefault( oldMetadataStore, 0, record, pageCursor );
                        long random = getValueOrDefault( oldMetadataStore, 1, record, pageCursor );
                        long storeVersion = getValueOrDefault( oldMetadataStore, 4, record, pageCursor );

                        long externalMostBits = getValueOrDefault( oldMetadataStore, 16, record, pageCursor );
                        long externalLeastBits = getValueOrDefault( oldMetadataStore, 17, record, pageCursor );

                        long databaseMostBits = getValueOrDefault( oldMetadataStore, 20, record, pageCursor );
                        long databaseLeastBits = getValueOrDefault( oldMetadataStore, 21, record, pageCursor );

                        storeId = new StoreId( creationTime, random, storeVersion );
                        externalId = new UUID( externalMostBits, externalLeastBits );
                        databaseId = new UUID( databaseMostBits, databaseLeastBits );
                    }
                }

                try ( NeoStores dstStore = dstFactory.openNeoStores( true, StoreType.SCHEMA, StoreType.PROPERTY, StoreType.META_DATA,
                                                                     StoreType.LABEL_TOKEN, StoreType.RELATIONSHIP_TYPE_TOKEN, StoreType.PROPERTY_KEY_TOKEN );
                      var dstCursors = new CachedStoreCursors( dstStore, cursorContext );
                      var dstAccess = createMigrationTargetSchemaRuleAccess( dstStore, contextFactory, memoryTracker, dstStore.getMetaDataStore() ) )
                {
                    MetaDataStore metaDataStore = dstStore.getMetaDataStore();
                    metaDataStore.regenerateMetadata( storeId, externalId, cursorContext );
                    metaDataStore.setDatabaseIdUuid( databaseId, cursorContext );

                    var dstTokensHolders = createTokenHolders( dstStore, dstCursors );
                    try ( var schemaStore44Reader = getSchemaStore44Reader( migrationLayout, oldFormat, idGeneratorFactory, dstStore, dstTokensHolders ) )
                    {
                        persistNodeLabelIndex( dstAccess );
                        filterOutBtreeIndexes( schemaStore44Reader, dstCursors, dstAccess, dstTokensHolders,
                                               SYSTEM_DATABASE_NAME.equals( directoryLayoutArg.getDatabaseName() ) );
                    }
                    dstStore.flush( cursorContext );
                }
            }

            MetaDataStore.setRecord( pageCache, migrationLayout.metadataStore(), STORE_VERSION, StoreVersion.versionStringToLong( toVersion.storeVersion() ),
                    migrationLayout.getDatabaseName(), cursorContext );
        }
    }

    private long getValueOrDefault( MetaDataStore oldMetadataStore, int id, MetaDataRecord record, PageCursor pageCursor )
    {
        MetaDataRecord recordByCursor = oldMetadataStore.getRecordByCursor( id, record, RecordLoad.FORCE, pageCursor );
        return recordByCursor.inUse() ? recordByCursor.getValue() : -1;
    }

    /**
     * Make sure node label index is correctly persisted. There are two situations where it might not be:
     * 1. As part of upgrade to 4.3/4.4 a schema record without any properties was written to the schema store.
     *    This record was used to represent the old label scan store (< 4.3) converted to node label index.
     *    In this case we need to rewrite this schema to give it the properties it should have. In this way we
     *    can keep the index id.
     * 2. If no write transaction happened after upgrade of old store to 4.3/4.4 the upgrade transaction was never injected
     *    and node label index (as schema rule with no properties) was never persisted at all. In this case
     *    {@link IndexDescriptor#INJECTED_NLI} will be injected by {@link org.neo4j.internal.recordstorage.SchemaStorage}
     *    when reading schema rules. In this case we materialise this injected rule with a new real id (instead of -2).
     */
    private static void persistNodeLabelIndex( SchemaRuleMigrationAccess dstAccess ) throws KernelException
    {
        SchemaRule foundNLIThatNeedsUpdate = null;
        Iterable<SchemaRule> all = dstAccess.getAll();
        for ( SchemaRule schemaRule : all )
        {
            // This is the previous labelscanstore that we want to make into a real complete record
            if ( schemaRule.schema().equals( IndexDescriptor.NLI_PROTOTYPE.schema() ) )
            {
                // It was never persisted and now needs to persisted with a new id.
                if ( schemaRule.getId() == IndexDescriptor.INJECTED_NLI_ID )
                {
                    foundNLIThatNeedsUpdate = IndexDescriptor.NLI_PROTOTYPE.materialise( dstAccess.nextId() );
                    break;
                }
                // It was persisted and is a record with no properties, rewriting it will give it properties.
                if ( IndexDescriptor.NLI_GENERATED_NAME.equals( schemaRule.getName() ) )
                {
                    foundNLIThatNeedsUpdate = schemaRule;
                    break;
                }
            }
        }
        if ( foundNLIThatNeedsUpdate != null )
        {
            dstAccess.writeSchemaRule( foundNLIThatNeedsUpdate );
        }
    }

    /**
     * If a BTREE index has a replacement index - RANGE, TEXT or POINT index on same schema - the BTREE index will be removed.
     * If BTREE index doesn't have any replacement, an exception will be thrown.
     * If a constraint backed by a BTREE index has a replacement constraint - constraint of same type, on same schema,
     * backed by other index type than BTREE - the BTREE backed constraint will be removed.
     * If constraint backed by BTREE index doesn't have any replacement, an exception will be thrown.
     *
     * The SchemaStore (and naturally also the PropertyStore) will be updated non-transactionally.
     *
     * BTREE index type was deprecated in 4.4 and removed in 5.0.
     *
     * @param schemaStore44Reader {@link SchemaStore44Reader} reader for legacy schema store
     * @param dstCursors {@link StoreCursors} cursors to use when reading from legacy store
     * @param dstAccess {@link SchemaRuleMigrationAccess} access to the SchemaStore at migration destination.
     * @param dstTokensHolders {@link TokenHolders} token holders for migration destination store.
     * @param systemDb true if the migrating database is system db, otherwise false.
     * @throws KernelException if BTREE index or BTREE backed constraint lacks replacement.
     */
    @VisibleForTesting
    static void filterOutBtreeIndexes( SchemaStore44Reader schemaStore44Reader, StoreCursors dstCursors, SchemaRuleMigrationAccess dstAccess,
                                       TokenHolders dstTokensHolders, boolean systemDb ) throws KernelException
    {
        if ( systemDb )
        {
            // Forcefully replace all
            return;
        }

        var all = schemaStore44Reader.loadAllSchemaRules( dstCursors );

        // Organize indexes by SchemaDescriptor

        var indexesBySchema = new HashMap<SchemaDescriptor,List<SchemaRule44.Index>>();
        var indexesByName = new HashMap<String,SchemaRule44.Index>();
        var constraintBySchemaAndType = new HashMap<SchemaDescriptor,EnumMap<SchemaRule44.ConstraintRuleType,List<SchemaRule44.Constraint>>>();
        for ( var schemaRule : all )
        {
            if ( schemaRule instanceof SchemaRule44.Index index )
            {
                indexesByName.put( index.name(), index );
                if ( !index.unique() )
                {
                    indexesBySchema.computeIfAbsent( index.schema(), k -> new ArrayList<>() ).add( index );
                }
            }
            if ( schemaRule instanceof SchemaRule44.Constraint constraint )
            {
                if ( constraint.constraintRuleType().isIndexBacked() )
                {
                    var constraintsByType =
                            constraintBySchemaAndType.computeIfAbsent( constraint.schema(), k -> new EnumMap<>( SchemaRule44.ConstraintRuleType.class ) );
                    constraintsByType.computeIfAbsent( constraint.constraintRuleType(), k -> new ArrayList<>() ).add( constraint );
                }
            }
        }

        // Figure out which btree indexes that has replacement and can be deleted and which don't
        var indexesToDelete = new ArrayList<SchemaRule44.Index>();
        var nonReplacedIndexes = new ArrayList<SchemaRule44.Index>();
        for ( var schema : indexesBySchema.keySet() )
        {
            SchemaRule44.Index btreeIndex = null;
            boolean hasReplacement = false;
            for ( SchemaRule44.Index index : indexesBySchema.get( schema ) )
            {
                if ( index.indexType() == SchemaRule44.IndexType.BTREE )
                {
                    btreeIndex = index;
                }
                else if ( index.indexType() == SchemaRule44.IndexType.RANGE ||
                          index.indexType() == SchemaRule44.IndexType.TEXT ||
                          index.indexType() == SchemaRule44.IndexType.POINT )
                {
                    hasReplacement = true;
                }
            }
            if ( btreeIndex != null )
            {
                if ( hasReplacement )
                {
                    indexesToDelete.add( btreeIndex );
                }
                else
                {
                    nonReplacedIndexes.add( btreeIndex );
                }
            }
        }

        // Figure out which constraints, backed by btree indexes, that has replacement and can be deleted and which don't
        var constraintsToDelete = new ArrayList<SchemaRule44.Constraint>();
        var nonReplacedConstraints = new ArrayList<SchemaRule44.Constraint>();
        constraintBySchemaAndType.values() // Collection<EnumMap<ConstraintType,List<ConstraintDescriptor>>>
                                 .stream().flatMap( enumMap -> enumMap.values().stream() ) // Stream<List<ConstraintDescriptor>>
                                 .forEach( constraintsGroupedBySchemaAndType ->
                                           {
                                               SchemaRule44.Constraint btreeConstraint = null;
                                               SchemaRule44.Index backingBtreeIndex = null;
                                               boolean hasReplacement = false;
                                               for ( var constraint : constraintsGroupedBySchemaAndType )
                                               {
                                                   var backingIndex = indexesByName.get( constraint.name() );
                                                   if ( backingIndex.indexType() == SchemaRule44.IndexType.BTREE )
                                                   {
                                                       btreeConstraint = constraint;
                                                       backingBtreeIndex = backingIndex;
                                                   }
                                                   else if ( backingIndex.indexType() == SchemaRule44.IndexType.RANGE )
                                                   {
                                                       hasReplacement = true;
                                                   }
                                               }
                                               if ( btreeConstraint != null )
                                               {
                                                   if ( hasReplacement )
                                                   {
                                                       constraintsToDelete.add( btreeConstraint );
                                                       indexesToDelete.add( backingBtreeIndex );
                                                   }
                                                   else
                                                   {
                                                       nonReplacedConstraints.add( btreeConstraint );
                                                   }
                                               }
                                           }
                                 );

        // Throw if non-replaced index exists
        if ( !nonReplacedIndexes.isEmpty() || !nonReplacedConstraints.isEmpty() )
        {
            var nonReplacedIndexString = new StringJoiner( ", ", "[", "]" );
            var nonReplacedConstraintsString = new StringJoiner( ", ", "[", "]" );
            nonReplacedIndexes.forEach( index -> nonReplacedIndexString.add( index.userDescription( dstTokensHolders ) ) );
            nonReplacedConstraints.forEach( constraint -> nonReplacedConstraintsString.add( constraint.userDescription( dstTokensHolders ) ) );
            throw new IllegalStateException(
                    "Migration will remove all BTREE indexes and constraints backed by BTREE indexes. " +
                    "To guard from unintentionally removing indexes or constraints, " +
                    "all BTREE indexes or constraints backed by BTREE indexes must either have been removed before this migration or " +
                    "need to have a valid replacement. " +
                    "Indexes can be replaced by RANGE, TEXT or POINT index and constraints can be replaced by constraints backed by RANGE index. " +
                    "Please drop your indexes and constraints or create replacements and retry the migration. " +
                    "The indexes and constraints without replacement are: " + nonReplacedIndexString + " and " + nonReplacedConstraintsString
            );
        }

        // Delete all btree indexes
        for ( SchemaRule44.Index indexToDelete : indexesToDelete )
        {
            dstAccess.deleteSchemaRule( indexToDelete.id() );
        }
        // Delete all btree backed constraints
        for ( SchemaRule44.Constraint constraintToDelete : constraintsToDelete )
        {
            dstAccess.deleteSchemaRule( constraintToDelete.id() );
        }
    }

    void writeLastTxInformation( DatabaseLayout migrationStructure, TransactionId txInfo ) throws IOException
    {
        writeTxLogCounters( fileSystem, lastTxInformationFile( migrationStructure ),
                txInfo.transactionId(), txInfo.checksum(), txInfo.commitTimestamp() );
    }

    void writeLastTxLogPosition( DatabaseLayout migrationStructure, LogPosition lastTxLogPosition ) throws IOException
    {
        writeTxLogCounters( fileSystem, lastTxLogPositionFile( migrationStructure ),
                lastTxLogPosition.getLogVersion(), lastTxLogPosition.getByteOffset() );
    }

    TransactionId readLastTxInformation( DatabaseLayout migrationStructure ) throws IOException
    {
        long[] counters = readTxLogCounters( fileSystem, lastTxInformationFile( migrationStructure ), 3 );
        return new TransactionId( counters[0], (int) counters[1], counters[2] );
    }

    LogPosition readLastTxLogPosition( DatabaseLayout migrationStructure ) throws IOException
    {
        long[] counters = readTxLogCounters( fileSystem, lastTxLogPositionFile( migrationStructure ), 2 );
        return new LogPosition( counters[0], counters[1] );
    }

    private static void writeTxLogCounters( FileSystemAbstraction fs, Path path, long... counters ) throws IOException
    {
        try ( Writer writer = fs.openAsWriter( path, StandardCharsets.UTF_8, false ) )
        {
            writer.write( StringUtils.join( counters, TX_LOG_COUNTERS_SEPARATOR ) );
        }
    }

    private static long[] readTxLogCounters( FileSystemAbstraction fs, Path path, int numberOfCounters )
            throws IOException
    {
        try ( var reader = fs.openAsReader( path, StandardCharsets.UTF_8 ) )
        {
            String line = lineIterator( reader ).next();
            String[] split = StringUtils.split( line, TX_LOG_COUNTERS_SEPARATOR );
            if ( split.length != numberOfCounters )
            {
                throw new IllegalArgumentException( "Unexpected number of tx counters '" + numberOfCounters +
                                                    "', file contains: '" + line + "'" );
            }
            long[] counters = new long[numberOfCounters];
            for ( int i = 0; i < split.length; i++ )
            {
                counters[i] = Long.parseLong( split[i] );
            }
            return counters;
        }
    }

    private static Path lastTxInformationFile( DatabaseLayout migrationStructure )
    {
        return migrationStructure.file( "lastxinformation" );
    }

    private static Path lastTxLogPositionFile( DatabaseLayout migrationStructure )
    {
        return migrationStructure.file( "lastxlogposition" );
    }

    private void migrateWithBatchImporter( RecordDatabaseLayout sourceDirectoryStructure, RecordDatabaseLayout migrationDirectoryStructure, long lastTxId,
            int lastTxChecksum, long lastTxLogVersion, long lastTxLogByteOffset, long lastCheckpointLogVersion, ProgressReporter progressReporter,
            RecordFormats oldFormat, RecordFormats newFormat, boolean requiresDynamicStoreMigration, boolean requiresPropertyMigration,
            IndexImporterFactory indexImporterFactory ) throws IOException
    {
        prepareBatchImportMigration( sourceDirectoryStructure, migrationDirectoryStructure, oldFormat, newFormat );

        try ( NeoStores legacyStore = instantiateLegacyStore( oldFormat, sourceDirectoryStructure ) )
        {
            Configuration importConfig = new Configuration.Overridden( defaultConfiguration(), config );
            AdditionalInitialIds additionalInitialIds =
                    readAdditionalIds( lastTxId, lastTxChecksum, lastTxLogVersion, lastTxLogByteOffset, lastCheckpointLogVersion );

            try ( var storeCursors = new CachedStoreCursors( legacyStore, CursorContext.NULL_CONTEXT ) )
            {
                // We have to make sure to keep the token ids if we're migrating properties/labels
                BatchImporter importer = batchImporterFactory.instantiate(
                        migrationDirectoryStructure, fileSystem, pageCacheTracer, importConfig, logService,
                        migrationBatchImporterMonitor( legacyStore, progressReporter,
                                importConfig ), additionalInitialIds, EMPTY_LOG_TAIL, config, Monitor.NO_MONITOR, jobScheduler,
                        Collector.STRICT, LogFilesInitializer.NULL, indexImporterFactory, memoryTracker, contextFactory );
                InputIterable nodes =
                        () -> legacyNodesAsInput( legacyStore, requiresPropertyMigration, memoryTracker, storeCursors, contextFactory );
                InputIterable relationships =
                        () -> legacyRelationshipsAsInput( legacyStore, requiresPropertyMigration, contextFactory, memoryTracker, storeCursors );
                long propertyStoreSize = storeSize( legacyStore.getPropertyStore() ) / 2 + storeSize( legacyStore.getPropertyStore().getStringStore() ) / 2 +
                        storeSize( legacyStore.getPropertyStore().getArrayStore() ) / 2;
                Estimates estimates =
                        Input.knownEstimates( legacyStore.getNodeStore().getNumberOfIdsInUse(), legacyStore.getRelationshipStore().getNumberOfIdsInUse(),
                                legacyStore.getPropertyStore().getNumberOfIdsInUse(), legacyStore.getPropertyStore().getNumberOfIdsInUse(),
                                propertyStoreSize / 2, propertyStoreSize / 2, 0 /*node labels left as 0 for now*/ );
                importer.doImport( Input.input( nodes, relationships, IdType.ACTUAL, estimates, ReadableGroups.EMPTY ) );
            }

            // During migration the batch importer doesn't necessarily writes all entities, depending on
            // which stores needs migration. Node, relationship, relationship group stores are always written
            // anyways and cannot be avoided with the importer, but delete the store files that weren't written
            // (left empty) so that we don't overwrite those in the real store directory later.
            Collection<DatabaseFile> storesToDeleteFromMigratedDirectory = new ArrayList<>();
            storesToDeleteFromMigratedDirectory.add( CommonDatabaseFile.METADATA_STORE );
            if ( !requiresPropertyMigration )
            {
                // We didn't migrate properties, so the property stores in the migrated store are just empty/bogus
                storesToDeleteFromMigratedDirectory.addAll( asList(
                        RecordDatabaseFile.PROPERTY_STORE,
                        RecordDatabaseFile.PROPERTY_STRING_STORE,
                        RecordDatabaseFile.PROPERTY_ARRAY_STORE ) );
            }
            if ( !requiresDynamicStoreMigration )
            {
                // We didn't migrate labels (dynamic node labels) or any other dynamic store.
                storesToDeleteFromMigratedDirectory.addAll( asList(
                        RecordDatabaseFile.NODE_LABEL_STORE,
                        RecordDatabaseFile.LABEL_TOKEN_STORE,
                        RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE,
                        RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE,
                        RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE,
                        RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE,
                        RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                        RecordDatabaseFile.SCHEMA_STORE ) );
            }

            fileOperation( DELETE, fileSystem, migrationDirectoryStructure, migrationDirectoryStructure, storesToDeleteFromMigratedDirectory, true, true,
                    null );
        }
    }

    private static long storeSize( CommonAbstractStore<? extends AbstractBaseRecord,? extends StoreHeader> store )
    {
        return store.getNumberOfIdsInUse() * store.getRecordSize();
    }

    private NeoStores instantiateLegacyStore( RecordFormats format, RecordDatabaseLayout directoryStructure )
    {
        return new StoreFactory( directoryStructure, config, new ScanOnOpenReadOnlyIdGeneratorFactory(), pageCache, fileSystem,
                format, NullLogProvider.getInstance(), contextFactory, readOnly(), EMPTY_LOG_TAIL, Sets.immutable.empty() )
                .openAllNeoStores( true );
    }

    private void prepareBatchImportMigration( RecordDatabaseLayout sourceDirectoryStructure, RecordDatabaseLayout migrationStrcuture, RecordFormats oldFormat,
            RecordFormats newFormat ) throws IOException
    {
        createStore( migrationStrcuture, newFormat );

        // We use the batch importer for migrating the data, and we use it in a special way where we only
        // rewrite the stores that have actually changed format. We know that to be node and relationship
        // stores. Although since the batch importer also populates the counts store, all labels need to
        // be read, i.e. both inlined and those existing in dynamic records. That's why we need to copy
        // that dynamic record store over before doing the "batch import".
        //   Copying this file just as-is assumes that the format hasn't change. If that happens we're in
        // a different situation, where we first need to migrate this file.

        // The token stores also need to be migrated because we use those as-is and ask for their high ids
        // when using the importer in the store migration scenario.
        RecordDatabaseFile[] storesFilesToMigrate = {
                RecordDatabaseFile.LABEL_TOKEN_STORE, RecordDatabaseFile.LABEL_TOKEN_NAMES_STORE,
                RecordDatabaseFile.PROPERTY_KEY_TOKEN_STORE, RecordDatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE, RecordDatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE,
                RecordDatabaseFile.NODE_LABEL_STORE};
        if ( newFormat.dynamic().equals( oldFormat.dynamic() ) )
        {
            fileOperation( COPY, fileSystem, sourceDirectoryStructure, migrationStrcuture, Arrays.asList( storesFilesToMigrate ), true,
                    true, ExistingTargetStrategy.OVERWRITE );
        }
        else
        {
            // Migrate all token stores and dynamic node label ids, keeping their ids intact
            DirectRecordStoreMigrator migrator = new DirectRecordStoreMigrator( pageCache, fileSystem, config, contextFactory );

            StoreType[] storesToMigrate = {
                    StoreType.LABEL_TOKEN, StoreType.LABEL_TOKEN_NAME,
                    StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY_KEY_TOKEN_NAME,
                    StoreType.RELATIONSHIP_TYPE_TOKEN, StoreType.RELATIONSHIP_TYPE_TOKEN_NAME,
                    StoreType.NODE_LABEL};

            // Migrate these stores silently because they are usually very small
            ProgressReporter progressReporter = ProgressReporter.SILENT;

            migrator.migrate( sourceDirectoryStructure, oldFormat, migrationStrcuture, newFormat, progressReporter, storesToMigrate, StoreType.NODE );
        }

        // Since we'll be using these stores in the batch importer where we don't have this fine control over IdGeneratorFactory
        // it's easier to just figure out highId and create simple id files of the current format at that highId.
        createStoreFactory( migrationStrcuture, newFormat,
                new ScanOnOpenOverwritingIdGeneratorFactory( fileSystem, migrationStrcuture.getDatabaseName() ) ).openAllNeoStores().close();
    }

    private void createStore( RecordDatabaseLayout migrationDirectoryStructure, RecordFormats newFormat )
    {
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate(), migrationDirectoryStructure.getDatabaseName() );
        createStoreFactory( migrationDirectoryStructure, newFormat, idGeneratorFactory ).openAllNeoStores( true ).close();
    }

    private StoreFactory createStoreFactory( RecordDatabaseLayout databaseLayout, RecordFormats formats, IdGeneratorFactory idGeneratorFactory )
    {
        return new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache, fileSystem, formats, NullLogProvider.getInstance(), contextFactory,
                writable(), EMPTY_LOG_TAIL, immutable.empty() );
    }

    private static AdditionalInitialIds readAdditionalIds( final long lastTxId, final int lastTxChecksum, final long lastTxLogVersion,
            final long lastTxLogByteOffset, long lastCheckpointLogVersion )
    {
        return new AdditionalInitialIds()
        {
            @Override
            public long lastCommittedTransactionId()
            {
                return lastTxId;
            }

            @Override
            public int lastCommittedTransactionChecksum()
            {
                return lastTxChecksum;
            }

            @Override
            public long lastCommittedTransactionLogVersion()
            {
                return lastTxLogVersion;
            }

            @Override
            public long lastCommittedTransactionLogByteOffset()
            {
                return lastTxLogByteOffset;
            }

            @Override
            public long checkpointLogVersion()
            {
                return lastCheckpointLogVersion;
            }
        };
    }

    private static ExecutionMonitor migrationBatchImporterMonitor( NeoStores legacyStore, final ProgressReporter progressReporter, Configuration config )
    {
        return new BatchImporterProgressMonitor(
                legacyStore.getNodeStore().getHighId(), legacyStore.getRelationshipStore().getHighId(),
                config, progressReporter );
    }

    private static InputIterator legacyRelationshipsAsInput( NeoStores legacyStore, boolean requiresPropertyMigration, CursorContextFactory contextFactory,
            MemoryTracker memoryTracker, StoreCursors storeCursors )
    {
        return new StoreScanAsInputIterator<>( legacyStore.getRelationshipStore() )
        {
            @Override
            public InputChunk newChunk()
            {
                var cursorContext = contextFactory.create( RELATIONSHIP_CHUNK_MIGRATION_TAG );
                return new RelationshipRecordChunk( new RecordStorageReader( legacyStore ), requiresPropertyMigration, cursorContext, storeCursors,
                        memoryTracker );
            }
        };
    }

    private static InputIterator legacyNodesAsInput( NeoStores legacyStore, boolean requiresPropertyMigration, MemoryTracker memoryTracker,
            StoreCursors storeCursors, CursorContextFactory contextFactory )
    {
        return new StoreScanAsInputIterator<>( legacyStore.getNodeStore() )
        {
            @Override
            public InputChunk newChunk()
            {
                var cursorContext = contextFactory.create( NODE_CHUNK_MIGRATION_TAG );
                return new NodeRecordChunk( new RecordStorageReader( legacyStore ), requiresPropertyMigration, cursorContext, storeCursors,
                        memoryTracker );
            }
        };
    }

    @Override
    public void moveMigratedFiles( DatabaseLayout migrationLayoutArg, DatabaseLayout directoryLayoutArg, String versionToUpgradeFrom,
            String versionToUpgradeTo ) throws IOException
    {
        RecordDatabaseLayout directoryLayout = RecordDatabaseLayout.convert( directoryLayoutArg );
        RecordDatabaseLayout migrationLayout = RecordDatabaseLayout.convert( migrationLayoutArg );
        // Move the migrated ones into the store directory
        fileOperation( MOVE, fileSystem, migrationLayout, directoryLayout,
                Iterables.iterable( RecordDatabaseFile.allValues() ), true, // allow to skip non existent source files
                true, ExistingTargetStrategy.OVERWRITE );

        RecordFormats oldFormat = selectForVersion( versionToUpgradeFrom );
        RecordFormats newFormat = selectForVersion( versionToUpgradeTo );
        if ( need50Migration( oldFormat, newFormat ) )
        {
            deleteBtreeIndexFiles( fileSystem, directoryLayout );
        }
    }

    /**
     * Migration of the schema store is invoked if the property store needs migration.
     */
    private void migrateSchemaStore( RecordDatabaseLayout directoryLayout, RecordDatabaseLayout migrationLayout, RecordFormats oldFormat,
            RecordFormats newFormat, CursorContext cursorContext, MemoryTracker memoryTracker ) throws IOException, KernelException
    {
        IdGeneratorFactory srcIdGeneratorFactory = new ScanOnOpenReadOnlyIdGeneratorFactory();
        StoreFactory srcFactory = createStoreFactory( directoryLayout, oldFormat, srcIdGeneratorFactory );
        StoreFactory dstFactory =
                createStoreFactory( migrationLayout, newFormat, new ScanOnOpenOverwritingIdGeneratorFactory( fileSystem, migrationLayout.getDatabaseName() ) );

        SchemaStorageCreator schemaStorageCreator = schemaStorageCreatorFlexible();
        // Token stores
        StoreType[] sourceStoresToOpen =
                new StoreType[]{StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY_KEY_TOKEN_NAME, StoreType.LABEL_TOKEN, StoreType.LABEL_TOKEN_NAME,
                                StoreType.RELATIONSHIP_TYPE_TOKEN, StoreType.RELATIONSHIP_TYPE_TOKEN_NAME};
        sourceStoresToOpen = ArrayUtil.concat( sourceStoresToOpen, schemaStorageCreator.additionalStoresToOpen() );
        try ( NeoStores srcStore = srcFactory.openNeoStores( sourceStoresToOpen );
              var srcCursors = new CachedStoreCursors( srcStore, cursorContext );
              NeoStores dstStore = dstFactory.openNeoStores( true, StoreType.SCHEMA, StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY );
              schemaStorageCreator )
        {
            dstStore.start( cursorContext );
            TokenHolders srcTokenHolders = createTokenHolders( srcStore, srcCursors );
            SchemaStorage srcAccess = schemaStorageCreator.create( srcStore, srcTokenHolders, cursorContext );

            try ( SchemaRuleMigrationAccess dstAccess =
                          createMigrationTargetSchemaRuleAccess( dstStore, contextFactory, memoryTracker );
                  var schemaCursors = schemaStorageCreator.getSchemaStorageTokenCursors( srcCursors ) )
            {
                migrateSchemaRules( srcAccess, dstAccess, schemaCursors );
            }

            dstStore.flush( cursorContext );
        }
    }

    private static void deleteBtreeIndexFiles( FileSystemAbstraction fs, RecordDatabaseLayout directoryLayout ) throws IOException
    {
        if ( directoryLayout.getDatabaseName().equals( SYSTEM_DATABASE_NAME ) )
        {
            // Don't deal with system db right now
            return;
        }

        fs.deleteRecursively( IndexDirectoryStructure.directoriesByProvider( directoryLayout.databaseDirectory() )
                                                     .forProvider( SchemaRule44.NATIVE_BTREE_10 )
                                                     .rootDirectory() );
        fs.deleteRecursively( IndexDirectoryStructure.directoriesByProvider( directoryLayout.databaseDirectory() )
                                                     .forProvider( SchemaRule44.LUCENE_NATIVE_30 )
                                                     .rootDirectory() );
    }

    static void migrateSchemaRules( SchemaStorage srcAccess, SchemaRuleMigrationAccess dstAccess,
            StoreCursors storeCursors ) throws KernelException
    {
        for ( SchemaRule rule : srcAccess.getAll( storeCursors ) )
        {
            dstAccess.writeSchemaRule( rule );
        }
    }

    private static boolean need50Migration( RecordFormats oldFormat, RecordFormats newFormat )
    {
        return (STANDARD_V5_0.versionString().equals( newFormat.storeVersion() ) ||
                ALIGNED_V5_0.versionString().equals( newFormat.storeVersion() ) ||
                HIGH_LIMIT_V5_0.versionString().equals( newFormat.storeVersion() )) &&
               !newFormat.storeVersion().equals( oldFormat.storeVersion() );
    }

    private static TokenHolders createTokenHolders( NeoStores stores, CachedStoreCursors cursors )
    {
        TokenHolders tokenHolders = new TokenHolders( StoreTokens.createReadOnlyTokenHolder( TokenHolder.TYPE_PROPERTY_KEY ),
                                                         StoreTokens.createReadOnlyTokenHolder( TokenHolder.TYPE_LABEL ),
                                                         StoreTokens.createReadOnlyTokenHolder( TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        tokenHolders.setInitialTokens( allTokens( stores ), cursors );
        return tokenHolders;
    }

    private SchemaStore44Reader getSchemaStore44Reader( RecordDatabaseLayout recordDatabaseLayout, RecordFormats formats, IdGeneratorFactory idGeneratorFactory,
                                                        NeoStores neoStores, TokenHolders tokenHolders )
    {
        return new SchemaStore44Reader(
                neoStores.getPropertyStore(),
                tokenHolders,
                neoStores.getMetaDataStore(),
                recordDatabaseLayout.schemaStore(),
                recordDatabaseLayout.idSchemaStore(),
                config,
                SchemaIdType.SCHEMA,
                idGeneratorFactory,
                pageCache,
                contextFactory,
                NullLogProvider.getInstance(),
                formats,
                recordDatabaseLayout.getDatabaseName(),
                org.eclipse.collections.api.factory.Sets.immutable.empty() );
    }

    @Override
    public void cleanup( DatabaseLayout migrationLayout ) throws IOException
    {
        fileSystem.deleteRecursively( migrationLayout.databaseDirectory() );
    }

    @Override
    public String toString()
    {
        return "Kernel StoreMigrator";
    }

    private static SchemaStorageCreator schemaStorageCreatorFlexible()
    {
        return new SchemaStorageCreator()
        {
            private SchemaStore schemaStore;

            @Override
            public SchemaStorage create( NeoStores store, TokenHolders tokenHolders, CursorContext cursorContext )
            {
                schemaStore = store.getSchemaStore();
                return new org.neo4j.internal.recordstorage.SchemaStorage( schemaStore, tokenHolders, KernelVersionRepository.LATEST );
            }

            @Override
            public StoreType[] additionalStoresToOpen()
            {
                // We need NeoStores to have those stores open so that we can get schema store out in create method.
                return new StoreType[]{StoreType.PROPERTY, StoreType.PROPERTY_STRING, StoreType.PROPERTY_ARRAY, StoreType.SCHEMA};
            }

            @Override
            public StoreCursors getSchemaStorageTokenCursors( StoreCursors srcCursors )
            {
                return srcCursors;
            }

            @Override
            public void close() throws IOException
            {
                IOUtils.closeAll( schemaStore );
            }
        };
    }

    private interface SchemaStorageCreator extends Closeable
    {
        SchemaStorage create( NeoStores store, TokenHolders tokenHolders, CursorContext cursorContext );

        StoreType[] additionalStoresToOpen();

        StoreCursors getSchemaStorageTokenCursors( StoreCursors srcCursors );
    }

    private static class NodeRecordChunk extends StoreScanChunk<RecordNodeCursor>
    {
        NodeRecordChunk( RecordStorageReader storageReader, boolean requiresPropertyMigration, CursorContext cursorContext, StoreCursors storeCursors,
                MemoryTracker memoryTracker )
        {
            super( storageReader.allocateNodeCursor( cursorContext, storeCursors ), storageReader, requiresPropertyMigration, cursorContext, storeCursors,
                    memoryTracker );
        }

        @Override
        protected void read( RecordNodeCursor cursor, long id )
        {
            cursor.single( id );
        }

        @Override
        protected void visitRecord( RecordNodeCursor record, InputEntityVisitor visitor )
        {
            visitor.id( record.entityReference() );
            visitor.labelField( record.getLabelField() );
            visitProperties( record, visitor );
        }
    }

    private static class RelationshipRecordChunk extends StoreScanChunk<StorageRelationshipScanCursor>
    {
        RelationshipRecordChunk( RecordStorageReader storageReader, boolean requiresPropertyMigration, CursorContext cursorContext, StoreCursors storeCursors,
                MemoryTracker memoryTracker )
        {
            super( storageReader.allocateRelationshipScanCursor( cursorContext, storeCursors ), storageReader, requiresPropertyMigration, cursorContext,
                    storeCursors, memoryTracker );
        }

        @Override
        protected void read( StorageRelationshipScanCursor cursor, long id )
        {
            cursor.single( id );
        }

        @Override
        protected void visitRecord( StorageRelationshipScanCursor record, InputEntityVisitor visitor )
        {
            visitor.startId( record.sourceNodeReference() );
            visitor.endId( record.targetNodeReference() );
            visitor.type( record.type() );
            visitProperties( record, visitor );
        }
    }

    private static class BatchImporterProgressMonitor extends CoarseBoundedProgressExecutionMonitor
    {
        private final ProgressReporter progressReporter;

        BatchImporterProgressMonitor( long highNodeId, long highRelationshipId,
                Configuration configuration,
                ProgressReporter progressReporter )
        {
            super( highNodeId, highRelationshipId, configuration );
            this.progressReporter = progressReporter;
            this.progressReporter.start( total() );
        }

        @Override
        protected void progress( long progress )
        {
            progressReporter.progress( progress );
        }
    }
}
