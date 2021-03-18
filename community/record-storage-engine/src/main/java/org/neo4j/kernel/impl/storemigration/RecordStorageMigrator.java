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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import org.neo4j.common.EntityType;
import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.Input.Estimates;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.ReadableGroups;
import org.neo4j.internal.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.ScanOnOpenOverwritingIdGeneratorFactory;
import org.neo4j.internal.id.ScanOnOpenReadOnlyIdGeneratorFactory;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.internal.recordstorage.RecordNodeCursor;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.RecordStorageCapability;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.storemigration.legacy.SchemaStorage35;
import org.neo4j.kernel.impl.storemigration.legacy.SchemaStore35;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.RangeLogVersionVisitor;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.format.CapabilityType;
import org.neo4j.storageengine.migration.AbstractStoreMigrationParticipant;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;

import static java.util.Arrays.asList;
import static org.apache.commons.io.IOUtils.lineIterator;
import static org.eclipse.collections.impl.factory.Sets.immutable;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.batchimport.Configuration.defaultConfiguration;
import static org.neo4j.internal.recordstorage.StoreTokens.allTokens;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.KERNEL_VERSION;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_CLOSED_TRANSACTION_LOG_VERSION;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_CHECKSUM;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_COMMIT_TIMESTAMP;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TRANSACTION_CHECKSUM;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TRANSACTION_COMMIT_TIMESTAMP;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForVersion;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.FIELD_NOT_PRESENT;
import static org.neo4j.kernel.impl.storemigration.FileOperation.COPY;
import static org.neo4j.kernel.impl.storemigration.FileOperation.DELETE;
import static org.neo4j.kernel.impl.storemigration.FileOperation.MOVE;
import static org.neo4j.kernel.impl.storemigration.IdGeneratorMigrator.requiresIdFilesMigration;
import static org.neo4j.kernel.impl.storemigration.StoreMigratorFileOperation.fileOperation;
import static org.neo4j.storageengine.api.LogVersionRepository.BASE_TX_LOG_BYTE_OFFSET;
import static org.neo4j.storageengine.api.LogVersionRepository.BASE_TX_LOG_VERSION;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_COMMIT_TIMESTAMP;

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
    private final JobScheduler jobScheduler;
    private final PageCacheTracer cacheTracer;
    private final BatchImporterFactory batchImporterFactory;
    private final MemoryTracker memoryTracker;

    public RecordStorageMigrator( FileSystemAbstraction fileSystem, PageCache pageCache, Config config,
            LogService logService, JobScheduler jobScheduler, PageCacheTracer cacheTracer,
            BatchImporterFactory batchImporterFactory, MemoryTracker memoryTracker )
    {
        super( "Store files" );
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.config = config;
        this.logService = logService;
        this.jobScheduler = jobScheduler;
        this.cacheTracer = cacheTracer;
        this.batchImporterFactory = batchImporterFactory;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public void migrate( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, ProgressReporter progressReporter,
            String versionToMigrateFrom, String versionToMigrateTo ) throws IOException, KernelException
    {
        // Extract information about the last transaction from legacy neostore
        Path neoStore = directoryLayout.metadataStore();
        try ( var cursorTracer = cacheTracer.createPageCursorTracer( RECORD_STORAGE_MIGRATION_TAG ) )
        {
            long lastTxId = MetaDataStore.getRecord( pageCache, neoStore, LAST_TRANSACTION_ID, directoryLayout.getDatabaseName(), cursorTracer );
            TransactionId lastTxInfo = extractTransactionIdInformation( neoStore, lastTxId, directoryLayout, cursorTracer );
            LogPosition lastTxLogPosition = extractTransactionLogPosition( neoStore, directoryLayout, lastTxId, cursorTracer );
            // Write the tx checksum to file in migrationStructure, because we need it later when moving files into storeDir
            writeLastTxInformation( migrationLayout, lastTxInfo );
            writeLastTxLogPosition( migrationLayout, lastTxLogPosition );

            if ( versionToMigrateFrom.equals( "vE.H.0" ) )
            {
                // NOTE for 3.0 here is a special case for vE.H.0 "from" record format.
                // Legend has it that 3.0.5 enterprise changed store format without changing store version.
                // This was done to cheat the migrator to avoid doing store migration since the
                // format itself was backwards compatible. Immediately a problem was detected:
                // if a user uses 3.0.5 for a while and then goes back to a previous 3.0.x patch release
                // the db wouldn't recognize it was an incompatible downgrade and start up normally,
                // but read records with scrambled values and pointers, sort of.
                //
                // This condition has two functions:
                //  1. preventing actual store migration between vE.H.0 --> vE.H.0b
                //  2. making vE.H.0b used in any migration where either vE.H.0 or vE.H.0b is the existing format,
                //     this because vE.H.0b is a superset of vE.H.0 and sometimes (for 3.0.5) vE.H.0
                //     actually means vE.H.0b (in later version).
                //
                // In later versions of neo4j there are better mechanics in place so that a non-migration like this
                // can be performed w/o special casing. To not require backporting that functionality
                // this condition is here and should be removed in 3.1.
                versionToMigrateFrom = "vE.H.0b";
            }
            RecordFormats oldFormat = selectForVersion( versionToMigrateFrom );
            RecordFormats newFormat = selectForVersion( versionToMigrateTo );
            boolean requiresDynamicStoreMigration = !newFormat.dynamic().equals( oldFormat.dynamic() );
            boolean requiresPropertyMigration =
                    !newFormat.property().equals( oldFormat.property() ) || requiresDynamicStoreMigration;
            boolean requiresIdFilesMigration = requiresIdFilesMigration( oldFormat, newFormat );
            if ( FormatFamily.isHigherFamilyFormat( newFormat, oldFormat ) ||
                    (FormatFamily.isSameFamily( oldFormat, newFormat ) && isDifferentCapabilities( oldFormat, newFormat )) )
            {
                // Some form of migration is required (a fallback/catch-all option)
                migrateWithBatchImporter( directoryLayout, migrationLayout, lastTxId, lastTxInfo.checksum(), lastTxLogPosition.getLogVersion(),
                        lastTxLogPosition.getByteOffset(), progressReporter, oldFormat, newFormat, requiresDynamicStoreMigration, requiresPropertyMigration );
            }

            // update necessary neostore records
            LogPosition logPosition = readLastTxLogPosition( migrationLayout );
            updateOrAddNeoStoreFieldsAsPartOfMigration( migrationLayout, directoryLayout, versionToMigrateTo, logPosition, requiresIdFilesMigration,
                    cursorTracer );

            if ( requiresSchemaStoreMigration( oldFormat, newFormat ) || requiresPropertyMigration )
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
                List<DatabaseFile> databaseFiles = asList( DatabaseFile.PROPERTY_STORE, DatabaseFile.PROPERTY_ARRAY_STORE, DatabaseFile.PROPERTY_STRING_STORE,
                        DatabaseFile.PROPERTY_KEY_TOKEN_STORE, DatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE, DatabaseFile.LABEL_TOKEN_STORE,
                        DatabaseFile.LABEL_TOKEN_NAMES_STORE, DatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE, DatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE );
                fileOperation( COPY, fileSystem, directoryLayout, migrationLayout, databaseFiles, true, !requiresIdFilesMigration,
                        ExistingTargetStrategy.SKIP );
                migrateSchemaStore( directoryLayout, migrationLayout, oldFormat, newFormat, cursorTracer, memoryTracker );
            }

            if ( requiresCountsStoreMigration( oldFormat, newFormat ) )
            {
                migrateCountsStore( directoryLayout, migrationLayout, oldFormat, cursorTracer, memoryTracker );
            }
        }
    }

    /**
     * Rebuilds the counts store by reading from the store that is being migrated from.
     * Instead of a rebuild this could have been done by reading the old counts store, but since we don't want any of that complex
     * code lingering in the code base a rebuild is cleaner, but will require a longer migration time. Worth it?
     */
    private void migrateCountsStore( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, RecordFormats oldFormat,
            PageCursorTracer cursorTracer, MemoryTracker memoryTracker ) throws IOException
    {
        // Just read from the old store (nodes, relationships, highLabelId, highRelationshipTypeId). This way we don't have to try and figure
        // out which stores, if any, have been migrated to the new format. The counts themselves are equivalent in both the old and the migrated stores.
        StoreFactory oldStoreFactory = createStoreFactory( directoryLayout, oldFormat, new ScanOnOpenReadOnlyIdGeneratorFactory() );
        try ( NeoStores oldStores = oldStoreFactory.openAllNeoStores();
                GBPTreeCountsStore countsStore = new GBPTreeCountsStore( pageCache, migrationLayout.countStore(), fileSystem, immediate(),
                        new CountsComputer( oldStores, pageCache, cacheTracer, directoryLayout, memoryTracker, logService.getInternalLog( getClass() ) ),
                        writable(), cacheTracer, GBPTreeCountsStore.NO_MONITOR, migrationLayout.getDatabaseName() ) )
        {
            countsStore.start( cursorTracer, memoryTracker );
            countsStore.checkpoint( cursorTracer );
        }
    }

    private boolean requiresCountsStoreMigration( RecordFormats oldFormat, RecordFormats newFormat )
    {
        return !oldFormat.hasCapability( RecordStorageCapability.GBPTREE_COUNTS_STORE ) &&
                newFormat.hasCapability( RecordStorageCapability.GBPTREE_COUNTS_STORE );
    }

    private static boolean isDifferentCapabilities( RecordFormats oldFormat, RecordFormats newFormat )
    {
        return !oldFormat.hasCompatibleCapabilities( newFormat, CapabilityType.FORMAT );
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

    TransactionId extractTransactionIdInformation( Path neoStore, long lastTransactionId, DatabaseLayout directoryLayout, PageCursorTracer cursorTracer )
            throws IOException
    {
        String databaseName = directoryLayout.getDatabaseName();
        int checksum = (int) MetaDataStore.getRecord( pageCache, neoStore, LAST_TRANSACTION_CHECKSUM, databaseName, cursorTracer );
        long commitTimestamp = MetaDataStore.getRecord( pageCache, neoStore, LAST_TRANSACTION_COMMIT_TIMESTAMP, databaseName, cursorTracer );
        if ( checksum != FIELD_NOT_PRESENT && commitTimestamp != FIELD_NOT_PRESENT )
        {
            return new TransactionId( lastTransactionId, checksum, commitTimestamp );
        }

        return specificTransactionInformationSupplier( lastTransactionId );
    }

    /**
     * In case if we can't find information about transaction in logs we will create new transaction
     * information record.
     * Those should be used <b>only</b> in case if we do not have any transaction logs available during
     * migration.
     *
     * Logs can be absent in two possible scenarios:
     * <ol>
     *     <li>We do not have any logs since there were not transaction.</li>
     *     <li>Logs are missing.</li>
     * </ol>
     * For both of those cases specific informational records will be produced.
     *
     * @param lastTransactionId last committed transaction id
     * @return supplier of custom id records.
     */
    private static TransactionId specificTransactionInformationSupplier( long lastTransactionId )
    {
        return lastTransactionId == TransactionIdStore.BASE_TX_ID
                                          ? new TransactionId( lastTransactionId, BASE_TX_CHECKSUM, BASE_TX_COMMIT_TIMESTAMP )
                                          : new TransactionId( lastTransactionId, UNKNOWN_TX_CHECKSUM, UNKNOWN_TX_COMMIT_TIMESTAMP );
    }

    LogPosition extractTransactionLogPosition( Path neoStore, DatabaseLayout sourceDirectoryStructure, long lastTxId,
            PageCursorTracer cursorTracer ) throws IOException
    {
        String databaseName = sourceDirectoryStructure.getDatabaseName();
        long lastClosedTxLogVersion = MetaDataStore.getRecord( pageCache, neoStore, LAST_CLOSED_TRANSACTION_LOG_VERSION, databaseName, cursorTracer );
        long lastClosedTxLogByteOffset = MetaDataStore.getRecord( pageCache, neoStore, LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET, databaseName, cursorTracer );
        if ( lastClosedTxLogVersion != MetaDataRecordFormat.FIELD_NOT_PRESENT &&
             lastClosedTxLogByteOffset != MetaDataRecordFormat.FIELD_NOT_PRESENT )
        {
            return new LogPosition( lastClosedTxLogVersion, lastClosedTxLogByteOffset );
        }

        // The legacy store we're migrating doesn't have this record in neostore so try to extract it from tx log
        if ( lastTxId == TransactionIdStore.BASE_TX_ID )
        {
            return new LogPosition( BASE_TX_LOG_VERSION, BASE_TX_LOG_BYTE_OFFSET );
        }

        TransactionLogFilesHelper logFiles = new TransactionLogFilesHelper( fileSystem, sourceDirectoryStructure.getTransactionLogsDirectory() );
        RangeLogVersionVisitor versionVisitor = new RangeLogVersionVisitor();
        logFiles.accept( versionVisitor );
        long logVersion = versionVisitor.getHighestVersion();
        if ( logVersion == RangeLogVersionVisitor.UNKNOWN )
        {
            return new LogPosition( BASE_TX_LOG_VERSION, BASE_TX_LOG_BYTE_OFFSET );
        }
        long offset = fileSystem.getFileSize( versionVisitor.getHighestFile() );
        return new LogPosition( logVersion, offset );
    }

    private void migrateWithBatchImporter( DatabaseLayout sourceDirectoryStructure, DatabaseLayout migrationDirectoryStructure, long lastTxId,
            int lastTxChecksum, long lastTxLogVersion, long lastTxLogByteOffset, ProgressReporter progressReporter, RecordFormats oldFormat,
            RecordFormats newFormat, boolean requiresDynamicStoreMigration, boolean requiresPropertyMigration ) throws IOException
    {
        prepareBatchImportMigration( sourceDirectoryStructure, migrationDirectoryStructure, oldFormat, newFormat );

        try ( NeoStores legacyStore = instantiateLegacyStore( oldFormat, sourceDirectoryStructure ) )
        {
            Configuration importConfig = new Configuration.Overridden( defaultConfiguration( sourceDirectoryStructure.databaseDirectory() ), config );
            AdditionalInitialIds additionalInitialIds =
                    readAdditionalIds( lastTxId, lastTxChecksum, lastTxLogVersion, lastTxLogByteOffset );

            // We have to make sure to keep the token ids if we're migrating properties/labels
            BatchImporter importer = batchImporterFactory.instantiate(
                    migrationDirectoryStructure, fileSystem, cacheTracer, importConfig, logService,
                    migrationBatchImporterMonitor( legacyStore, progressReporter,
                            importConfig ), additionalInitialIds, config, newFormat, ImportLogic.NO_MONITOR, jobScheduler, Collector.STRICT,
                    LogFilesInitializer.NULL, memoryTracker );
            InputIterable nodes = () -> legacyNodesAsInput( legacyStore, requiresPropertyMigration, cacheTracer, memoryTracker );
            InputIterable relationships = () -> legacyRelationshipsAsInput( legacyStore, requiresPropertyMigration, cacheTracer, memoryTracker );
            long propertyStoreSize = storeSize( legacyStore.getPropertyStore() ) / 2 +
                storeSize( legacyStore.getPropertyStore().getStringStore() ) / 2 +
                storeSize( legacyStore.getPropertyStore().getArrayStore() ) / 2;
            Estimates estimates = Input.knownEstimates(
                    legacyStore.getNodeStore().getNumberOfIdsInUse(),
                    legacyStore.getRelationshipStore().getNumberOfIdsInUse(),
                    legacyStore.getPropertyStore().getNumberOfIdsInUse(),
                    legacyStore.getPropertyStore().getNumberOfIdsInUse(),
                    propertyStoreSize / 2, propertyStoreSize / 2,
                    0 /*node labels left as 0 for now*/);
            importer.doImport( Input.input( nodes, relationships, IdType.ACTUAL, estimates, ReadableGroups.EMPTY ) );

            // During migration the batch importer doesn't necessarily writes all entities, depending on
            // which stores needs migration. Node, relationship, relationship group stores are always written
            // anyways and cannot be avoided with the importer, but delete the store files that weren't written
            // (left empty) so that we don't overwrite those in the real store directory later.
            Collection<DatabaseFile> storesToDeleteFromMigratedDirectory = new ArrayList<>();
            storesToDeleteFromMigratedDirectory.add( DatabaseFile.METADATA_STORE );
            if ( !requiresPropertyMigration )
            {
                // We didn't migrate properties, so the property stores in the migrated store are just empty/bogus
                storesToDeleteFromMigratedDirectory.addAll( asList(
                        DatabaseFile.PROPERTY_STORE,
                        DatabaseFile.PROPERTY_STRING_STORE,
                        DatabaseFile.PROPERTY_ARRAY_STORE ) );
            }
            if ( !requiresDynamicStoreMigration )
            {
                // We didn't migrate labels (dynamic node labels) or any other dynamic store.
                storesToDeleteFromMigratedDirectory.addAll( asList(
                        DatabaseFile.NODE_LABEL_STORE,
                        DatabaseFile.LABEL_TOKEN_STORE,
                        DatabaseFile.LABEL_TOKEN_NAMES_STORE,
                        DatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE,
                        DatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE,
                        DatabaseFile.PROPERTY_KEY_TOKEN_STORE,
                        DatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                        DatabaseFile.SCHEMA_STORE ) );
            }

            fileOperation( DELETE, fileSystem, migrationDirectoryStructure, migrationDirectoryStructure, storesToDeleteFromMigratedDirectory, true, true,
                    null );
        }
    }

    private static long storeSize( CommonAbstractStore<? extends AbstractBaseRecord,? extends StoreHeader> store )
    {
        return store.getNumberOfIdsInUse() * store.getRecordSize();
    }

    private NeoStores instantiateLegacyStore( RecordFormats format, DatabaseLayout directoryStructure )
    {
        return new StoreFactory( directoryStructure, config, new ScanOnOpenReadOnlyIdGeneratorFactory(), pageCache, fileSystem,
                format, NullLogProvider.getInstance(), cacheTracer, readOnly(), Sets.immutable.empty() ).openAllNeoStores( true );
    }

    private void prepareBatchImportMigration( DatabaseLayout sourceDirectoryStructure, DatabaseLayout migrationStrcuture, RecordFormats oldFormat,
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
        DatabaseFile[] storesFilesToMigrate = {
                DatabaseFile.LABEL_TOKEN_STORE, DatabaseFile.LABEL_TOKEN_NAMES_STORE,
                DatabaseFile.PROPERTY_KEY_TOKEN_STORE, DatabaseFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                DatabaseFile.RELATIONSHIP_TYPE_TOKEN_STORE, DatabaseFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE,
                DatabaseFile.NODE_LABEL_STORE};
        if ( newFormat.dynamic().equals( oldFormat.dynamic() ) )
        {
            fileOperation( COPY, fileSystem, sourceDirectoryStructure, migrationStrcuture,
                    Arrays.asList( storesFilesToMigrate ), true, !requiresIdFilesMigration( oldFormat, newFormat ), ExistingTargetStrategy.OVERWRITE );
        }
        else
        {
            // Migrate all token stores and dynamic node label ids, keeping their ids intact
            DirectRecordStoreMigrator migrator = new DirectRecordStoreMigrator( pageCache, fileSystem, config, cacheTracer );

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

    private void createStore( DatabaseLayout migrationDirectoryStructure, RecordFormats newFormat )
    {
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate(), migrationDirectoryStructure.getDatabaseName() );
        createStoreFactory( migrationDirectoryStructure, newFormat, idGeneratorFactory ).openAllNeoStores( true ).close();
    }

    private StoreFactory createStoreFactory( DatabaseLayout databaseLayout, RecordFormats formats, IdGeneratorFactory idGeneratorFactory )
    {
        return new StoreFactory( databaseLayout, config, idGeneratorFactory, pageCache, fileSystem, formats, NullLogProvider.getInstance(), cacheTracer,
                writable(), immutable.empty() );
    }

    private static AdditionalInitialIds readAdditionalIds( final long lastTxId, final int lastTxChecksum, final long lastTxLogVersion,
            final long lastTxLogByteOffset )
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
        };
    }

    private static ExecutionMonitor migrationBatchImporterMonitor( NeoStores legacyStore, final ProgressReporter progressReporter, Configuration config )
    {
        return new BatchImporterProgressMonitor(
                legacyStore.getNodeStore().getHighId(), legacyStore.getRelationshipStore().getHighId(),
                config, progressReporter );
    }

    private static InputIterator legacyRelationshipsAsInput( NeoStores legacyStore, boolean requiresPropertyMigration, PageCacheTracer cacheTracer,
            MemoryTracker memoryTracker )
    {
        return new StoreScanAsInputIterator<>( legacyStore.getRelationshipStore() )
        {
            @Override
            public InputChunk newChunk()
            {
                var cursorTracer = cacheTracer.createPageCursorTracer( RELATIONSHIP_CHUNK_MIGRATION_TAG );
                return new RelationshipRecordChunk( new RecordStorageReader( legacyStore ), requiresPropertyMigration, cursorTracer, memoryTracker );
            }
        };
    }

    private static InputIterator legacyNodesAsInput( NeoStores legacyStore, boolean requiresPropertyMigration, PageCacheTracer cacheTracer,
            MemoryTracker memoryTracker )
    {
        return new StoreScanAsInputIterator<>( legacyStore.getNodeStore() )
        {
            @Override
            public InputChunk newChunk()
            {
                var cursorTracer = cacheTracer.createPageCursorTracer( NODE_CHUNK_MIGRATION_TAG );
                return new NodeRecordChunk( new RecordStorageReader( legacyStore ), requiresPropertyMigration, cursorTracer, memoryTracker );
            }
        };
    }

    @Override
    public void moveMigratedFiles( DatabaseLayout migrationLayout, DatabaseLayout directoryLayout, String versionToUpgradeFrom,
            String versionToUpgradeTo ) throws IOException
    {
        // Move the migrated ones into the store directory
        fileOperation( MOVE, fileSystem, migrationLayout, directoryLayout,
                Iterables.iterable( DatabaseFile.values() ), true, // allow to skip non existent source files
                true, ExistingTargetStrategy.OVERWRITE );
        RecordFormats oldFormat = selectForVersion( versionToUpgradeFrom );
        RecordFormats newFormat = selectForVersion( versionToUpgradeTo );
        if ( requiresCountsStoreMigration( oldFormat, newFormat ) )
        {
            // Delete the old counts store
            fileSystem.deleteFile( directoryLayout.databaseDirectory().resolve( "neostore.counts.db.a" ) );
            fileSystem.deleteFile( directoryLayout.databaseDirectory().resolve( "neostore.counts.db.b" ) );
        }
    }

    private void updateOrAddNeoStoreFieldsAsPartOfMigration( DatabaseLayout migrationStructure, DatabaseLayout sourceDirectoryStructure,
            String versionToMigrateTo, LogPosition lastClosedTxLogPosition, boolean requiresIdFilesMigration, PageCursorTracer cursorTracer ) throws IOException
    {
        final Path storeDirNeoStore = sourceDirectoryStructure.metadataStore();
        final Path migrationDirNeoStore = migrationStructure.metadataStore();
        String databaseName = sourceDirectoryStructure.getDatabaseName();
        fileOperation( COPY, fileSystem, sourceDirectoryStructure, migrationStructure, Iterables.iterable( DatabaseFile.METADATA_STORE ), true,
                !requiresIdFilesMigration, ExistingTargetStrategy.SKIP );

        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, UPGRADE_TRANSACTION_ID,
                MetaDataStore.getRecord( pageCache, storeDirNeoStore, LAST_TRANSACTION_ID, databaseName, cursorTracer ), databaseName, cursorTracer );
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, UPGRADE_TIME, System.currentTimeMillis(), databaseName, cursorTracer );

        // Store the checksum of the transaction id the upgrade is at right now. Store it both as
        // LAST_TRANSACTION_CHECKSUM and UPGRADE_TRANSACTION_CHECKSUM. Initially the last transaction and the
        // upgrade transaction will be the same, but imagine this scenario:
        //  - legacy store is migrated on instance A at transaction T
        //  - upgraded store is copied, via backup or whatever to instance B
        //  - instance A performs a transaction
        //  - instance B would like to communicate with A where B's last transaction checksum
        //    is verified on A. A, at this point not having logs from pre-migration era, will need to
        //    know the checksum of transaction T to accommodate for this request from B. A will be able
        //    to look up checksums for transactions succeeding T by looking at its transaction logs,
        //    but T needs to be stored in neostore to be accessible. Obviously this scenario is only
        //    problematic as long as we don't migrate and translate old logs.
        TransactionId lastTxInfo = readLastTxInformation( migrationStructure );

        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, LAST_TRANSACTION_CHECKSUM, lastTxInfo.checksum(), databaseName, cursorTracer );
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, UPGRADE_TRANSACTION_CHECKSUM, lastTxInfo.checksum(), databaseName, cursorTracer );
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, LAST_TRANSACTION_COMMIT_TIMESTAMP, lastTxInfo.commitTimestamp(), databaseName, cursorTracer );
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, UPGRADE_TRANSACTION_COMMIT_TIMESTAMP, lastTxInfo.commitTimestamp(), databaseName,
                cursorTracer );

        // add LAST_CLOSED_TRANSACTION_LOG_VERSION and LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET to the migrated
        // NeoStore
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, LAST_CLOSED_TRANSACTION_LOG_VERSION,
                lastClosedTxLogPosition.getLogVersion(), databaseName, cursorTracer );
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET,
                lastClosedTxLogPosition.getByteOffset(), databaseName, cursorTracer );

        // Upgrade version in NeoStore
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, STORE_VERSION, MetaDataStore.versionStringToLong( versionToMigrateTo ), databaseName,
                cursorTracer );
        if ( MetaDataStore.getRecord( pageCache, sourceDirectoryStructure.metadataStore(), KERNEL_VERSION, databaseName, cursorTracer ) == FIELD_NOT_PRESENT )
        {
            MetaDataStore.setRecord( pageCache, migrationDirNeoStore, KERNEL_VERSION, KernelVersion.V4_2.version(), databaseName, cursorTracer );
        }
    }

    private boolean requiresSchemaStoreMigration( RecordFormats oldFormat, RecordFormats newFormat )
    {
        return oldFormat.hasCapability( RecordStorageCapability.FLEXIBLE_SCHEMA_STORE ) !=
                newFormat.hasCapability( RecordStorageCapability.FLEXIBLE_SCHEMA_STORE );
    }

    /**
     * Migration of the schema store is invoked if the old and new formats differ in their {@link RecordStorageCapability#FLEXIBLE_SCHEMA_STORE} capability.
     */
    private void migrateSchemaStore( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, RecordFormats oldFormat, RecordFormats newFormat,
            PageCursorTracer cursorTracer, MemoryTracker memoryTracker ) throws IOException, KernelException
    {
        IdGeneratorFactory srcIdGeneratorFactory = new ScanOnOpenReadOnlyIdGeneratorFactory();
        StoreFactory srcFactory = createStoreFactory( directoryLayout, oldFormat, srcIdGeneratorFactory );
        StoreFactory dstFactory =
                createStoreFactory( migrationLayout, newFormat, new ScanOnOpenOverwritingIdGeneratorFactory( fileSystem, migrationLayout.getDatabaseName() ) );

        if ( newFormat.hasCapability( RecordStorageCapability.FLEXIBLE_SCHEMA_STORE ) )
        {
            SchemaStorageCreator schemaStorageCreator = oldFormat.hasCapability( RecordStorageCapability.FLEXIBLE_SCHEMA_STORE ) ?
                                                        schemaStorageCreatorFlexible() :
                                                        schemaStorageCreator35( directoryLayout, oldFormat, srcIdGeneratorFactory );
            // Token stores
            StoreType[] sourceStoresToOpen = new StoreType[]{
                    StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY_KEY_TOKEN_NAME,
                    StoreType.LABEL_TOKEN, StoreType.LABEL_TOKEN_NAME,
                    StoreType.RELATIONSHIP_TYPE_TOKEN, StoreType.RELATIONSHIP_TYPE_TOKEN_NAME};
            sourceStoresToOpen = ArrayUtil.concat( sourceStoresToOpen, schemaStorageCreator.additionalStoresToOpen() );
            try ( NeoStores srcStore = srcFactory.openNeoStores( sourceStoresToOpen );
                  NeoStores dstStore = dstFactory.openNeoStores( true, StoreType.SCHEMA, StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY );
                  schemaStorageCreator )
            {
                dstStore.start( cursorTracer );
                TokenHolders srcTokenHolders = new TokenHolders(
                        StoreTokens.createReadOnlyTokenHolder( TokenHolder.TYPE_PROPERTY_KEY ),
                        StoreTokens.createReadOnlyTokenHolder( TokenHolder.TYPE_LABEL ),
                        StoreTokens.createReadOnlyTokenHolder( TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
                srcTokenHolders.setInitialTokens( allTokens( srcStore ), cursorTracer );
                SchemaStorage srcAccess = schemaStorageCreator.create( srcStore, srcTokenHolders, cursorTracer );

                SchemaRuleMigrationAccess dstAccess = RecordStorageEngineFactory.createMigrationTargetSchemaRuleAccess( dstStore, cursorTracer, memoryTracker );

                migrateSchemaRules( srcTokenHolders, srcAccess, dstAccess, cursorTracer );

                dstStore.flush( cursorTracer );
            }
        }
    }

    static void migrateSchemaRules( TokenHolders srcTokenHolders, SchemaStorage srcAccess, SchemaRuleMigrationAccess dstAccess,
            PageCursorTracer cursorTracer ) throws KernelException
    {
        LinkedHashMap<Long,SchemaRule> rules = new LinkedHashMap<>();

        schemaGenerateNames( srcAccess, srcTokenHolders, rules, cursorTracer );

        // Once all rules have been processed, write them out.
        for ( SchemaRule rule : rules.values() )
        {
            dstAccess.writeSchemaRule( rule );
        }
    }

    public static void schemaGenerateNames( SchemaStorage srcAccess, TokenHolders srcTokenHolders,
            Map<Long,SchemaRule> rules, PageCursorTracer cursorTracer ) throws KernelException
    {
        SchemaNameGiver nameGiver = new SchemaNameGiver( srcTokenHolders );
        List<SchemaRule> namedRules = new ArrayList<>();
        List<SchemaRule> unnamedRules = new ArrayList<>();
        srcAccess.getAll( cursorTracer ).forEach( r -> (hasName( r ) ? namedRules : unnamedRules).add( r ) );
        // Make sure that we process explicitly named schemas first.
        namedRules.forEach( r -> rules.put( r.getId(), r ) );
        unnamedRules.forEach( r -> rules.put( r.getId(), r ) );

        for ( Map.Entry<Long,SchemaRule> entry : rules.entrySet() )
        {
            SchemaRule rule = entry.getValue();

            if ( rule instanceof IndexDescriptor )
            {
                IndexDescriptor index = (IndexDescriptor) rule;
                OptionalLong owningConstraintId = index.getOwningConstraintId();
                if ( owningConstraintId.isPresent() && rules.containsKey( owningConstraintId.getAsLong() ) )
                {
                    // Indexes that are owned by constraints needs to be named after their constraints.
                    ConstraintDescriptor constraint = (ConstraintDescriptor) rules.get( owningConstraintId.getAsLong() );
                    constraint = nameGiver.ensureHasUniqueName( constraint );
                    rules.put( constraint.getId(), constraint );
                    index = index.withName( constraint.getName() );
                }
                else
                {
                    index = nameGiver.ensureHasUniqueName( index );
                }
                entry.setValue( index );
            }
            else
            {
                ConstraintDescriptor constraint = (ConstraintDescriptor) rule;
                constraint = nameGiver.ensureHasUniqueName( constraint );
                entry.setValue( constraint );
                if ( constraint.isIndexBackedConstraint() )
                {
                    IndexBackedConstraintDescriptor ibc = constraint.asIndexBackedConstraint();
                    if ( ibc.hasOwnedIndexId() )
                    {
                        IndexDescriptor index = (IndexDescriptor) rules.get( ibc.ownedIndexId() );
                        rules.put( index.getId(), index.withName( constraint.getName() ) );
                    }
                }
            }
        }
    }

    private static boolean hasName( SchemaRule rule )
    {
        String name = rule.getName();
        return name != null && !name.startsWith( "index_" ) && !name.startsWith( "constraint_" );
    }

    private static final class SchemaNameGiver
    {
        private final Map<String, SchemaRule> takenNames = new HashMap<>();
        private final TokenHolders tokens;

        private SchemaNameGiver( TokenHolders tokens )
        {
            this.tokens = tokens;
        }

        @SuppressWarnings( "unchecked" )
        private <T extends SchemaRule> T ensureHasUniqueName( T rule ) throws KernelException
        {
            String name = rule.getName();
            if ( name != null && takenNames.get( name ) == rule )
            {
                return rule;
            }
            if ( !hasName( rule ) )
            {
                String[] entityTokenNames = getEntityTokenNames( tokens, rule );
                String[] propertyTokenNames = getPropertyTokenNames( tokens, rule );
                name = SchemaRule.generateName( rule, entityTokenNames, propertyTokenNames );
            }
            int count = 0;
            String originalName = name;
            while ( takenNames.containsKey( name ) )
            {
                count++;
                name = originalName + "_" + count;
            }
            rule = (T) rule.withName( name );
            takenNames.put( name, rule );
            return rule;
        }
    }

    private static String[] getEntityTokenNames( TokenHolders tokenHolders, SchemaRule rule ) throws KernelException
    {
        SchemaDescriptor schema = rule.schema();
        int[] entityTokenIds = schema.getEntityTokenIds();
        String[] entityTokenNames = new String[entityTokenIds.length];
        TokenHolder tokenHolder = schema.entityType() == EntityType.NODE ? tokenHolders.labelTokens() : tokenHolders.relationshipTypeTokens();
        for ( int i = 0; i < entityTokenIds.length; i++ )
        {
            try
            {
                entityTokenNames[i] = tokenHolder.getTokenById( entityTokenIds[i] ).name();
            }
            catch ( TokenNotFoundException e )
            {
                if ( schema.entityType() == EntityType.NODE )
                {
                    throw new LabelNotFoundKernelException( entityTokenIds[i], e );
                }
                throw new RelationshipTypeIdNotFoundKernelException( entityTokenIds[i], e );
            }
        }
        return entityTokenNames;
    }

    private static String[] getPropertyTokenNames( TokenHolders tokenHolders, SchemaRule rule ) throws KernelException
    {
        SchemaDescriptor schema = rule.schema();
        int[] propertyIds = schema.getPropertyIds();
        String[] propertyNames = new String[propertyIds.length];
        TokenHolder tokenHolder = tokenHolders.propertyKeyTokens();
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            try
            {
                propertyNames[i] = tokenHolder.getTokenById( propertyIds[i] ).name();
            }
            catch ( TokenNotFoundException e )
            {
                throw new PropertyKeyIdNotFoundKernelException( propertyIds[i], e );
            }
        }
        return propertyNames;
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

    private SchemaStorageCreator schemaStorageCreatorFlexible()
    {
        return new SchemaStorageCreator()
        {
            private SchemaStore schemaStore;

            @Override
            public SchemaStorage create( NeoStores store, TokenHolders tokenHolders, PageCursorTracer cursorTracer )
            {
                schemaStore = store.getSchemaStore();
                return new org.neo4j.internal.recordstorage.SchemaStorage( schemaStore, tokenHolders );
            }

            @Override
            public StoreType[] additionalStoresToOpen()
            {
                // We need NeoStores to have those stores open so that we can get schema store out in create method.
                return new StoreType[]{StoreType.PROPERTY, StoreType.PROPERTY_STRING, StoreType.PROPERTY_ARRAY, StoreType.SCHEMA};
            }

            @Override
            public void close() throws IOException
            {
                IOUtils.closeAll( schemaStore );
            }
        };
    }

    private SchemaStorageCreator schemaStorageCreator35( DatabaseLayout directoryLayout, RecordFormats oldFormat,
            IdGeneratorFactory srcIdGeneratorFactory )
    {
        return new SchemaStorageCreator()
        {
            SchemaStore35 srcSchema;

            @Override
            public SchemaStorage create( NeoStores store, TokenHolders tokenHolders, PageCursorTracer cursorTracer )
            {
                srcSchema = new SchemaStore35(
                        directoryLayout.schemaStore(),
                        directoryLayout.idSchemaStore(),
                        config,
                        org.neo4j.internal.id.IdType.SCHEMA,
                        srcIdGeneratorFactory,
                        pageCache,
                        NullLogProvider.getInstance(),
                        oldFormat,
                        readOnly(),
                        directoryLayout.getDatabaseName(),
                        immutable.empty() );
                srcSchema.initialise( true, cursorTracer );
                return new SchemaStorage35( srcSchema );
            }

            @Override
            public StoreType[] additionalStoresToOpen()
            {
                return new StoreType[0];
            }

            @Override
            public void close() throws IOException
            {
                IOUtils.closeAll( srcSchema );
            }
        };
    }

    private interface SchemaStorageCreator extends Closeable
    {
        SchemaStorage create( NeoStores store, TokenHolders tokenHolders, PageCursorTracer cursorTracer );

        StoreType[] additionalStoresToOpen();
    }

    private static class NodeRecordChunk extends StoreScanChunk<RecordNodeCursor>
    {
        NodeRecordChunk( RecordStorageReader storageReader, boolean requiresPropertyMigration, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
        {
            super( storageReader.allocateNodeCursor( cursorTracer ), storageReader, requiresPropertyMigration, cursorTracer, memoryTracker );
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
        RelationshipRecordChunk( RecordStorageReader storageReader, boolean requiresPropertyMigration, PageCursorTracer cursorTracer,
                MemoryTracker memoryTracker )
        {
            super( storageReader.allocateRelationshipScanCursor( cursorTracer ), storageReader, requiresPropertyMigration, cursorTracer, memoryTracker );
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
