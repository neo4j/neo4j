/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.storemigration.participant;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.FileHandle;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.store.StorePropertyCursor;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.format.CapabilityType;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_2;
import org.neo4j.kernel.impl.store.id.ReadOnlyIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.storemigration.DirectRecordStoreMigrator;
import org.neo4j.kernel.impl.storemigration.ExistingTargetStrategy;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.storemigration.StoreMigratorCheckPointer;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogs;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication.PropertyDeduplicator;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.CustomIOConfigValidator;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.Collectors;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.unsafe.impl.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;

import static java.util.Arrays.asList;
import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.store.format.Capability.VERSION_TRAILERS;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForVersion;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.FIELD_NOT_PRESENT;
import static org.neo4j.kernel.impl.storemigration.FileOperation.COPY;
import static org.neo4j.kernel.impl.storemigration.FileOperation.DELETE;
import static org.neo4j.kernel.impl.storemigration.FileOperation.MOVE;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_LOG_BYTE_OFFSET;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_LOG_VERSION;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.UNKNOWN_TX_CHECKSUM;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.UNKNOWN_TX_COMMIT_TIMESTAMP;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors.withDynamicProcessorAssignment;

/**
 * Migrates a neo4j kernel database from one version to the next.
 * <p>
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the migration code is specific for the current upgrade and changes with each store format version.
 * <p>
 * Just one out of many potential participants in a {@link StoreUpgrader migration}.
 *
 * @see StoreUpgrader
 */
public class StoreMigrator extends AbstractStoreMigrationParticipant
{
    // Developers: There is a benchmark, storemigrate-benchmark, that generates large stores and benchmarks
    // the upgrade process. Please utilize that when writing upgrade code to ensure the code is fast enough to
    // complete upgrades in a reasonable time period.

    private static final char TX_LOG_COUNTERS_SEPARATOR = 'A';
    public static final String CUSTOM_IO_EXCEPTION_MESSAGE =
            "Migrating this version is not supported for custom IO configurations.";

    private final Config config;
    private final LogService logService;
    private final LegacyLogs legacyLogs;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final SchemaIndexProvider schemaIndexProvider;

    public StoreMigrator( FileSystemAbstraction fileSystem, PageCache pageCache, Config config,
            LogService logService, SchemaIndexProvider schemaIndexProvider )
    {
        this( fileSystem, pageCache, config, logService, schemaIndexProvider, new LegacyLogs( fileSystem ) );
    }

    public StoreMigrator( FileSystemAbstraction fileSystem, PageCache pageCache, Config config,
            LogService logService, SchemaIndexProvider schemaIndexProvider, LegacyLogs legacyLogs )
    {
        super( "Store files" );
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.config = config;
        this.logService = logService;
        this.schemaIndexProvider = schemaIndexProvider;
        this.legacyLogs = legacyLogs;
    }

    @Override
    public void migrate( File storeDir, File migrationDir, MigrationProgressMonitor.Section progressMonitor,
            String versionToMigrateFrom, String versionToMigrateTo ) throws IOException
    {
        if ( versionToMigrateFrom.equals( StandardV2_0.STORE_VERSION ) ||
             versionToMigrateFrom.equals( StandardV2_1.STORE_VERSION ) ||
             versionToMigrateFrom.equals( StandardV2_2.STORE_VERSION ) )
        {
            // These versions are not supported for block devices.
            CustomIOConfigValidator.assertCustomIOConfigNotUsed( config, CUSTOM_IO_EXCEPTION_MESSAGE );
        }
        // Extract information about the last transaction from legacy neostore
        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        long lastTxId = MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_ID );
        TransactionId lastTxInfo = extractTransactionIdInformation( neoStore, storeDir, lastTxId );
        LogPosition lastTxLogPosition = extractTransactionLogPosition( neoStore, storeDir, lastTxId );
        // Write the tx checksum to file in migrationDir, because we need it later when moving files into storeDir
        writeLastTxInformation( migrationDir, lastTxInfo );
        writeLastTxLogPosition( migrationDir, lastTxLogPosition );

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
        if ( FormatFamily.isHigherFamilyFormat( newFormat, oldFormat ) ||
             (FormatFamily.isSameFamily( oldFormat, newFormat ) && isDifferentCapabilities( oldFormat, newFormat )) )
        {
            // TODO if this store has relationship indexes then warn user about that they will be incorrect
            // after migration, because now we're rewriting the relationship ids.

            // Some form of migration is required (a fallback/catch-all option)
            migrateWithBatchImporter( storeDir, migrationDir,
                    lastTxId, lastTxInfo.checksum(), lastTxLogPosition.getLogVersion(),
                    lastTxLogPosition.getByteOffset(), progressMonitor, oldFormat, newFormat );
        }

        if ( versionToMigrateFrom.equals( StandardV2_1.STORE_VERSION ) )
        {
            removeDuplicateEntityProperties( storeDir, migrationDir, pageCache, schemaIndexProvider, oldFormat );
        }

        // DO NOT migrate logs. LegacyLogs is able to migrate logs, but only changes its format, not any
        // contents of it, and since the record format has changed there would be a mismatch between the
        // commands in the log and the contents in the store. If log migration is to be performed there
        // must be a proper translation happening while doing so.
    }

    private boolean isDifferentCapabilities( RecordFormats oldFormat, RecordFormats newFormat )
    {
        return !oldFormat.hasSameCapabilities( newFormat, CapabilityType.FORMAT );
    }

    void writeLastTxInformation( File migrationDir, TransactionId txInfo ) throws IOException
    {
        writeTxLogCounters( fileSystem, lastTxInformationFile( migrationDir ),
                txInfo.transactionId(), txInfo.checksum(), txInfo.commitTimestamp() );
    }

    void writeLastTxLogPosition( File migrationDir, LogPosition lastTxLogPosition ) throws IOException
    {
        writeTxLogCounters( fileSystem, lastTxLogPositionFile( migrationDir ),
                lastTxLogPosition.getLogVersion(), lastTxLogPosition.getByteOffset() );
    }

    TransactionId readLastTxInformation( File migrationDir ) throws IOException
    {
        long[] counters = readTxLogCounters( fileSystem, lastTxInformationFile( migrationDir ), 3 );
        return new TransactionId( counters[0], counters[1], counters[2] );
    }

    LogPosition readLastTxLogPosition( File migrationDir ) throws IOException
    {
        long[] counters = readTxLogCounters( fileSystem, lastTxLogPositionFile( migrationDir ), 2 );
        return new LogPosition( counters[0], counters[1] );
    }

    private static void writeTxLogCounters( FileSystemAbstraction fs, File file, long... counters ) throws IOException
    {
        try ( Writer writer = fs.openAsWriter( file, StandardCharsets.UTF_8, false ) )
        {
            writer.write( StringUtils.join( counters, TX_LOG_COUNTERS_SEPARATOR ) );
        }
    }

    private static long[] readTxLogCounters( FileSystemAbstraction fs, File file, int numberOfCounters )
            throws IOException
    {
        try ( BufferedReader reader = new BufferedReader( fs.openAsReader( file, StandardCharsets.UTF_8 ) ) )
        {
            String line = reader.readLine();
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

    private static File lastTxInformationFile( File migrationDir )
    {
        return new File( migrationDir, "lastxinformation" );
    }

    private static File lastTxLogPositionFile( File migrationDir )
    {
        return new File( migrationDir, "lastxlogposition" );
    }

    TransactionId extractTransactionIdInformation( File neoStore, File storeDir, long lastTransactionId )
            throws IOException
    {
        long checksum = MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_CHECKSUM );
        long commitTimestamp = MetaDataStore.getRecord( pageCache, neoStore,
                Position.LAST_TRANSACTION_COMMIT_TIMESTAMP );
        if ( checksum != FIELD_NOT_PRESENT && commitTimestamp != FIELD_NOT_PRESENT )
        {
            return new TransactionId( lastTransactionId, checksum, commitTimestamp );
        }
        // The legacy store we're migrating doesn't have this record in neostore so try to extract it from tx log

        Optional<TransactionId> transactionInformation = legacyLogs.getTransactionInformation( storeDir, lastTransactionId );
        return transactionInformation.orElseGet( specificTransactionInformationSupplier( lastTransactionId ) );
    }

    /**
     * In case if we can't find information about transaction in legacy logs we will create new transaction
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
    private Supplier<TransactionId> specificTransactionInformationSupplier( long lastTransactionId )
    {
        return () -> lastTransactionId == TransactionIdStore.BASE_TX_ID
                                          ? new TransactionId( lastTransactionId, BASE_TX_CHECKSUM, BASE_TX_COMMIT_TIMESTAMP )
                                          : new TransactionId( lastTransactionId, UNKNOWN_TX_CHECKSUM, UNKNOWN_TX_COMMIT_TIMESTAMP );
    }

    private LogPosition extractTransactionLogPosition( File neoStore, File storeDir, long lastTxId ) throws IOException
    {
        long lastClosedTxLogVersion =
                MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_CLOSED_TRANSACTION_LOG_VERSION );
        long lastClosedTxLogByteOffset =
                MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
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

        PhysicalLogFiles logFiles = new PhysicalLogFiles( storeDir, fileSystem );
        long logVersion = logFiles.getHighestLogVersion();
        if ( logVersion == -1 )
        {
            return new LogPosition( BASE_TX_LOG_VERSION, BASE_TX_LOG_BYTE_OFFSET );
        }
        long offset = fileSystem.getFileSize( logFiles.getLogFileForVersion( logVersion ) );
        return new LogPosition( logVersion, offset );

    }

    private void removeDuplicateEntityProperties( File storeDir, File migrationDir, PageCache pageCache,
            SchemaIndexProvider schemaIndexProvider, RecordFormats oldFormat )
            throws IOException
    {
        StoreFile.fileOperation( COPY, fileSystem, storeDir, migrationDir, Iterables.iterable(
                StoreFile.PROPERTY_STORE,
                StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                StoreFile.PROPERTY_KEY_TOKEN_STORE,
                StoreFile.PROPERTY_ARRAY_STORE,
                StoreFile.PROPERTY_STRING_STORE,
                StoreFile.NODE_STORE,
                StoreFile.NODE_LABEL_STORE,
                StoreFile.SCHEMA_STORE ), false, ExistingTargetStrategy.SKIP, StoreFileType.STORE );

        // copy ids only if present
        StoreFile.fileOperation( COPY, fileSystem, storeDir, migrationDir, Iterables.iterable(
                StoreFile.PROPERTY_STORE,
                StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                StoreFile.PROPERTY_KEY_TOKEN_STORE,
                StoreFile.NODE_STORE ), true, ExistingTargetStrategy.SKIP, StoreFileType.ID );

        // let's remove trailers here on the copied files since the new code doesn't remove them since in 2.3
        // there are no store trailers
        StoreFile.removeTrailers( oldFormat.storeVersion(), fileSystem, migrationDir, pageCache.pageSize() );

        new PropertyDeduplicator( fileSystem, migrationDir, pageCache, schemaIndexProvider )
                .deduplicateProperties();
    }

    private void rebuildCountsFromScratch( File storeDir, long lastTxId, PageCache pageCache )
    {
        final File storeFileBase = new File( storeDir, MetaDataStore.DEFAULT_NAME + StoreFactory.COUNTS_STORE );

        StoreFactory storeFactory = new StoreFactory( storeDir, pageCache, fileSystem, NullLogProvider.getInstance() );
        try ( NeoStores neoStores = storeFactory.openAllNeoStores() )
        {
            NodeStore nodeStore = neoStores.getNodeStore();
            RelationshipStore relationshipStore = neoStores.getRelationshipStore();
            try ( Lifespan life = new Lifespan() )
            {
                int highLabelId = (int) neoStores.getLabelTokenStore().getHighId();
                int highRelationshipTypeId = (int) neoStores.getRelationshipTypeTokenStore().getHighId();
                CountsComputer initializer = new CountsComputer(
                        lastTxId, nodeStore, relationshipStore, highLabelId, highRelationshipTypeId );
                life.add( new CountsTracker(
                        logService.getInternalLogProvider(), fileSystem, pageCache, config, storeFileBase )
                        .setInitializer( initializer ) );
            }
        }
    }

    private void migrateWithBatchImporter( File storeDir, File migrationDir, long lastTxId, long lastTxChecksum,
            long lastTxLogVersion, long lastTxLogByteOffset, MigrationProgressMonitor.Section progressMonitor,
            RecordFormats oldFormat, RecordFormats newFormat )
            throws IOException
    {
        prepareBatchImportMigration( storeDir, migrationDir, oldFormat, newFormat );

        boolean requiresDynamicStoreMigration = !newFormat.dynamic().equals( oldFormat.dynamic() );
        boolean requiresPropertyMigration =
                !newFormat.property().equals( oldFormat.property() ) || requiresDynamicStoreMigration;
        File badFile = new File( storeDir, Configuration.BAD_FILE_NAME );
        try ( NeoStores legacyStore = instantiateLegacyStore( oldFormat, storeDir );
                RecordCursors nodeInputCursors = new RecordCursors( legacyStore );
                RecordCursors relationshipInputCursors = new RecordCursors( legacyStore );
                OutputStream badOutput = new BufferedOutputStream( new FileOutputStream( badFile, false ) ) )
        {
            Configuration importConfig = new Configuration.Overridden( config );
            AdditionalInitialIds additionalInitialIds =
                    readAdditionalIds( lastTxId, lastTxChecksum, lastTxLogVersion, lastTxLogByteOffset );

            // We have to make sure to keep the token ids if we're migrating properties/labels
            BatchImporter importer = new ParallelBatchImporter( migrationDir.getAbsoluteFile(), fileSystem, pageCache,
                    importConfig, logService,
                    withDynamicProcessorAssignment( migrationBatchImporterMonitor( legacyStore, progressMonitor,
                            importConfig ), importConfig ), additionalInitialIds, config, newFormat );
            InputIterable<InputNode> nodes =
                    legacyNodesAsInput( legacyStore, requiresPropertyMigration, nodeInputCursors );
            InputIterable<InputRelationship> relationships =
                    legacyRelationshipsAsInput( legacyStore, requiresPropertyMigration, relationshipInputCursors );
            importer.doImport(
                    Inputs.input( nodes, relationships, IdMappers.actual(), IdGenerators.fromInput(),
                            Collectors.badCollector( badOutput, 0 ) ) );

            // During migration the batch importer doesn't necessarily writes all entities, depending on
            // which stores needs migration. Node, relationship, relationship group stores are always written
            // anyways and cannot be avoided with the importer, but delete the store files that weren't written
            // (left empty) so that we don't overwrite those in the real store directory later.
            Collection<StoreFile> storesToDeleteFromMigratedDirectory = new ArrayList<>();
            storesToDeleteFromMigratedDirectory.add( StoreFile.NEO_STORE );
            if ( !requiresPropertyMigration )
            {
                // We didn't migrate properties, so the property stores in the migrated store are just empty/bogus
                storesToDeleteFromMigratedDirectory.addAll( asList(
                        StoreFile.PROPERTY_STORE,
                        StoreFile.PROPERTY_STRING_STORE,
                        StoreFile.PROPERTY_ARRAY_STORE ) );
            }
            if ( !requiresDynamicStoreMigration )
            {
                // We didn't migrate labels (dynamic node labels) or any other dynamic store
                storesToDeleteFromMigratedDirectory.addAll( asList(
                        StoreFile.NODE_LABEL_STORE,
                        StoreFile.LABEL_TOKEN_STORE,
                        StoreFile.LABEL_TOKEN_NAMES_STORE,
                        StoreFile.RELATIONSHIP_TYPE_TOKEN_STORE,
                        StoreFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE,
                        StoreFile.PROPERTY_KEY_TOKEN_STORE,
                        StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                        StoreFile.SCHEMA_STORE ) );
            }
            StoreFile.fileOperation( DELETE, fileSystem, migrationDir, null, storesToDeleteFromMigratedDirectory,
                    true, null, StoreFileType.values() );
            // When migrating on a block device there might be some files only accessible via the page cache.
            try
            {
                Predicate<FileHandle> fileHandlePredicate = fileHandle -> storesToDeleteFromMigratedDirectory.stream()
                        .anyMatch( storeFile -> storeFile.fileName( StoreFileType.STORE )
                                .equals( fileHandle.getFile().getName() ) );
                pageCache.streamFilesRecursive( migrationDir ).filter( fileHandlePredicate )
                        .forEach( FileHandle.HANDLE_DELETE );
            }
            catch ( NoSuchFileException e )
            {
                // This means that we had no files only present in the page cache, this is fine.
            }
        }
    }

    private NeoStores instantiateLegacyStore( RecordFormats format, File storeDir )
    {
        return new StoreFactory( storeDir, config, new ReadOnlyIdGeneratorFactory(), pageCache, fileSystem,
                format, NullLogProvider.getInstance() ).openAllNeoStores( true );
    }

    private void prepareBatchImportMigration( File storeDir, File migrationDir, RecordFormats oldFormat,
            RecordFormats newFormat ) throws IOException
    {
        createStore( migrationDir, newFormat );

        // We use the batch importer for migrating the data, and we use it in a special way where we only
        // rewrite the stores that have actually changed format. We know that to be node and relationship
        // stores. Although since the batch importer also populates the counts store, all labels need to
        // be read, i.e. both inlined and those existing in dynamic records. That's why we need to copy
        // that dynamic record store over before doing the "batch import".
        //   Copying this file just as-is assumes that the format hasn't change. If that happens we're in
        // a different situation, where we first need to migrate this file.

        // The token stores also need to be migrated because we use those as-is and ask for their high ids
        // when using the importer in the store migration scenario.
        StoreFile[] storesFilesToMigrate = {
                StoreFile.LABEL_TOKEN_STORE, StoreFile.LABEL_TOKEN_NAMES_STORE,
                StoreFile.PROPERTY_KEY_TOKEN_STORE, StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                StoreFile.RELATIONSHIP_TYPE_TOKEN_STORE, StoreFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE,
                StoreFile.NODE_LABEL_STORE};
        if ( newFormat.dynamic().equals( oldFormat.dynamic() ) )
        {
            // We use the page cache for copying the STORE files since these might be on a block device.
            for ( StoreFile file : storesFilesToMigrate )
            {
                File fromPath = new File( storeDir, file.fileName( StoreFileType.STORE ) );
                File toPath = new File( migrationDir, file.fileName( StoreFileType.STORE ) );
                int pageSize = pageCache.pageSize();
                try ( PagedFile fromFile = pageCache.map( fromPath, pageSize );
                      PagedFile toFile = pageCache.map( toPath, pageSize, StandardOpenOption.CREATE );
                      PageCursor fromCursor = fromFile.io( 0L, PagedFile.PF_SHARED_READ_LOCK );
                      PageCursor toCursor = toFile.io( 0L, PagedFile.PF_SHARED_WRITE_LOCK ); )
                {
                    while ( fromCursor.next() )
                    {
                        toCursor.next();
                        do
                        {
                            fromCursor.copyTo( 0, toCursor, 0, pageSize );
                        }
                        while ( fromCursor.shouldRetry() );
                    }
                }
                catch ( NoSuchFileException e )
                {
                    // It is okay for the file to not be there.
                }
            }

            // The ID files are to be kept on the normal file system, hence we use fileOperation to copy them.
            StoreFile.fileOperation( COPY, fileSystem, storeDir, migrationDir, Arrays.asList( storesFilesToMigrate ),
                    true, // OK if it's not there (1.9)
                    ExistingTargetStrategy.FAIL, StoreFileType.ID);
        }
        else
        {
            // Migrate all token stores, schema store and dynamic node label ids, keeping their ids intact
            DirectRecordStoreMigrator migrator = new DirectRecordStoreMigrator( pageCache, fileSystem, config );

            StoreType[] storesToMigrate = {
                    StoreType.LABEL_TOKEN, StoreType.LABEL_TOKEN_NAME,
                    StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY_KEY_TOKEN_NAME,
                    StoreType.RELATIONSHIP_TYPE_TOKEN, StoreType.RELATIONSHIP_TYPE_TOKEN_NAME,
                    StoreType.NODE_LABEL,
                    StoreType.SCHEMA};

            // Migrate these stores silently because they are usually very small
            MigrationProgressMonitor.Section section = SilentMigrationProgressMonitor.NO_OP_SECTION;

            migrator.migrate( storeDir, oldFormat, migrationDir, newFormat, section, storesToMigrate, StoreType.NODE );
        }
    }

    private void createStore( File migrationDir, RecordFormats newFormat )
    {
        StoreFactory storeFactory = new StoreFactory( new File( migrationDir.getPath() ), pageCache, fileSystem,
                newFormat, NullLogProvider.getInstance() );
        try ( NeoStores neoStores = storeFactory.openAllNeoStores( true ) )
        {
            neoStores.getMetaDataStore();
            neoStores.getLabelTokenStore();
            neoStores.getNodeStore();
            neoStores.getPropertyStore();
            neoStores.getRelationshipGroupStore();
            neoStores.getRelationshipStore();
            neoStores.getSchemaStore();
        }
    }

    private AdditionalInitialIds readAdditionalIds( final long lastTxId, final long lastTxChecksum,
            final long lastTxLogVersion, final long lastTxLogByteOffset ) throws IOException
    {
        return new AdditionalInitialIds()
        {
            @Override
            public long lastCommittedTransactionId()
            {
                return lastTxId;
            }

            @Override
            public long lastCommittedTransactionChecksum()
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

    private ExecutionMonitor migrationBatchImporterMonitor( NeoStores legacyStore,
            final MigrationProgressMonitor.Section progressMonitor, Configuration config )
    {
        return new BatchImporterProgressMonitor(
                legacyStore.getNodeStore().getHighId(), legacyStore.getRelationshipStore().getHighId(),
                config, progressMonitor );
    }

    private InputIterable<InputRelationship> legacyRelationshipsAsInput( NeoStores legacyStore,
            boolean requiresPropertyMigration, RecordCursors cursors )
    {
        RelationshipStore store = legacyStore.getRelationshipStore();
        final BiConsumer<InputRelationship,RelationshipRecord> propertyDecorator =
                propertyDecorator( requiresPropertyMigration, cursors );
        return new StoreScanAsInputIterable<InputRelationship,RelationshipRecord>( store )
        {
            @Override
            protected InputRelationship inputEntityOf( RelationshipRecord record )
            {
                InputRelationship result = new InputRelationship(
                        "legacy store", record.getId(), record.getId() * RelationshipRecordFormat.RECORD_SIZE,
                        InputEntity.NO_PROPERTIES, record.getNextProp(),
                        record.getFirstNode(), record.getSecondNode(), null, record.getType() );
                propertyDecorator.accept( result, record );
                return result;
            }
        };
    }

    private InputIterable<InputNode> legacyNodesAsInput( NeoStores legacyStore,
            boolean requiresPropertyMigration, RecordCursors cursors )
    {
        NodeStore store = legacyStore.getNodeStore();
        final BiConsumer<InputNode,NodeRecord> propertyDecorator =
                propertyDecorator( requiresPropertyMigration, cursors );

        return new StoreScanAsInputIterable<InputNode,NodeRecord>( store )
        {
            @Override
            protected InputNode inputEntityOf( NodeRecord record )
            {
                InputNode node = new InputNode(
                        "legacy store", record.getId(), record.getId() * NodeRecordFormat.RECORD_SIZE,
                        record.getId(), InputEntity.NO_PROPERTIES, record.getNextProp(),
                        InputNode.NO_LABELS, record.getLabelField() );
                propertyDecorator.accept( node, record );
                return node;
            }
        };
    }

    private <ENTITY extends InputEntity, RECORD extends PrimitiveRecord> BiConsumer<ENTITY,RECORD> propertyDecorator(
            boolean requiresPropertyMigration, RecordCursors cursors )
    {
        if ( !requiresPropertyMigration )
        {
            return (a, b) -> {};
        }

        final StorePropertyCursor cursor = new StorePropertyCursor( cursors, ignored -> {} );
        final List<Object> scratch = new ArrayList<>();
        return ( ENTITY entity, RECORD record ) ->
        {
            cursor.init( record.getNextProp(), LockService.NO_LOCK );
            scratch.clear();
            while ( cursor.next() )
            {
                scratch.add( cursor.propertyKeyId() ); // add key as int here as to have the importer use the token id
                scratch.add( cursor.value() );
            }
            entity.setProperties( scratch.isEmpty() ? InputEntity.NO_PROPERTIES : scratch.toArray() );
            cursor.close();
        };
    }

    @Override
    public void moveMigratedFiles( File migrationDir, File storeDir, String versionToUpgradeFrom,
            String versionToUpgradeTo ) throws IOException
    {
        // Move the migrated ones into the store directory
        StoreFile.fileOperation( MOVE, fileSystem, migrationDir, storeDir, StoreFile.currentStoreFiles(),
                true, // allow to skip non existent source files
                ExistingTargetStrategy.OVERWRITE, // allow to overwrite target files
                StoreFileType.values() );
        // Since some of the files might only be accessible through the page cache (i.e. block devices), we also try to
        // move the files with the page cache.
        try
        {
            Iterable<FileHandle> fileHandles = pageCache.streamFilesRecursive( migrationDir )::iterator;
            for ( FileHandle fh : fileHandles )
            {
                Predicate<StoreFile> predicate =
                        storeFile -> storeFile.fileName( StoreFileType.STORE ).equals( fh.getFile().getName() );
                if ( StreamSupport.stream( StoreFile.currentStoreFiles().spliterator(), false ).anyMatch( predicate ) )
                {
                    final Optional<PagedFile> optionalPagedFile = pageCache.getExistingMapping( fh.getFile() );
                    if ( optionalPagedFile.isPresent() )
                    {
                        optionalPagedFile.get().close();
                    }
                    fh.rename( new File( storeDir, fh.getFile().getName() ), StandardCopyOption.REPLACE_EXISTING );
                }
            }
        }
        catch ( NoSuchFileException e )
        {
            //This means that we had no files only present in the page cache, this is fine.
        }

        RecordFormats oldFormat = selectForVersion( versionToUpgradeFrom );
        RecordFormats newFormat = selectForVersion( versionToUpgradeTo );
        boolean movingAwayFromVersionTrailers =
                oldFormat.hasCapability( VERSION_TRAILERS ) && !newFormat.hasCapability( VERSION_TRAILERS );

        if ( movingAwayFromVersionTrailers )
        {
            StoreFile.removeTrailers( versionToUpgradeFrom, fileSystem, storeDir, pageCache.pageSize() );
        }

        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        long lastCommittedTx = MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_ID );

        // update necessary neostore records
        LogPosition logPosition = readLastTxLogPosition( migrationDir );
        updateOrAddNeoStoreFieldsAsPartOfMigration( migrationDir, storeDir, versionToUpgradeTo, logPosition );

        // delete old logs
        legacyLogs.deleteUnusedLogFiles( storeDir );

        if ( movingAwayFromVersionTrailers )
        {
            // write a check point in the log in order to make recovery work in the newer version
            new StoreMigratorCheckPointer( storeDir, fileSystem ).checkPoint( logPosition, lastCommittedTx );
        }
    }

    @Override
    public void rebuildCounts( File storeDir, String versionToMigrateFrom, String versionToMigrateTo ) throws
            IOException
    {
        if ( StandardV2_1.STORE_VERSION.equals( versionToMigrateFrom ) ||
             StandardV2_2.STORE_VERSION.equals( versionToMigrateFrom ) )
        {
            // create counters from scratch
            Iterable<StoreFile> countsStoreFiles =
                    Iterables.iterable( StoreFile.COUNTS_STORE_LEFT, StoreFile.COUNTS_STORE_RIGHT );
            StoreFile.fileOperation( DELETE, fileSystem, storeDir, storeDir,
                    countsStoreFiles, true, null, StoreFileType.STORE );
            File neoStore = new File( storeDir, DEFAULT_NAME );
            long lastTxId = MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_ID );
            rebuildCountsFromScratch( storeDir, lastTxId, pageCache );
        }
    }

    private void updateOrAddNeoStoreFieldsAsPartOfMigration( File migrationDir, File storeDir,
            String versionToMigrateTo, LogPosition lastClosedTxLogPosition ) throws IOException
    {
        final File storeDirNeoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.UPGRADE_TRANSACTION_ID,
                MetaDataStore.getRecord( pageCache, storeDirNeoStore, Position.LAST_TRANSACTION_ID ) );
        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.UPGRADE_TIME, System.currentTimeMillis() );

        // Store the checksum of the transaction id the upgrade is at right now. Store it both as
        // LAST_TRANSACTION_CHECKSUM and UPGRADE_TRANSACTION_CHECKSUM. Initially the last transaction and the
        // upgrade transaction will be the same, but imagine this scenario:
        //  - legacy store is migrated on instance A at transaction T
        //  - upgraded store is copied, via backup or HA or whatever to instance B
        //  - instance A performs a transaction
        //  - instance B would like to communicate with A where B's last transaction checksum
        //    is verified on A. A, at this point not having logs from pre-migration era, will need to
        //    know the checksum of transaction T to accommodate for this request from B. A will be able
        //    to look up checksums for transactions succeeding T by looking at its transaction logs,
        //    but T needs to be stored in neostore to be accessible. Obvioously this scenario is only
        //    problematic as long as we don't migrate and translate old logs.
        TransactionId lastTxInfo = readLastTxInformation( migrationDir );

        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.LAST_TRANSACTION_CHECKSUM,
                lastTxInfo.checksum() );
        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.UPGRADE_TRANSACTION_CHECKSUM,
                lastTxInfo.checksum() );
        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.LAST_TRANSACTION_COMMIT_TIMESTAMP,
                lastTxInfo.commitTimestamp() );
        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.UPGRADE_TRANSACTION_COMMIT_TIMESTAMP,
                lastTxInfo.commitTimestamp() );

        // add LAST_CLOSED_TRANSACTION_LOG_VERSION and LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET to the migrated
        // NeoStore
        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.LAST_CLOSED_TRANSACTION_LOG_VERSION,
                lastClosedTxLogPosition.getLogVersion() );
        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET,
                lastClosedTxLogPosition.getByteOffset() );

        // Upgrade version in NeoStore
        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.STORE_VERSION,
                MetaDataStore.versionStringToLong( versionToMigrateTo ) );
    }

    @Override
    public void cleanup( File migrationDir ) throws IOException
    {
        fileSystem.deleteRecursively( migrationDir );
    }

    @Override
    public String toString()
    {
        return "Kernel StoreMigrator";
    }

    private class BatchImporterProgressMonitor extends CoarseBoundedProgressExecutionMonitor
    {
        private final MigrationProgressMonitor.Section progressMonitor;

        BatchImporterProgressMonitor( long highNodeId, long highRelationshipId,
                org.neo4j.unsafe.impl.batchimport.Configuration configuration,
                MigrationProgressMonitor.Section progressMonitor )
        {
            super( highNodeId, highRelationshipId, configuration );
            this.progressMonitor = progressMonitor;
            this.progressMonitor.start( total() );
        }

        @Override
        protected void progress( long progress )
        {
            progressMonitor.progress( progress );
        }
    }
}
