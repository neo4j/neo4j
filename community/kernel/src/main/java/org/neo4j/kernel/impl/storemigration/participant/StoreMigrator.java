/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import org.neo4j.io.fs.FileHandle;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreFile;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.store.format.CapabilityType;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.ReadOnlyIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.storemigration.DirectRecordStoreMigrator;
import org.neo4j.kernel.impl.storemigration.ExistingTargetStrategy;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.util.CustomIOConfigValidator;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.kernel.impl.util.monitoring.SilentProgressReporter;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.BatchImporterFactory;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.Collectors;
import org.neo4j.unsafe.impl.batchimport.input.Input.Estimates;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.unsafe.impl.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;

import static java.util.Arrays.asList;
import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;
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
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.InputIterable.replayable;
import static org.neo4j.unsafe.impl.batchimport.input.Inputs.knownEstimates;
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
    private static final char TX_LOG_COUNTERS_SEPARATOR = 'A';
    public static final String CUSTOM_IO_EXCEPTION_MESSAGE =
            "Migrating this version is not supported for custom IO configurations.";

    private final Config config;
    private final LogService logService;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;

    public StoreMigrator( FileSystemAbstraction fileSystem, PageCache pageCache, Config config,
            LogService logService )
    {
        super( "Store files" );
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.config = config;
        this.logService = logService;
    }

    @Override
    public void migrate( File storeDir, File migrationDir, ProgressReporter progressReporter,
            String versionToMigrateFrom, String versionToMigrateTo ) throws IOException
    {
        if ( versionToMigrateFrom.equals( StandardV2_3.STORE_VERSION ) )
        {
            // These versions are not supported for block devices.
            CustomIOConfigValidator.assertCustomIOConfigNotUsed( config, CUSTOM_IO_EXCEPTION_MESSAGE );
        }
        // Extract information about the last transaction from legacy neostore
        File neoStore = new File( storeDir, DEFAULT_NAME );
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
                    lastTxLogPosition.getByteOffset(), progressReporter, oldFormat, newFormat );
        }
        // update necessary neostore records
        LogPosition logPosition = readLastTxLogPosition( migrationDir );
        updateOrAddNeoStoreFieldsAsPartOfMigration( migrationDir, storeDir, versionToMigrateTo, logPosition );
    }

    private boolean isDifferentCapabilities( RecordFormats oldFormat, RecordFormats newFormat )
    {
        return !oldFormat.hasCompatibleCapabilities( newFormat, CapabilityType.FORMAT );
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
    private TransactionId specificTransactionInformationSupplier( long lastTransactionId )
    {
        return lastTransactionId == TransactionIdStore.BASE_TX_ID
                                          ? new TransactionId( lastTransactionId, BASE_TX_CHECKSUM, BASE_TX_COMMIT_TIMESTAMP )
                                          : new TransactionId( lastTransactionId, UNKNOWN_TX_CHECKSUM, UNKNOWN_TX_COMMIT_TIMESTAMP );
    }

    LogPosition extractTransactionLogPosition( File neoStore, File storeDir, long lastTxId ) throws IOException
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

        LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( storeDir,fileSystem, pageCache )
                                           .withConfig( config )
                                           .build();
        long logVersion = logFiles.getHighestLogVersion();
        if ( logVersion == -1 )
        {
            return new LogPosition( BASE_TX_LOG_VERSION, BASE_TX_LOG_BYTE_OFFSET );
        }
        long offset = fileSystem.getFileSize( logFiles.getHighestLogFile() );
        return new LogPosition( logVersion, offset );

    }

    private void migrateWithBatchImporter( File storeDir, File migrationDir, long lastTxId, long lastTxChecksum,
            long lastTxLogVersion, long lastTxLogByteOffset, ProgressReporter progressReporter,
            RecordFormats oldFormat, RecordFormats newFormat )
            throws IOException
    {
        prepareBatchImportMigration( storeDir, migrationDir, oldFormat, newFormat );

        boolean requiresDynamicStoreMigration = !newFormat.dynamic().equals( oldFormat.dynamic() );
        boolean requiresPropertyMigration =
                !newFormat.property().equals( oldFormat.property() ) || requiresDynamicStoreMigration;
        File badFile = new File( storeDir, Configuration.BAD_FILE_NAME );
        try ( NeoStores legacyStore = instantiateLegacyStore( oldFormat, storeDir );
              OutputStream badOutput = new BufferedOutputStream( new FileOutputStream( badFile, false ) ) )
        {
            Configuration importConfig = new Configuration.Overridden( config )
            {
                @Override
                public boolean highIO()
                {
                    return FileUtils.highIODevice( storeDir.toPath(), super.highIO() );
                }
            };
            AdditionalInitialIds additionalInitialIds =
                    readAdditionalIds( lastTxId, lastTxChecksum, lastTxLogVersion, lastTxLogByteOffset );

            // We have to make sure to keep the token ids if we're migrating properties/labels
            BatchImporter importer = BatchImporterFactory.withHighestPriority().instantiate( migrationDir.getAbsoluteFile(),
                    fileSystem, pageCache, importConfig, logService,
                    withDynamicProcessorAssignment( migrationBatchImporterMonitor( legacyStore, progressReporter,
                            importConfig ), importConfig ), additionalInitialIds, config, newFormat, NO_MONITOR );
            InputIterable nodes = replayable( () -> legacyNodesAsInput( legacyStore, requiresPropertyMigration ) );
            InputIterable relationships = replayable( () ->
                    legacyRelationshipsAsInput( legacyStore, requiresPropertyMigration ) );
            long propertyStoreSize = storeSize( legacyStore.getPropertyStore() ) / 2 +
                storeSize( legacyStore.getPropertyStore().getStringStore() ) / 2 +
                storeSize( legacyStore.getPropertyStore().getArrayStore() ) / 2;
            Estimates estimates = knownEstimates(
                    legacyStore.getNodeStore().getNumberOfIdsInUse(),
                    legacyStore.getRelationshipStore().getNumberOfIdsInUse(),
                    legacyStore.getPropertyStore().getNumberOfIdsInUse(),
                    legacyStore.getPropertyStore().getNumberOfIdsInUse(),
                    propertyStoreSize / 2, propertyStoreSize / 2,
                    0 /*node labels left as 0 for now*/);
            importer.doImport(
                    Inputs.input( nodes, relationships, IdMappers.actual(), Collectors.badCollector( badOutput, 0 ), estimates ) );

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
            // When migrating on a block device there might be some files only accessible via the file system
            // provided by the page cache.
            try
            {
                Predicate<FileHandle> fileHandlePredicate = fileHandle -> storesToDeleteFromMigratedDirectory.stream()
                        .anyMatch( storeFile -> storeFile.fileName( StoreFileType.STORE )
                                .equals( fileHandle.getFile().getName() ) );
                pageCache.getCachedFileSystem().streamFilesRecursive( migrationDir ).filter( fileHandlePredicate )
                        .forEach( FileHandle.HANDLE_DELETE );
            }
            catch ( NoSuchFileException e )
            {
                // This means that we had no files only present in the page cache, this is fine.
            }
        }
    }

    private static long storeSize( CommonAbstractStore<? extends AbstractBaseRecord,? extends StoreHeader> store )
    {
        return store.getNumberOfIdsInUse() * store.getRecordSize();
    }

    private NeoStores instantiateLegacyStore( RecordFormats format, File storeDir )
    {
        return new StoreFactory( storeDir, config, new ReadOnlyIdGeneratorFactory(), pageCache, fileSystem,
                format, NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY ).openAllNeoStores( true );
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
                try
                {
                    copyWithPageCache( fromPath, toPath );
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
            ProgressReporter progressReporter = SilentProgressReporter.INSTANCE;

            migrator.migrate( storeDir, oldFormat, migrationDir, newFormat, progressReporter, storesToMigrate, StoreType.NODE );
        }
    }

    private void createStore( File migrationDir, RecordFormats newFormat )
    {
        IdGeneratorFactory idGeneratorFactory = new ReadOnlyIdGeneratorFactory( fileSystem );
        NullLogProvider logProvider = NullLogProvider.getInstance();
        StoreFactory storeFactory = new StoreFactory(
                migrationDir, config, idGeneratorFactory, pageCache, fileSystem, newFormat, logProvider,
                EmptyVersionContextSupplier.EMPTY );
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
            final long lastTxLogVersion, final long lastTxLogByteOffset )
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
            final ProgressReporter progressReporter, Configuration config )
    {
        return new BatchImporterProgressMonitor(
                legacyStore.getNodeStore().getHighId(), legacyStore.getRelationshipStore().getHighId(),
                config, progressReporter );
    }

    private InputIterator legacyRelationshipsAsInput( NeoStores legacyStore, boolean requiresPropertyMigration )
    {
        return new StoreScanAsInputIterator<RelationshipRecord>( legacyStore.getRelationshipStore() )
        {
            @Override
            public InputChunk newChunk()
            {
                return new RelationshipRecordChunk( createCursor(), legacyStore, requiresPropertyMigration );
            }
        };
    }

    private InputIterator legacyNodesAsInput( NeoStores legacyStore, boolean requiresPropertyMigration )
    {
        return new StoreScanAsInputIterator<NodeRecord>( legacyStore.getNodeStore() )
        {
            @Override
            public InputChunk newChunk()
            {
                return new NodeRecordChunk( createCursor(), legacyStore, requiresPropertyMigration );
            }
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
        // Since some of the files might only be accessible through the file system provided by the page cache (i.e.
        // block devices), we also try to move the files with the page cache.
        try
        {
            Iterable<FileHandle> fileHandles = pageCache.getCachedFileSystem()
                    .streamFilesRecursive( migrationDir )::iterator;
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
    }

    private void updateOrAddNeoStoreFieldsAsPartOfMigration( File migrationDir, File storeDir,
            String versionToMigrateTo, LogPosition lastClosedTxLogPosition ) throws IOException
    {
        final File storeDirNeoStore = new File( storeDir, DEFAULT_NAME );
        final File migrationDirNeoStore = new File( migrationDir, DEFAULT_NAME );
        copyWithPageCache( storeDirNeoStore, migrationDirNeoStore );

        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, Position.UPGRADE_TRANSACTION_ID,
                MetaDataStore.getRecord( pageCache, storeDirNeoStore, Position.LAST_TRANSACTION_ID ) );
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, Position.UPGRADE_TIME, System.currentTimeMillis() );

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
        //    but T needs to be stored in neostore to be accessible. Obviously this scenario is only
        //    problematic as long as we don't migrate and translate old logs.
        TransactionId lastTxInfo = readLastTxInformation( migrationDir );

        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, Position.LAST_TRANSACTION_CHECKSUM,
                lastTxInfo.checksum() );
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, Position.UPGRADE_TRANSACTION_CHECKSUM,
                lastTxInfo.checksum() );
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, Position.LAST_TRANSACTION_COMMIT_TIMESTAMP,
                lastTxInfo.commitTimestamp() );
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, Position.UPGRADE_TRANSACTION_COMMIT_TIMESTAMP,
                lastTxInfo.commitTimestamp() );

        // add LAST_CLOSED_TRANSACTION_LOG_VERSION and LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET to the migrated
        // NeoStore
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, Position.LAST_CLOSED_TRANSACTION_LOG_VERSION,
                lastClosedTxLogPosition.getLogVersion() );
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET,
                lastClosedTxLogPosition.getByteOffset() );

        // Upgrade version in NeoStore
        MetaDataStore.setRecord( pageCache, migrationDirNeoStore, Position.STORE_VERSION,
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

    private void copyWithPageCache( File sourceFile, File targetFile ) throws IOException
    {
        // We use the page cache for copying the neostore since it might be on a block device.
        int pageSize = pageCache.pageSize();
        try ( PagedFile fromFile = pageCache.map( sourceFile, pageSize );
              PagedFile toFile = pageCache.map( targetFile, pageSize, StandardOpenOption.CREATE );
              PageCursor fromCursor = fromFile.io( 0L, PagedFile.PF_SHARED_READ_LOCK );
              PageCursor toCursor = toFile.io( 0L, PagedFile.PF_SHARED_WRITE_LOCK ) )
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
    }

    private static class NodeRecordChunk extends StoreScanChunk<NodeRecord>
    {
        NodeRecordChunk( RecordCursor<NodeRecord> recordCursor, NeoStores neoStores, boolean requiresPropertyMigration )
        {
            super( recordCursor, neoStores, requiresPropertyMigration );
        }

        @Override
        protected void visitRecord( NodeRecord record, InputEntityVisitor visitor )
        {
            visitor.id( record.getId() );
            visitor.labelField( record.getLabelField() );
            visitProperties( record, visitor );
        }
    }

    private static class RelationshipRecordChunk extends StoreScanChunk<RelationshipRecord>
    {
        RelationshipRecordChunk( RecordCursor<RelationshipRecord> recordCursor, NeoStores neoStore, boolean requiresPropertyMigration )
        {
            super( recordCursor, neoStore, requiresPropertyMigration );
        }

        @Override
        protected void visitRecord( RelationshipRecord record, InputEntityVisitor visitor )
        {
            visitor.startId( record.getFirstNode() );
            visitor.endId( record.getSecondNode() );
            visitor.type( record.getType() );
            visitProperties( record, visitor );
        }
    }

    private static class BatchImporterProgressMonitor extends CoarseBoundedProgressExecutionMonitor
    {
        private final ProgressReporter progressReporter;

        BatchImporterProgressMonitor( long highNodeId, long highRelationshipId,
                org.neo4j.unsafe.impl.batchimport.Configuration configuration,
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
