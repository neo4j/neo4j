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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordNodeCursor;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageReader;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.store.format.CapabilityType;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.ReadOnlyIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.storemigration.DirectRecordStoreMigrator;
import org.neo4j.kernel.impl.storemigration.ExistingTargetStrategy;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.kernel.impl.util.monitoring.SilentProgressReporter;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
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
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForVersion;
import static org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat.FIELD_NOT_PRESENT;
import static org.neo4j.kernel.impl.storemigration.FileOperation.COPY;
import static org.neo4j.kernel.impl.storemigration.FileOperation.DELETE;
import static org.neo4j.kernel.impl.storemigration.FileOperation.MOVE;
import static org.neo4j.kernel.impl.storemigration.participant.StoreMigratorFileOperation.fileOperation;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_LOG_BYTE_OFFSET;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_LOG_VERSION;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.UNKNOWN_TX_CHECKSUM;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.UNKNOWN_TX_COMMIT_TIMESTAMP;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;
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

    private final Config config;
    private final LogService logService;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final JobScheduler jobScheduler;

    public StoreMigrator( FileSystemAbstraction fileSystem, PageCache pageCache, Config config,
            LogService logService, JobScheduler jobScheduler )
    {
        super( "Store files" );
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.config = config;
        this.logService = logService;
        this.jobScheduler = jobScheduler;
    }

    @Override
    public void migrate( DatabaseLayout directoryLayout, DatabaseLayout migrationLayout, ProgressReporter progressReporter,
            String versionToMigrateFrom, String versionToMigrateTo ) throws IOException
    {
        // Extract information about the last transaction from legacy neostore
        File neoStore = directoryLayout.metadataStore();
        long lastTxId = MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_ID );
        TransactionId lastTxInfo = extractTransactionIdInformation( neoStore, lastTxId );
        LogPosition lastTxLogPosition = extractTransactionLogPosition( neoStore, directoryLayout, lastTxId );
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
        if ( FormatFamily.isHigherFamilyFormat( newFormat, oldFormat ) ||
             (FormatFamily.isSameFamily( oldFormat, newFormat ) && isDifferentCapabilities( oldFormat, newFormat )) )
        {
            // TODO if this store has relationship indexes then warn user about that they will be incorrect
            // after migration, because now we're rewriting the relationship ids.

            // Some form of migration is required (a fallback/catch-all option)
            migrateWithBatchImporter( directoryLayout, migrationLayout,
                    lastTxId, lastTxInfo.checksum(), lastTxLogPosition.getLogVersion(),
                    lastTxLogPosition.getByteOffset(), progressReporter, oldFormat, newFormat );
        }
        // update necessary neostore records
        LogPosition logPosition = readLastTxLogPosition( migrationLayout );
        updateOrAddNeoStoreFieldsAsPartOfMigration( migrationLayout, directoryLayout, versionToMigrateTo, logPosition );
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
        return new TransactionId( counters[0], counters[1], counters[2] );
    }

    LogPosition readLastTxLogPosition( DatabaseLayout migrationStructure ) throws IOException
    {
        long[] counters = readTxLogCounters( fileSystem, lastTxLogPositionFile( migrationStructure ), 2 );
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

    private static File lastTxInformationFile( DatabaseLayout migrationStructure )
    {
        return migrationStructure.file( "lastxinformation" );
    }

    private static File lastTxLogPositionFile( DatabaseLayout migrationStructure )
    {
        return migrationStructure.file( "lastxlogposition" );
    }

    TransactionId extractTransactionIdInformation( File neoStore, long lastTransactionId )
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
    private static TransactionId specificTransactionInformationSupplier( long lastTransactionId )
    {
        return lastTransactionId == TransactionIdStore.BASE_TX_ID
                                          ? new TransactionId( lastTransactionId, BASE_TX_CHECKSUM, BASE_TX_COMMIT_TIMESTAMP )
                                          : new TransactionId( lastTransactionId, UNKNOWN_TX_CHECKSUM, UNKNOWN_TX_COMMIT_TIMESTAMP );
    }

    LogPosition extractTransactionLogPosition( File neoStore, DatabaseLayout sourceDirectoryStructure, long lastTxId ) throws IOException
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

        LogFiles logFiles = LogFilesBuilder.activeFilesBuilder( sourceDirectoryStructure, fileSystem, pageCache )
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

    private void migrateWithBatchImporter( DatabaseLayout sourceDirectoryStructure, DatabaseLayout migrationDirectoryStructure,
            long lastTxId, long lastTxChecksum,
            long lastTxLogVersion, long lastTxLogByteOffset, ProgressReporter progressReporter,
            RecordFormats oldFormat, RecordFormats newFormat )
            throws IOException
    {
        prepareBatchImportMigration( sourceDirectoryStructure, migrationDirectoryStructure, oldFormat, newFormat );

        boolean requiresDynamicStoreMigration = !newFormat.dynamic().equals( oldFormat.dynamic() );
        boolean requiresPropertyMigration =
                !newFormat.property().equals( oldFormat.property() ) || requiresDynamicStoreMigration;
        File badFile = sourceDirectoryStructure.file( Configuration.BAD_FILE_NAME );
        try ( NeoStores legacyStore = instantiateLegacyStore( oldFormat, sourceDirectoryStructure );
              OutputStream badOutput = new BufferedOutputStream( new FileOutputStream( badFile, false ) ) )
        {
            Configuration importConfig = new Configuration.Overridden( config )
            {
                @Override
                public boolean highIO()
                {
                    return FileUtils.highIODevice( sourceDirectoryStructure.databaseDirectory().toPath(), super.highIO() );
                }
            };
            AdditionalInitialIds additionalInitialIds =
                    readAdditionalIds( lastTxId, lastTxChecksum, lastTxLogVersion, lastTxLogByteOffset );

            // We have to make sure to keep the token ids if we're migrating properties/labels
            BatchImporter importer = BatchImporterFactory.withHighestPriority().instantiate( migrationDirectoryStructure,
                    fileSystem, pageCache, importConfig, logService,
                    withDynamicProcessorAssignment( migrationBatchImporterMonitor( legacyStore, progressReporter,
                            importConfig ), importConfig ), additionalInitialIds, config, newFormat, NO_MONITOR, jobScheduler );
            InputIterable nodes = () -> legacyNodesAsInput( legacyStore, requiresPropertyMigration );
            InputIterable relationships = () -> legacyRelationshipsAsInput( legacyStore, requiresPropertyMigration );
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
                // We didn't migrate labels (dynamic node labels) or any other dynamic store
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
            fileOperation( DELETE, fileSystem, migrationDirectoryStructure, migrationDirectoryStructure, storesToDeleteFromMigratedDirectory,
                    true, null );
        }
    }

    private static long storeSize( CommonAbstractStore<? extends AbstractBaseRecord,? extends StoreHeader> store )
    {
        return store.getNumberOfIdsInUse() * store.getRecordSize();
    }

    private NeoStores instantiateLegacyStore( RecordFormats format, DatabaseLayout directoryStructure )
    {
        return new StoreFactory( directoryStructure, config, new ReadOnlyIdGeneratorFactory(), pageCache, fileSystem,
                format, NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY ).openAllNeoStores( true );
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
                    Arrays.asList( storesFilesToMigrate ), true, ExistingTargetStrategy.FAIL);
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

            migrator.migrate( sourceDirectoryStructure, oldFormat, migrationStrcuture, newFormat, progressReporter, storesToMigrate, StoreType.NODE );
        }
    }

    private void createStore( DatabaseLayout migrationDirectoryStructure, RecordFormats newFormat )
    {
        IdGeneratorFactory idGeneratorFactory = new ReadOnlyIdGeneratorFactory( fileSystem );
        NullLogProvider logProvider = NullLogProvider.getInstance();
        StoreFactory storeFactory = new StoreFactory(
                migrationDirectoryStructure, config, idGeneratorFactory, pageCache, fileSystem, newFormat, logProvider,
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

    private static AdditionalInitialIds readAdditionalIds( final long lastTxId, final long lastTxChecksum, final long lastTxLogVersion,
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

    private static ExecutionMonitor migrationBatchImporterMonitor( NeoStores legacyStore, final ProgressReporter progressReporter, Configuration config )
    {
        return new BatchImporterProgressMonitor(
                legacyStore.getNodeStore().getHighId(), legacyStore.getRelationshipStore().getHighId(),
                config, progressReporter );
    }

    private static InputIterator legacyRelationshipsAsInput( NeoStores legacyStore, boolean requiresPropertyMigration )
    {
        return new StoreScanAsInputIterator<RelationshipRecord>( legacyStore.getRelationshipStore() )
        {
            @Override
            public InputChunk newChunk()
            {
                return new RelationshipRecordChunk( new RecordStorageReader( legacyStore ), requiresPropertyMigration );
            }
        };
    }

    private static InputIterator legacyNodesAsInput( NeoStores legacyStore, boolean requiresPropertyMigration )
    {
        return new StoreScanAsInputIterator<NodeRecord>( legacyStore.getNodeStore() )
        {
            @Override
            public InputChunk newChunk()
            {
                return new NodeRecordChunk( new RecordStorageReader( legacyStore ), requiresPropertyMigration );
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
                ExistingTargetStrategy.OVERWRITE );
    }

    private void updateOrAddNeoStoreFieldsAsPartOfMigration( DatabaseLayout migrationStructure, DatabaseLayout sourceDirectoryStructure,
            String versionToMigrateTo, LogPosition lastClosedTxLogPosition ) throws IOException
    {
        final File storeDirNeoStore = sourceDirectoryStructure.metadataStore();
        final File migrationDirNeoStore = migrationStructure.metadataStore();
        fileOperation( COPY, fileSystem, sourceDirectoryStructure,
                migrationStructure, Iterables.iterable( DatabaseFile.METADATA_STORE ), true,
                ExistingTargetStrategy.SKIP );

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
        TransactionId lastTxInfo = readLastTxInformation( migrationStructure );

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
    public void cleanup( DatabaseLayout migrationLayout ) throws IOException
    {
        fileSystem.deleteRecursively( migrationLayout.databaseDirectory() );
    }

    @Override
    public String toString()
    {
        return "Kernel StoreMigrator";
    }

    private static class NodeRecordChunk extends StoreScanChunk<RecordNodeCursor>
    {
        NodeRecordChunk( RecordStorageReader storageReader, boolean requiresPropertyMigration )
        {
            super( storageReader.allocateNodeCursor(), storageReader, requiresPropertyMigration );
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
        RelationshipRecordChunk( RecordStorageReader storageReader, boolean requiresPropertyMigration )
        {
            super( storageReader.allocateRelationshipScanCursor(), storageReader, requiresPropertyMigration );
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
