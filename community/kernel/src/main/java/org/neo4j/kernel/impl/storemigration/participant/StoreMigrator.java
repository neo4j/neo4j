/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.MetaDataStore.Position;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.storemigration.StoreFile;
import org.neo4j.kernel.impl.storemigration.StoreFileType;
import org.neo4j.kernel.impl.storemigration.StoreMigratorCheckPointer;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogs;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication.PropertyDeduplicator;
import org.neo4j.kernel.impl.storemigration.legacystore.v22.Legacy22Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v23.Legacy23Store;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFiles;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.Collectors;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.unsafe.impl.batchimport.input.SourceInputIterator;
import org.neo4j.unsafe.impl.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.neo4j.helpers.UTF8.encode;
import static org.neo4j.helpers.collection.Iterables.iterable;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.storemigration.FileOperation.COPY;
import static org.neo4j.kernel.impl.storemigration.FileOperation.DELETE;
import static org.neo4j.kernel.impl.storemigration.FileOperation.MOVE;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_LOG_BYTE_OFFSET;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_LOG_VERSION;
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

    private final Config config;
    private final LogService logService;
    private final LegacyLogs legacyLogs;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final SchemaIndexProvider schemaIndexProvider;

    public StoreMigrator( FileSystemAbstraction fileSystem, PageCache pageCache, Config config,
            LogService logService, SchemaIndexProvider schemaIndexProvider )
    {
        super( "Store files" );
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.config = config;
        this.logService = logService;
        this.schemaIndexProvider = schemaIndexProvider;
        this.legacyLogs = new LegacyLogs( fileSystem );
    }

    @Override
    public void migrate( File storeDir, File migrationDir, MigrationProgressMonitor.Section progressMonitor,
            String versionToMigrateFrom ) throws IOException
    {
        // Extract information about the last transaction from legacy neostore
        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        long lastTxId = MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_ID );
        long lastTxChecksum = extractTransactionChecksum( neoStore, storeDir, lastTxId );
        LogPosition lastTxLogPosition = extractTransactionLogPosition( neoStore, storeDir, lastTxId );
        // Write the tx checksum to file in migrationDir, because we need it later when moving files into storeDir
        writeLastTxChecksum( migrationDir, lastTxChecksum );
        writeLastTxLogPosition( migrationDir, lastTxLogPosition );

        switch ( versionToMigrateFrom )
        {
        case Legacy23Store.LEGACY_VERSION:
        case Legacy22Store.LEGACY_VERSION:
            // nothing to do
            break;
        case Legacy21Store.LEGACY_VERSION:
            removeDuplicateEntityProperties(
                    storeDir, migrationDir, pageCache, schemaIndexProvider, Legacy21Store.LEGACY_VERSION );
            break;
        case Legacy20Store.LEGACY_VERSION:
        case Legacy19Store.LEGACY_VERSION:
            // migrate stores
            migrateWithBatchImporter( storeDir, migrationDir,
                    lastTxId, lastTxChecksum, lastTxLogPosition.getLogVersion(), lastTxLogPosition.getByteOffset(),
                    pageCache, progressMonitor, versionToMigrateFrom );
            // don't create counters from scratch, since the batch importer just did
            break;
        default:
            throw new IllegalStateException( "Unknown version to upgrade from: " + versionToMigrateFrom );
        }

        // DO NOT migrate logs. LegacyLogs is able to migrate logs, but only changes its format, not any
        // contents of it, and since the record format has changed there would be a mismatch between the
        // commands in the log and the contents in the store. If log migration is to be performed there
        // must be a proper translation happening while doing so.
    }

    private void writeLastTxChecksum( File migrationDir, long lastTxChecksum ) throws IOException
    {
        try ( Writer writer = fileSystem.openAsWriter( lastTxChecksumFile( migrationDir ), UTF_8, false ) )
        {
            writer.write( String.valueOf( lastTxChecksum ) );
        }
    }

    private void writeLastTxLogPosition( File migrationDir, LogPosition lastTxLogPosition ) throws IOException
    {
        try ( Writer writer = fileSystem.openAsWriter( lastTxLogPositionFile( migrationDir ), UTF_8, false ) )
        {
            writer.write( lastTxLogPosition.getLogVersion() + "A" + lastTxLogPosition.getByteOffset() );
        }
    }

    // accessible for tests
    static long readLastTxChecksum( FileSystemAbstraction fileSystem, File migrationDir ) throws IOException
    {
        try ( Reader reader = fileSystem.openAsReader( lastTxChecksumFile( migrationDir ), UTF_8 ) )
        {
            char[] buffer = new char[100];
            int chars = reader.read( buffer );
            return Long.parseLong( String.valueOf( buffer, 0, chars ) );
        }
    }

    // accessible for tests
    static LogPosition readLastTxLogPosition( FileSystemAbstraction fileSystem, File migrationDir ) throws IOException
    {
        try ( Reader reader = fileSystem.openAsReader( lastTxLogPositionFile( migrationDir ), UTF_8 ) )
        {
            char[] buffer = new char[4096];
            int chars = reader.read( buffer );
            String s = String.valueOf( buffer, 0, chars );
            String[] split = s.split( "A" );
            return new LogPosition( Long.parseLong( split[0] ), Long.parseLong( split[1] ) );
        }
    }

    private static File lastTxChecksumFile( File migrationDir )
    {
        return new File( migrationDir, "lastxchecksum" );
    }

    private static File lastTxLogPositionFile( File migrationDir )
    {
        return new File( migrationDir, "lastxlogposition" );
    }

    private long extractTransactionChecksum( File neoStore, File storeDir, long txId ) throws IOException
    {
        try
        {
            return MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_CHECKSUM );
        }
        catch ( IllegalStateException e )
        {
            // The legacy store we're migrating doesn't have this record in neostore so try to extract it from tx log
            try
            {
                return legacyLogs.getTransactionChecksum( storeDir, txId );
            }
            catch ( IOException ioe )
            {
                // OK, so the legacy store didn't even have this transaction checksum in its transaction logs,
                // so just generate a random new one. I don't think it matters since we know that in a
                // multi-database scenario there can only be one of them upgrading, the other ones will have to
                // copy that database.
                return txId == TransactionIdStore.BASE_TX_ID
                       ? TransactionIdStore.BASE_TX_CHECKSUM
                       : Math.abs( new Random().nextLong() );
            }
        }
    }

    private LogPosition extractTransactionLogPosition( File neoStore, File storeDir, long lastTxId ) throws IOException
    {
        long lastClosedTxLogVersion =
                MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_CLOSED_TRANSACTION_LOG_VERSION );
        long lastClosedTxLogByteOffset =
                MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET );
        if ( lastClosedTxLogVersion != MetaDataStore.FIELD_NOT_PRESENT &&
             lastClosedTxLogByteOffset != MetaDataStore.FIELD_NOT_PRESENT )
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
            SchemaIndexProvider schemaIndexProvider, String versionToUpgradeFrom ) throws IOException
    {
        StoreFile.fileOperation( COPY, fileSystem, storeDir, migrationDir, Iterables.<StoreFile,StoreFile>iterable(
                StoreFile.PROPERTY_STORE,
                StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                StoreFile.PROPERTY_KEY_TOKEN_STORE,
                StoreFile.PROPERTY_ARRAY_STORE,
                StoreFile.PROPERTY_STRING_STORE,
                StoreFile.NODE_STORE,
                StoreFile.NODE_LABEL_STORE,
                StoreFile.SCHEMA_STORE ), false, false, StoreFileType.STORE );

        // copy ids only if present
        StoreFile.fileOperation( COPY, fileSystem, storeDir, migrationDir, Iterables.<StoreFile,StoreFile>iterable(
                StoreFile.PROPERTY_STORE,
                StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                StoreFile.PROPERTY_KEY_TOKEN_STORE,
                StoreFile.NODE_STORE ), true, false, StoreFileType.ID );

        // let's remove trailers here on the copied files since the new code doesn't remove them since in 2.3
        // there are no store trailers
        StoreFile.removeTrailers( versionToUpgradeFrom, fileSystem, migrationDir, pageCache.pageSize() );

        new PropertyDeduplicator( fileSystem, migrationDir, pageCache, schemaIndexProvider ).deduplicateProperties();
    }

    private void rebuildCountsFromScratch( File storeDir, long lastTxId, PageCache pageCache ) throws IOException
    {
        final File storeFileBase = new File( storeDir, MetaDataStore.DEFAULT_NAME + StoreFactory.COUNTS_STORE );

        final StoreFactory storeFactory =
                new StoreFactory( fileSystem, storeDir, pageCache, NullLogProvider.getInstance() );
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
            long lastTxLogVersion, long lastTxLogByteOffset, PageCache pageCache,
            MigrationProgressMonitor.Section progressMonitor, String versionToUpgradeFrom )
            throws IOException
    {
        prepareBatchImportMigration( storeDir, migrationDir );

        LegacyStore legacyStore;
        switch ( versionToUpgradeFrom )
        {
        case Legacy19Store.LEGACY_VERSION:
            legacyStore = new Legacy19Store( fileSystem, new File( storeDir, MetaDataStore.DEFAULT_NAME ) );
            break;
        case Legacy20Store.LEGACY_VERSION:
            legacyStore = new Legacy20Store( fileSystem, new File( storeDir, MetaDataStore.DEFAULT_NAME ) );
            break;
        default:
            throw new IllegalStateException( "Unknown version to upgrade from: " + versionToUpgradeFrom );
        }

        Configuration importConfig = new Configuration.Overridden( config );
        AdditionalInitialIds additionalInitialIds =
                readAdditionalIds( storeDir, lastTxId, lastTxChecksum, lastTxLogVersion, lastTxLogByteOffset );
        BatchImporter importer = new ParallelBatchImporter( migrationDir.getAbsoluteFile(), fileSystem,
                importConfig, logService,
                withDynamicProcessorAssignment( migrationBatchImporterMonitor( legacyStore, progressMonitor,
                        importConfig ), importConfig ), additionalInitialIds );
        InputIterable<InputNode> nodes = legacyNodesAsInput( legacyStore );
        InputIterable<InputRelationship> relationships = legacyRelationshipsAsInput( legacyStore );
        File badFile = new File( storeDir, Configuration.BAD_FILE_NAME );
        OutputStream badOutput = new BufferedOutputStream( new FileOutputStream( badFile, false ) );
        importer.doImport(
                Inputs.input( nodes, relationships, IdMappers.actual(), IdGenerators.fromInput(), true,
                        Collectors.badCollector( badOutput, 0 ) ) );

        // During migration the batch importer only writes node, relationship, relationship group and counts stores.
        // Delete the property store files from the batch import migration so that even if we won't
        // migrate property stores as part of deduplicating property key tokens or properties then
        // we won't move these empty property store files to the store directory, overwriting the old ones.
        StoreFile.fileOperation( DELETE, fileSystem, migrationDir, null, Iterables.<StoreFile,StoreFile>iterable(
                StoreFile.PROPERTY_STORE,
                StoreFile.PROPERTY_STRING_STORE,
                StoreFile.PROPERTY_ARRAY_STORE,
                StoreFile.PROPERTY_KEY_TOKEN_STORE,
                StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE ), true, false, StoreFileType.values() );

        // Finish the import of nodes and relationships
        if ( legacyStore instanceof Legacy19Store )
        {
            // we may need to upgrade the property tokens
            migratePropertyKeys( (Legacy19Store) legacyStore, pageCache, migrationDir );
        }
        // Close
        legacyStore.close();
    }

    private void prepareBatchImportMigration( File storeDir, File migrationDir ) throws IOException
    {
        // We use the batch importer for migrating the data, and we use it in a special way where we only
        // rewrite the stores that have actually changed format. We know that to be node and relationship
        // stores. Although since the batch importer also populates the counts store, all labels need to
        // be read, i.e. both inlined and those existing in dynamic records. That's why we need to copy
        // that dynamic record store over before doing the "batch import".
        //   Copying this file just as-is assumes that the format hasn't change. If that happens we're in
        // a different situation, where we first need to migrate this file.
        BatchingNeoStores.createStore( fileSystem, migrationDir.getPath() );
        Iterable<StoreFile> storeFiles = iterable( StoreFile.NODE_LABEL_STORE );
        StoreFile.fileOperation( COPY, fileSystem, storeDir, migrationDir, storeFiles,
                true, // OK if it's not there (1.9)
                false, StoreFileType.values() );
    }

    private AdditionalInitialIds readAdditionalIds( File storeDir, final long lastTxId, final long lastTxChecksum,
            final long lastTxLogVersion, final long lastTxLogByteOffset ) throws IOException
    {
        final int propertyKeyTokenHighId =
                readHighIdFromIdFileIfExists( storeDir, StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME );
        final int labelTokenHighId =
                readHighIdFromIdFileIfExists( storeDir, StoreFactory.LABEL_TOKEN_STORE_NAME );
        final int relationshipTypeTokenHighId =
                readHighIdFromIdFileIfExists( storeDir, StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME );
        return new AdditionalInitialIds()
        {
            @Override
            public int highRelationshipTypeTokenId()
            {
                return relationshipTypeTokenHighId;
            }

            @Override
            public int highPropertyKeyTokenId()
            {
                return propertyKeyTokenHighId;
            }

            @Override
            public int highLabelTokenId()
            {
                return labelTokenHighId;
            }

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

    private int readHighIdFromIdFileIfExists( File storeDir, String storeName ) throws IOException
    {
        String file = StoreFileType.ID.augment( new File( storeDir, DEFAULT_NAME + storeName ).getPath() );
        try
        {
            return (int) IdGeneratorImpl.readHighId( fileSystem, new File( file ) );
        }
        catch ( FileNotFoundException e )
        {
            return 0;
        }
    }

    private ExecutionMonitor migrationBatchImporterMonitor( LegacyStore legacyStore,
            final MigrationProgressMonitor.Section progressMonitor, Configuration config )
    {
        return new BatchImporterProgressMonitor(
                legacyStore.getNodeStoreReader().getMaxId(), legacyStore.getRelStoreReader().getMaxId(),
                config, progressMonitor );
    }

    private StoreFactory storeFactory( PageCache pageCache, File migrationDir )
    {
        return new StoreFactory( migrationDir, new Config(), new DefaultIdGeneratorFactory( fileSystem ), pageCache,
                fileSystem, NullLogProvider.getInstance() );
    }

    private void migratePropertyKeys( Legacy19Store legacyStore, PageCache pageCache, File migrationDir )
            throws IOException
    {
        Token[] tokens = legacyStore.getPropertyIndexReader().readTokens();
        if ( containsAnyDuplicates( tokens ) )
        {   // The legacy property key token store contains duplicates, copy over and deduplicate
            // property key token store and go through property store with the new token ids.
            StoreFactory storeFactory = storeFactory( pageCache, migrationDir );
            try ( NeoStores neoStores = storeFactory.openAllNeoStores( true ) )
            {
                PropertyStore propertyStore = neoStores.getPropertyStore();
                // dedup and write new property key token store (incl. names)
                Map<Integer,Integer> propertyKeyTranslation =
                        dedupAndWritePropertyKeyTokenStore( propertyStore, tokens );

                // read property store, replace property key ids
                migratePropertyStore( legacyStore, propertyKeyTranslation, propertyStore );
            }
        }
    }

    private boolean containsAnyDuplicates( Token[] tokens )
    {
        Set<String> names = new HashSet<>();
        for ( Token token : tokens )
        {
            if ( !names.add( token.name() ) )
            {
                return true;
            }
        }
        return false;
    }

    private Map<Integer,Integer> dedupAndWritePropertyKeyTokenStore(
            PropertyStore propertyStore, Token[] tokens /*ordered ASC*/ )
    {
        PropertyKeyTokenStore keyTokenStore = propertyStore.getPropertyKeyTokenStore();
        Map<Integer/*duplicate*/,Integer/*use this instead*/> translations = new HashMap<>();
        Map<String,Integer> createdTokens = new HashMap<>();
        for ( Token token : tokens )
        {
            Integer id = createdTokens.get( token.name() );
            if ( id == null )
            {   // Not a duplicate, add to store
                id = (int) keyTokenStore.nextId();
                PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( id );
                Collection<DynamicRecord> nameRecords =
                        keyTokenStore.allocateNameRecords( encode( token.name() ) );
                record.setNameId( (int) first( nameRecords ).getId() );
                record.addNameRecords( nameRecords );
                record.setInUse( true );
                record.setCreated();
                keyTokenStore.updateRecord( record );
                createdTokens.put( token.name(), id );
            }
            translations.put( token.id(), id );
        }
        return translations;
    }

    private void migratePropertyStore( Legacy19Store legacyStore, Map<Integer,Integer> propertyKeyTranslation,
            PropertyStore propertyStore ) throws IOException
    {
        long lastInUseId = -1;
        for ( PropertyRecord propertyRecord : loop( legacyStore.getPropertyStoreReader().readPropertyStore() ) )
        {
            // Translate property keys
            for ( PropertyBlock block : propertyRecord )
            {
                int key = block.getKeyIndexId();
                Integer translation = propertyKeyTranslation.get( key );
                if ( translation != null )
                {
                    block.setKeyIndexId( translation );
                }
            }
            propertyStore.setHighId( propertyRecord.getId() + 1 );
            propertyStore.updateRecord( propertyRecord );
            for ( long id = lastInUseId + 1; id < propertyRecord.getId(); id++ )
            {
                propertyStore.freeId( id );
            }
            lastInUseId = propertyRecord.getId();
        }
    }

    private StoreFile[] allExcept( StoreFile... exceptions )
    {
        List<StoreFile> result = new ArrayList<>();
        result.addAll( Arrays.asList( StoreFile.values() ) );
        for ( StoreFile except : exceptions )
        {
            result.remove( except );
        }
        return result.toArray( new StoreFile[result.size()] );
    }

    private InputIterable<InputRelationship> legacyRelationshipsAsInput( LegacyStore legacyStore )
    {
        final LegacyRelationshipStoreReader reader = legacyStore.getRelStoreReader();
        return new InputIterable<InputRelationship>()
        {
            @Override
            public InputIterator<InputRelationship> iterator()
            {
                final Iterator<RelationshipRecord> source;
                try
                {
                    source = reader.iterator( 0 );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }

                final StoreSourceTraceability traceability =
                        new StoreSourceTraceability( "legacy relationships", reader.getRecordSize() );
                return new SourceInputIterator<InputRelationship,RelationshipRecord>( traceability )
                {
                    @Override
                    public boolean hasNext()
                    {
                        return source.hasNext();
                    }

                    @Override
                    public InputRelationship next()
                    {
                        if ( !hasNext() )
                        {
                            throw new NoSuchElementException();
                        }

                        RelationshipRecord record = source.next();
                        InputRelationship result = new InputRelationship(
                                "legacy store", record.getId(), record.getId() * RelationshipStore.RECORD_SIZE,
                                InputEntity.NO_PROPERTIES, record.getNextProp(),
                                record.getFirstNode(), record.getSecondNode(), null, record.getType() );
                        result.setSpecificId( record.getId() );
                        traceability.atId( record.getId() );
                        return result;
                    }

                    @Override
                    public void close()
                    {
                    }
                };
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return true;
            }
        };
    }

    private InputIterable<InputNode> legacyNodesAsInput( LegacyStore legacyStore )
    {
        final LegacyNodeStoreReader reader = legacyStore.getNodeStoreReader();
        return new InputIterable<InputNode>()
        {
            @Override
            public InputIterator<InputNode> iterator()
            {
                final Iterator<NodeRecord> source;
                try
                {
                    source = reader.iterator();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }

                final StoreSourceTraceability traceability =
                        new StoreSourceTraceability( "legacy nodes", reader.getRecordSize() );
                return new SourceInputIterator<InputNode,NodeRecord>( traceability )
                {
                    @Override
                    public boolean hasNext()
                    {
                        return source.hasNext();
                    }

                    @Override
                    public InputNode next()
                    {
                        if ( !hasNext() )
                        {
                            throw new NoSuchElementException();
                        }

                        NodeRecord record = source.next();
                        traceability.atId( record.getId() );
                        return new InputNode(
                                "legacy store", record.getId(), record.getId() * NodeStore.RECORD_SIZE,
                                record.getId(), InputEntity.NO_PROPERTIES, record.getNextProp(),
                                InputNode.NO_LABELS, record.getLabelField() );
                    }

                    @Override
                    public void close()
                    {
                    }
                };
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return true;
            }
        };
    }

    @Override
    public void moveMigratedFiles( File migrationDir, File storeDir, String versionToUpgradeFrom ) throws IOException
    {
        // The batch importer will create a whole store. so
        // Disregard the new and empty node/relationship".id" files, i.e. reuse the existing id files

        Iterable<StoreFile> filesToMove;
        StoreFile[] idFilesToDelete;
        switch ( versionToUpgradeFrom )
        {
        case Legacy19Store.LEGACY_VERSION:
            filesToMove = Arrays.asList(
                    StoreFile.NODE_STORE,
                    StoreFile.RELATIONSHIP_STORE,
                    StoreFile.RELATIONSHIP_GROUP_STORE,
                    StoreFile.LABEL_TOKEN_STORE,
                    StoreFile.NODE_LABEL_STORE,
                    StoreFile.LABEL_TOKEN_NAMES_STORE,
                    StoreFile.PROPERTY_STORE,
                    StoreFile.PROPERTY_KEY_TOKEN_STORE,
                    StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE,
                    StoreFile.SCHEMA_STORE,
                    StoreFile.COUNTS_STORE_LEFT,
                    StoreFile.COUNTS_STORE_RIGHT );
            idFilesToDelete = allExcept(
                    StoreFile.RELATIONSHIP_GROUP_STORE
            );
            break;
        case Legacy20Store.LEGACY_VERSION:
            // Note: We don't overwrite the label stores in 2.0
            filesToMove = Arrays.asList(
                    StoreFile.NODE_STORE,
                    StoreFile.RELATIONSHIP_STORE,
                    StoreFile.RELATIONSHIP_GROUP_STORE,
                    StoreFile.COUNTS_STORE_LEFT,
                    StoreFile.COUNTS_STORE_RIGHT );
            idFilesToDelete = allExcept(
                    StoreFile.RELATIONSHIP_GROUP_STORE
            );
            break;
        case Legacy21Store.LEGACY_VERSION:
            filesToMove = Arrays.asList(
                    StoreFile.NODE_STORE,
                    StoreFile.COUNTS_STORE_LEFT,
                    StoreFile.COUNTS_STORE_RIGHT,
                    StoreFile.PROPERTY_STORE,
                    StoreFile.PROPERTY_KEY_TOKEN_STORE,
                    StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE );
            idFilesToDelete = new StoreFile[]{};
            break;
        case Legacy22Store.LEGACY_VERSION:
        case Legacy23Store.LEGACY_VERSION:
            filesToMove = Collections.emptyList();
            idFilesToDelete = new StoreFile[]{};
            break;
        default:
            throw new IllegalStateException( "Unknown version to upgrade from: " + versionToUpgradeFrom );
        }

        StoreFile.fileOperation( DELETE, fileSystem, migrationDir, null,
                Iterables.<StoreFile,StoreFile>iterable( idFilesToDelete ),
                true, // allow to skip non existent source files
                false, // not allow to overwrite target files
                StoreFileType.ID );

        // Move the migrated ones into the store directory
        StoreFile.fileOperation( MOVE, fileSystem, migrationDir, storeDir, filesToMove,
                true, // allow to skip non existent source files
                true, // allow to overwrite target files
                StoreFileType.values() );

        if ( !Legacy23Store.LEGACY_VERSION.equals( versionToUpgradeFrom ) )
        {
            StoreFile.removeTrailers( versionToUpgradeFrom, fileSystem, storeDir, pageCache.pageSize() );
        }

        File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        long logVersion = MetaDataStore.getRecord( pageCache, neoStore, Position.LOG_VERSION );
        long lastCommittedTx = MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_ID );

        // update or add upgrade id and time and other necessary neostore records
        updateOrAddNeoStoreFieldsAsPartOfMigration( migrationDir, storeDir );

        // delete old logs
        legacyLogs.deleteUnusedLogFiles( storeDir );

        if ( !Legacy23Store.LEGACY_VERSION.equals( versionToUpgradeFrom ) )
        {
            // write a check point in the log in order to make recovery work in the newer version
            new StoreMigratorCheckPointer( storeDir, fileSystem ).checkPoint( logVersion, lastCommittedTx );
        }
    }

    @Override
    public void rebuildCounts( File storeDir, String versionToMigrateFrom ) throws IOException
    {
        switch ( versionToMigrateFrom )
        {
        case Legacy19Store.LEGACY_VERSION:
        case Legacy20Store.LEGACY_VERSION:
        case Legacy23Store.LEGACY_VERSION:
            // nothing to do
            break;
        case Legacy21Store.LEGACY_VERSION:
        case Legacy22Store.LEGACY_VERSION:
            // create counters from scratch
            Iterable<StoreFile> countsStoreFiles =
                    Iterables.iterable( StoreFile.COUNTS_STORE_LEFT, StoreFile.COUNTS_STORE_RIGHT );
            StoreFile.fileOperation( DELETE, fileSystem, storeDir, storeDir,
                    countsStoreFiles, true, false, StoreFileType.STORE );
            File neoStore = new File( storeDir, DEFAULT_NAME );
            long lastTxId = MetaDataStore.getRecord( pageCache, neoStore, Position.LAST_TRANSACTION_ID );
            rebuildCountsFromScratch( storeDir, lastTxId, pageCache );
            break;
        default:
            throw new IllegalStateException( "Unknown version to upgrade from: " + versionToMigrateFrom );
        }
    }

    private void updateOrAddNeoStoreFieldsAsPartOfMigration( File migrationDir, File storeDir ) throws IOException
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
        long lastTxChecksum = readLastTxChecksum( fileSystem, migrationDir );
        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.LAST_TRANSACTION_CHECKSUM, lastTxChecksum );
        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.UPGRADE_TRANSACTION_CHECKSUM, lastTxChecksum );

        // add LAST_CLOSED_TRANSACTION_LOG_VERSION and LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET to the migrated
        // NeoStore
        LogPosition logPosition = readLastTxLogPosition( fileSystem, migrationDir );
        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.LAST_CLOSED_TRANSACTION_LOG_VERSION, logPosition
                .getLogVersion() );
        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.LAST_CLOSED_TRANSACTION_LOG_BYTE_OFFSET,
                logPosition.getByteOffset() );

        // Upgrade version in NeoStore
        MetaDataStore.setRecord( pageCache, storeDirNeoStore, Position.STORE_VERSION,
                MetaDataStore.versionStringToLong( ALL_STORES_VERSION ) );
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
                org.neo4j.unsafe.impl.batchimport.staging.Configuration configuration,
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
