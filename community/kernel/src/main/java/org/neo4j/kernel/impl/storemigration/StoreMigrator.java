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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.LabelTokenStore;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NeoStore.Position;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.RelationshipTypeTokenStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreVersionMismatchHandler;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreUtil;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.storemigration.legacylogs.LegacyLogs;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.Legacy21Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v21.propertydeduplication.PropertyDeduplicator;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.unsafe.impl.batchimport.input.SourceInputIterator;
import org.neo4j.unsafe.impl.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStore;

import static org.neo4j.helpers.UTF8.encode;
import static org.neo4j.helpers.collection.Iterables.iterable;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.kernel.impl.store.NeoStore.DEFAULT_NAME;
import static org.neo4j.kernel.impl.storemigration.FileOperation.COPY;
import static org.neo4j.kernel.impl.storemigration.FileOperation.DELETE;
import static org.neo4j.kernel.impl.storemigration.FileOperation.MOVE;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;
import static org.neo4j.unsafe.impl.batchimport.WriterFactories.parallel;
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
public class StoreMigrator implements StoreMigrationParticipant
{
    // Developers: There is a benchmark, storemigrate-benchmark, that generates large stores and benchmarks
    // the upgrade process. Please utilize that when writing upgrade code to ensure the code is fast enough to
    // complete upgrades in a reasonable time period.

    private final MigrationProgressMonitor progressMonitor;
    private final FileSystemAbstraction fileSystem;
    private final UpgradableDatabase upgradableDatabase;
    private final Config config;
    private final Logging logging;
    private final LegacyLogs legacyLogs;
    private String versionToUpgradeFrom;

    // TODO progress meter should be an aspect of StoreUpgrader, not specific to this participant.

    public StoreMigrator( MigrationProgressMonitor progressMonitor, FileSystemAbstraction fileSystem,
                          Logging logging )
    {
        this( progressMonitor, fileSystem, new UpgradableDatabase( new StoreVersionCheck( fileSystem ) ),
                new Config(), logging );
    }

    public StoreMigrator( MigrationProgressMonitor progressMonitor, FileSystemAbstraction fileSystem,
                          UpgradableDatabase upgradableDatabase, Config config, Logging logging )
    {
        this.progressMonitor = progressMonitor;
        this.fileSystem = fileSystem;
        this.upgradableDatabase = upgradableDatabase;
        this.config = config;
        this.logging = logging;
        this.legacyLogs = new LegacyLogs( fileSystem );
    }

    @Override
    public boolean needsMigration( File storeDir ) throws IOException
    {
        boolean sameVersion = upgradableDatabase.hasCurrentVersion( fileSystem, storeDir );
        if ( !sameVersion )
        {
            upgradableDatabase.checkUpgradeable( storeDir );
        }
        return !sameVersion;
    }

    /**
     * Will detect which version we're upgrading from.
     * Doing that initialization here is good because we do this check when
     * {@link #moveMigratedFiles(File, File) moving migrated files}, which might be done
     * as part of a resumed migration, i.e. run even if
     * {@link StoreMigrationParticipant#migrate(java.io.File, java.io.File, org.neo4j.kernel.api.index.SchemaIndexProvider, org.neo4j.io.pagecache.PageCache)}
     * hasn't been run.
     */
    private String versionToUpgradeFrom( File storeDir )
    {
        if ( versionToUpgradeFrom == null )
        {
            versionToUpgradeFrom = upgradableDatabase.checkUpgradeable( storeDir );
        }
        return versionToUpgradeFrom;
    }

    @Override
    public void migrate( File storeDir, File migrationDir, SchemaIndexProvider schemaIndexProvider,
                         PageCache pageCache ) throws IOException
    {
        progressMonitor.started();

        // Extract information about the last transaction from legacy neostore
        NeoStoreUtil neoStoreAccess = new NeoStoreUtil( storeDir, fileSystem );
        long lastTxId = neoStoreAccess.getLastCommittedTx();
        long lastTxChecksum = extractTransactionChecksum( neoStoreAccess, storeDir, lastTxId );
        // Write the tx checksum to file in migrationDir, because we need it later when moving files into storeDir
        writeLastTxChecksum( migrationDir, lastTxChecksum );

        if ( versionToUpgradeFrom( storeDir ).equals( Legacy21Store.LEGACY_VERSION ) )
        {
            // create counters from scratch
            removeDuplicateEntityProperties( storeDir, migrationDir, pageCache, schemaIndexProvider );
            rebuildCountsFromScratch( storeDir, migrationDir, lastTxId, pageCache );
        }
        else
        {
            // migrate stores
            migrateWithBatchImporter( storeDir, migrationDir, lastTxId, lastTxChecksum, pageCache );

            // don't create counters from scratch, since the batch importer just did
        }

        // DO NOT migrate logs. LegacyLogs is able to migrate logs, but only changes its format, not any
        // contents of it, and since the record format has changed there would be a mismatch between the
        // commands in the log and the contents in the store. If log migration is to be performed there
        // must be a proper translation happening while doing so.

        progressMonitor.finished();
    }

    private void writeLastTxChecksum( File migrationDir, long lastTxChecksum ) throws IOException
    {
        try ( Writer writer = fileSystem.openAsWriter( lastTxChecksumFile( migrationDir ), "utf-8", false ) )
        {
            writer.write( String.valueOf( lastTxChecksum ) );
        }
    }

    private long readLastTxChecksum( File migrationDir ) throws IOException
    {
        try ( Reader reader = fileSystem.openAsReader( lastTxChecksumFile( migrationDir ), "utf-8" ) )
        {
            char[] buffer = new char[100];
            int chars = reader.read( buffer );
            return Long.parseLong( String.valueOf( buffer, 0, chars ) );
        }
    }

    private File lastTxChecksumFile( File migrationDir )
    {
        return new File( migrationDir, "lastxchecksum" );
    }

    private long extractTransactionChecksum( NeoStoreUtil neoStoreAccess, File storeDir, long txId )
    {
        try
        {
            return neoStoreAccess.getLastCommittedTxChecksum();
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

    private void copyStores( File storeDir, File migrationDir, String... suffixes ) throws IOException {
        for (String suffix: suffixes) {
            FileUtils.copyFile(
                    new File( storeDir, NeoStore.DEFAULT_NAME + suffix ),
                    new File( migrationDir, NeoStore.DEFAULT_NAME + suffix )
            );
        }
    }

    private void removeDuplicateEntityProperties( File storeDir, File migrationDir, PageCache pageCache,
                                                  SchemaIndexProvider schemaIndexProvider ) throws IOException
    {
        copyStores( storeDir, migrationDir,
                StoreFactory.PROPERTY_STORE_NAME,
                StoreFactory.PROPERTY_STORE_NAME + ".id",
                StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME,
                StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME + ".id",
                StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME,
                StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME + ".id",
                StoreFactory.PROPERTY_STRINGS_STORE_NAME,
                StoreFactory.PROPERTY_ARRAYS_STORE_NAME,
                StoreFactory.NODE_STORE_NAME,
                StoreFactory.NODE_STORE_NAME + ".id",
                StoreFactory.NODE_LABELS_STORE_NAME,
                StoreFactory.SCHEMA_STORE_NAME
        );

        PropertyDeduplicator deduplicator = new PropertyDeduplicator(
                fileSystem, migrationDir, pageCache, schemaIndexProvider );
        deduplicator.deduplicateProperties();
    }

    private void rebuildCountsFromScratch(
            File storeDir, File migrationDir, long lastTxId, PageCache pageCache ) throws IOException
    {
        final File storeFileBase = new File( migrationDir, NeoStore.DEFAULT_NAME + StoreFactory.COUNTS_STORE );

        final StoreFactory storeFactory =
                new StoreFactory( fileSystem, storeDir, pageCache, DEV_NULL, new Monitors(),
                        StoreVersionMismatchHandler.ALLOW_OLD_VERSION );
        try ( NodeStore nodeStore = storeFactory.newNodeStore();
              RelationshipStore relationshipStore = storeFactory.newRelationshipStore() )
        {
            try ( Lifespan life = new Lifespan() )
            {
                int highLabelId = (int) storeFactory.getHighId( StoreFile.LABEL_TOKEN_STORE,
                                                                LabelTokenStore.RECORD_SIZE );
                int highRelationshipTypeId = (int) storeFactory.getHighId( StoreFile.RELATIONSHIP_TYPE_TOKEN_STORE,
                                                                           RelationshipTypeTokenStore.RECORD_SIZE );
                CountsComputer initializer = new CountsComputer(
                        lastTxId, nodeStore, relationshipStore, highLabelId, highRelationshipTypeId );
                life.add( new CountsTracker(
                        logging.getMessagesLog( CountsTracker.class ), fileSystem, pageCache, storeFileBase )
                                  .setInitializer( initializer ) );
            }
        }
    }

    private void migrateWithBatchImporter( File storeDir, File migrationDir, long lastTxId, long lastTxChecksum,
            PageCache pageCache ) throws IOException
    {
        prepareBatchImportMigration( storeDir, migrationDir );

        LegacyStore legacyStore;
        switch ( versionToUpgradeFrom( storeDir ) )
        {
            case Legacy19Store.LEGACY_VERSION:
                legacyStore = new Legacy19Store( fileSystem, new File( storeDir, NeoStore.DEFAULT_NAME ) );
                break;
            case Legacy20Store.LEGACY_VERSION:
                legacyStore = new Legacy20Store( fileSystem, new File( storeDir, NeoStore.DEFAULT_NAME ) );
                break;
            default:
                throw new IllegalStateException( "Unknown version to upgrade from: " + versionToUpgradeFrom( storeDir ) );
        }

        Configuration importConfig = new Configuration.Overridden( config );
        BatchImporter importer = new ParallelBatchImporter( migrationDir.getAbsolutePath(), fileSystem,
                importConfig, logging, withDynamicProcessorAssignment( migrationBatchImporterMonitor(
                        legacyStore, progressMonitor ), importConfig ),
                parallel(), readAdditionalIds( storeDir, lastTxId, lastTxChecksum ) );
        InputIterable<InputNode> nodes = legacyNodesAsInput( legacyStore );
        InputIterable<InputRelationship> relationships = legacyRelationshipsAsInput( legacyStore );
        importer.doImport( Inputs.input( nodes, relationships, IdMappers.actual(), IdGenerators.fromInput(), true, 0 ) );

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
        BatchingNeoStore.createStore( fileSystem, migrationDir.getPath() );
        Iterable<StoreFile> storeFiles = iterable( StoreFile.NODE_LABEL_STORE );
        StoreFile.fileOperation( COPY, fileSystem, storeDir, migrationDir, storeFiles,
                true, // OK if it's not there (1.9)
                false, StoreFileType.values() );
        StoreFile.ensureStoreVersion( fileSystem, migrationDir, storeFiles );
    }

    private AdditionalInitialIds readAdditionalIds( File storeDir, final long lastTxId, final long lastTxChecksum )
            throws IOException
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
            MigrationProgressMonitor progressMonitor2 )
    {
        return new CoarseBoundedProgressExecutionMonitor(
                legacyStore.getNodeStoreReader().getMaxId(), legacyStore.getRelStoreReader().getMaxId() )
        {
            @Override
            protected void percent( int percent )
            {
                progressMonitor.percentComplete( percent );
            }
        };
    }

    private StoreFactory storeFactory( PageCache pageCache, File migrationDir )
    {
        return new StoreFactory(
                StoreFactory.configForStoreDir( config, migrationDir ),
                new DefaultIdGeneratorFactory(), pageCache,
                fileSystem, DEV_NULL, new Monitors() );
    }

    private void migratePropertyKeys( Legacy19Store legacyStore, PageCache pageCache, File migrationDir )
            throws IOException
    {
        Token[] tokens = legacyStore.getPropertyIndexReader().readTokens();
        if ( containsAnyDuplicates( tokens ) )
        {   // The legacy property key token store contains duplicates, copy over and deduplicate
            // property key token store and go through property store with the new token ids.
            StoreFactory storeFactory = storeFactory( pageCache, migrationDir );
            storeFactory.createPropertyStore();
            try ( PropertyStore propertyStore = storeFactory.newPropertyStore() )
            {
                // dedup and write new property key token store (incl. names)
                Map<Integer, Integer> propertyKeyTranslation = dedupAndWritePropertyKeyTokenStore( propertyStore, tokens );

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

    private Map<Integer, Integer> dedupAndWritePropertyKeyTokenStore(
            PropertyStore propertyStore, Token[] tokens /*ordered ASC*/ )
    {
        PropertyKeyTokenStore keyTokenStore = propertyStore.getPropertyKeyTokenStore();
        Map<Integer/*duplicate*/, Integer/*use this instead*/> translations = new HashMap<>();
        Map<String, Integer> createdTokens = new HashMap<>();
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

    private void migratePropertyStore( Legacy19Store legacyStore, Map<Integer, Integer> propertyKeyTranslation,
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
                return new SourceInputIterator<InputRelationship, RelationshipRecord>( traceability )
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
                                "legacy store", record.getId(), record.getId()*RelationshipStore.RECORD_SIZE,
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
                return new SourceInputIterator<InputNode, NodeRecord>( traceability )
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
                                "legacy store", record.getId(), record.getId()*NodeStore.RECORD_SIZE,
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
    public void moveMigratedFiles( File migrationDir, File storeDir ) throws IOException
    {
        // The batch importer will create a whole store. so
        // Disregard the new and empty node/relationship".id" files, i.e. reuse the existing id files

        Iterable<StoreFile> filesToMove;
        StoreFile[] idFilesToDelete;
        switch ( versionToUpgradeFrom( storeDir ) )
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
            default:
                throw new IllegalStateException( "Unknown version to upgrade from: " + versionToUpgradeFrom( storeDir ) );
        }

        StoreFile.fileOperation( DELETE, fileSystem, migrationDir, null,
                Iterables.<StoreFile,StoreFile>iterable( idFilesToDelete ),
                true, false, StoreFileType.ID );

        // Move the migrated ones into the store directory
        StoreFile.fileOperation( MOVE, fileSystem, migrationDir, storeDir, filesToMove,
                true, // allow to skip non existent source files
                true, // allow to overwrite target files
                StoreFileType.values() );

        // ensure the store version is correct
        ensureStoreVersions( storeDir );

        // update or add upgrade id and time and other necessary neostore records
        updateOrAddNeoStoreFieldsAsPartOfMigration( migrationDir, storeDir );

        // delete old logs
        legacyLogs.operate( DELETE, storeDir, null );
        legacyLogs.deleteUnusedLogFiles( storeDir );
    }

    private void ensureStoreVersions( File dir ) throws IOException
    {
        final Iterable<StoreFile> versionedStores = iterable( allExcept() );
        StoreFile.ensureStoreVersion( fileSystem, dir, versionedStores );
    }

    private void updateOrAddNeoStoreFieldsAsPartOfMigration( File migrationDir, File storeDir )
            throws IOException
    {
        final File storeDirNeoStore = new File( storeDir, NeoStore.DEFAULT_NAME );
        NeoStore.setRecord( fileSystem, storeDirNeoStore, Position.UPGRADE_TRANSACTION_ID,
                NeoStore.getRecord( fileSystem, storeDirNeoStore, Position.LAST_TRANSACTION_ID ) );
        NeoStore.setRecord( fileSystem, storeDirNeoStore, Position.UPGRADE_TIME, System.currentTimeMillis() );

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
        long lastTxChecksum = readLastTxChecksum( migrationDir );
        NeoStore.setRecord( fileSystem, storeDirNeoStore, Position.LAST_TRANSACTION_CHECKSUM, lastTxChecksum );
        NeoStore.setRecord( fileSystem, storeDirNeoStore, Position.UPGRADE_TRANSACTION_CHECKSUM, lastTxChecksum );
    }

    @Override
    public void cleanup( File migrationDir ) throws IOException
    {
        fileSystem.deleteRecursively( migrationDir );
    }

    @Override
    public void close()
    { // nothing to do
    }

    @Override
    public String toString()
    {
        return "Kernel StoreMigrator";
    }
}
