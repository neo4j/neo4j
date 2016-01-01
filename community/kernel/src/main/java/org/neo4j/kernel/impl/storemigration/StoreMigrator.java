/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreUtil;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.PhysicalLogNeoXaCommandWriter;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyLogFiles;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyLogIoUtil;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyLuceneCommandReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19CommandReader;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19LogIoUtil;
import org.neo4j.kernel.impl.storemigration.legacystore.v19.Legacy19Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20CommandReader;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20LogIoUtil;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.Legacy20Store;
import org.neo4j.kernel.impl.storemigration.legacystore.v20.StoreFile20;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntryWriterv1;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SystemOutLogging;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappings;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;

import static org.neo4j.helpers.UTF8.encode;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.IteratorUtil.loop;

/**
 * Migrates a neo4j kernel database from one version to the next.
 *
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the migration code is specific for the current upgrade and changes with each store format version.
 *
 * Just one out of many potential participants in a {@link StoreUpgrader migration}.
 *
 * @see StoreUpgrader
 */
public class StoreMigrator extends StoreMigrationParticipant.Adapter
{
    private static final Object[] NO_PROPERTIES = new Object[0];

    // Developers: There is a benchmark, storemigrate-benchmark, that generates large stores and benchmarks
    // the upgrade process. Please utilize that when writing upgrade code to ensure the code is fast enough to
    // complete upgrades in a reasonable time period.

    private final MigrationProgressMonitor progressMonitor;
    private final UpgradableDatabase upgradableDatabase;
    private final Config config;
    private final Logging logging;
    private String versionToUpgradeFrom;
    private LegacyStore legacyStore;
    private LegacyLogFiles legacyLogFiles;

    // TODO progress meter should be an aspect of StoreUpgrader, not specific to this participant.

    public StoreMigrator( MigrationProgressMonitor progressMonitor, FileSystemAbstraction fileSystem )
    {
        this( progressMonitor, new UpgradableDatabase( new StoreVersionCheck( fileSystem ) ),
                new Config(), new SystemOutLogging() );
    }

    public StoreMigrator( MigrationProgressMonitor progressMonitor, UpgradableDatabase upgradableDatabase,
            Config config, Logging logging )
    {
        this.progressMonitor = progressMonitor;
        this.upgradableDatabase = upgradableDatabase;
        this.config = config;
        this.logging = logging;
    }

    @Override
    public boolean needsMigration( FileSystemAbstraction fileSystem, File storeDir ) throws IOException
    {
        NeoStoreUtil neoStoreUtil = new NeoStoreUtil( storeDir, fileSystem );
        String versionAsString = NeoStore.versionLongToString( neoStoreUtil.getStoreVersion() );
        boolean sameVersion = CommonAbstractStore.ALL_STORES_VERSION.equals( versionAsString );
        if ( !sameVersion )
        {
            upgradableDatabase.checkUpgradeable( storeDir );
        }
        return !sameVersion;
    }

    /**
     * Will detect which version we're upgrading from. Also initialize some legacy objects based on that version.
     * Doing that initialization here is good because we do this check when
     * {@link #moveMigratedFiles(FileSystemAbstraction, File, File) moving migrated files}, which might be done
     * as part of a resumed migration, i.e. run even if {@link #migrate(FileSystemAbstraction, File, File, DependencyResolver)}
     * hasn't been run.
     */
    private String versionToUpgradeFrom( FileSystemAbstraction fileSystem, File storeDir )
    {
        if ( versionToUpgradeFrom == null )
        {
            versionToUpgradeFrom = upgradableDatabase.checkUpgradeable( storeDir );
            final LogEntryWriterv1 logEntryWriter = new LogEntryWriterv1();
            logEntryWriter.setCommandWriter( new PhysicalLogNeoXaCommandWriter() );
            final LogEntryWriterv1 luceneLogEntryWriter = new LogEntryWriterv1();
            luceneLogEntryWriter.setCommandWriter( LegacyLuceneCommandReader.newWriter() );
            if ( versionToUpgradeFrom.equals( Legacy19Store.LEGACY_VERSION ) )
            {
                final LegacyLogIoUtil logIoUtil = new Legacy19LogIoUtil( new Legacy19CommandReader() );
                final LegacyLogIoUtil luceneLogIoUtil = new Legacy19LogIoUtil( LegacyLuceneCommandReader.newReader() );
                legacyLogFiles = new LegacyLogFiles( fileSystem, logEntryWriter, luceneLogEntryWriter,
                        logIoUtil, luceneLogIoUtil );
            }
            else
            {
                final LegacyLogIoUtil logIoUtil = new Legacy20LogIoUtil( new Legacy20CommandReader() );
                final LegacyLogIoUtil luceneLogIoUtil = new Legacy20LogIoUtil( LegacyLuceneCommandReader.newReader() );
                legacyLogFiles = new LegacyLogFiles( fileSystem, logEntryWriter, luceneLogEntryWriter,
                        logIoUtil, luceneLogIoUtil );
            }
        }
        return versionToUpgradeFrom;
    }

    @Override
    public void migrate( FileSystemAbstraction fileSystem, File storeDir, File migrationDir,
            DependencyResolver dependencyResolver ) throws IOException
    {
        progressMonitor.started();

        if ( versionToUpgradeFrom( fileSystem, storeDir ).equals( Legacy19Store.LEGACY_VERSION ) )
        {
            legacyStore = new Legacy19Store( fileSystem, new File( storeDir, NeoStore.DEFAULT_NAME ) );
        }
        else
        {
            legacyStore = new Legacy20Store( fileSystem, new File( storeDir, NeoStore.DEFAULT_NAME ) );
        }

        ExecutionMonitor executionMonitor = new CoarseBoundedProgressExecutionMonitor(
                legacyStore.getNodeStoreReader().getMaxId(), legacyStore.getRelStoreReader().getMaxId() )
        {
            @Override
            protected void percent( int percent )
            {
                progressMonitor.percentComplete( percent );
            }
        };
        BatchImporter importer = new ParallelBatchImporter( migrationDir.getAbsolutePath(), fileSystem,
                new Configuration.OverrideFromConfig( config ), logging, executionMonitor );
        Iterable<InputNode> nodes = legacyNodesAsInput( legacyStore );
        Iterable<InputRelationship> relationships = legacyRelationshipsAsInput( legacyStore );
        importer.doImport( nodes, relationships, IdMappings.actual() );
        progressMonitor.finished();

        // Finish the import of nodes and relationships
        importer.shutdown();
        if ( legacyStore instanceof Legacy19Store )
        {
            Legacy19Store legacy19Store = (Legacy19Store) legacyStore;

            // we need may need to upgrade the property keys
            PropertyStore propertyStore = storeFactory( fileSystem, migrationDir )
                    .newPropertyStore( new File( migrationDir.getPath(),
                                                 NeoStore.DEFAULT_NAME + StoreFactory.PROPERTY_STORE_NAME ) );
            try
            {
                migratePropertyKeys( legacy19Store, propertyStore );
            }
            finally
            {
                propertyStore.close();
            }

        }

        legacyLogFiles.migrateNeoLogs( fileSystem, migrationDir, storeDir );
        legacyLogFiles.migrateLuceneLogs( fileSystem, migrationDir, storeDir );
        // Close
        legacyStore.close();
    }

    private StoreFactory storeFactory( FileSystemAbstraction fileSystem, File migrationDir )
    {
        return new StoreFactory(
                StoreFactory.configForNeoStore( config, new File( migrationDir, NeoStore.DEFAULT_NAME ) ),
                new DefaultIdGeneratorFactory(), new DefaultWindowPoolFactory(), fileSystem, StringLogger.DEV_NULL,
                new DefaultTxHook() );
    }

    private void migratePropertyKeys( Legacy19Store legacyStore, PropertyStore propertyStore ) throws IOException
    {
        Token[] tokens = legacyStore.getPropertyIndexReader().readTokens();

        // dedup and write new property key token store (incl. names)
        Map<Integer, Integer> propertyKeyTranslation = dedupAndWritePropertyKeyTokenStore( propertyStore, tokens );

        // read property store, replace property key ids
        migratePropertyStore( legacyStore, propertyKeyTranslation, propertyStore );
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
            for ( PropertyBlock block : propertyRecord.getPropertyBlocks() )
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

    private StoreFile20[] allExcept( StoreFile20... exceptions )
    {
        List<StoreFile20> result = new ArrayList<>();
        result.addAll( Arrays.asList( StoreFile20.values() ) );
        for ( StoreFile20 except : exceptions )
        {
            result.remove( except );
        }
        return result.toArray( new StoreFile20[result.size()] );
    }

    private Iterable<InputRelationship> legacyRelationshipsAsInput( LegacyStore legacyStore )
    {
        final LegacyRelationshipStoreReader reader = legacyStore.getRelStoreReader();
        return new Iterable<InputRelationship>()
        {
            @Override
            public Iterator<InputRelationship> iterator()
            {
                Iterator<RelationshipRecord> source;
                try
                {
                    source = reader.iterator( 0 );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }

                return new IteratorWrapper<InputRelationship, RelationshipRecord>( source )
                {
                    @Override
                    protected InputRelationship underlyingObjectToObject( RelationshipRecord record )
                    {
                        return new InputRelationship( record.getId(), NO_PROPERTIES, record.getNextProp(),
                                record.getFirstNode(), record.getSecondNode(), null, record.getType() );
                    }
                };
            }
        };
    }

    private Iterable<InputNode> legacyNodesAsInput( LegacyStore legacyStore )
    {
        final LegacyNodeStoreReader reader = legacyStore.getNodeStoreReader();
        final String[] NO_LABELS = new String[0];
        return new Iterable<InputNode>()
        {
            @Override
            public Iterator<InputNode> iterator()
            {
                Iterator<NodeRecord> source;
                try
                {
                    source = reader.iterator();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }

                return new IteratorWrapper<InputNode, NodeRecord>( source )
                {
                    @Override
                    protected InputNode underlyingObjectToObject( NodeRecord record )
                    {
                        return new InputNode( record.getId(), NO_PROPERTIES, record.getNextProp(),
                                NO_LABELS, record.getLabelField() );
                    }
                };
            }
        };
    }

    @Override
    public void moveMigratedFiles( FileSystemAbstraction fs, File migrationDir, File storeDir ) throws IOException
    {
        // The batch importer will create a whole store. so
        // Disregard the new and empty node/relationship".id" files, i.e. reuse the existing id files
        StoreFile20.deleteIdFile( fs, migrationDir, allExcept( StoreFile20.RELATIONSHIP_GROUP_STORE ) );

        Iterable<StoreFile20> filesToMove;
        if ( versionToUpgradeFrom( fs, storeDir ).equals( Legacy19Store.LEGACY_VERSION ) )
        {
            filesToMove = Arrays.asList(
                    StoreFile20.NODE_STORE,
                    StoreFile20.RELATIONSHIP_STORE,
                    StoreFile20.RELATIONSHIP_GROUP_STORE,
                    StoreFile20.LABEL_TOKEN_STORE,
                    StoreFile20.NODE_LABEL_STORE,
                    StoreFile20.LABEL_TOKEN_NAMES_STORE,
                    StoreFile20.PROPERTY_STORE,
                    StoreFile20.PROPERTY_KEY_TOKEN_STORE,
                    StoreFile20.PROPERTY_KEY_TOKEN_NAMES_STORE,
                    StoreFile20.SCHEMA_STORE
            );
        }
        else
        {
            // Note: We don't overwrite the label stores in 2.0
            filesToMove = Arrays.asList(
                    StoreFile20.NODE_STORE,
                    StoreFile20.RELATIONSHIP_STORE,
                    StoreFile20.RELATIONSHIP_GROUP_STORE);
        }

        // Move the migrated ones into the store directory
        StoreFile20.move( fs, migrationDir, storeDir, filesToMove,
                true, // allow to skip non existent source files
                true, // allow to overwrite target files
                StoreFileType.values() );

        StoreFile20.ensureStoreVersion( fs, storeDir, StoreFile20.currentStoreFiles() );

        legacyLogFiles.moveRewrittenNeoLogs( migrationDir, storeDir );
        legacyLogFiles.moveRewrittenLuceneLogs( migrationDir, storeDir );
    }

    @Override
    public void cleanup( FileSystemAbstraction fileSystem, File migrationDir ) throws IOException
    {
        fileSystem.deleteRecursively( migrationDir );
    }

    @Override
    public String toString()
    {
        return "Kernel StoreMigrator";
    }
}
