/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.CountsComputer;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyKeyTokenStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
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
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappings;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.unsafe.impl.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;

import static org.neo4j.helpers.UTF8.encode;
import static org.neo4j.helpers.collection.Iterables.iterable;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.IteratorUtil.loop;
import static org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.store.StoreFactory.buildTypeDescriptorAndVersion;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

/**
 * Migrates a neo4j kernel database from one version to the next.
 * <p/>
 * Since only one store migration is supported at any given version (migration from the previous store version)
 * the migration code is specific for the current upgrade and changes with each store format version.
 * <p/>
 * Just one out of many potential participants in a {@link StoreUpgrader migration}.
 *
 * @see StoreUpgrader
 */
public class StoreMigrator implements StoreMigrationParticipant
{
    private static final Object[] NO_PROPERTIES = new Object[0];

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

    public StoreMigrator( MigrationProgressMonitor progressMonitor, FileSystemAbstraction fileSystem, Logging logging )
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
     * Will detect which version we're upgrading from.
     * Doing that initialization here is good because we do this check when
     * {@link #moveMigratedFiles(File, File) moving migrated files}, which might be done
     * as part of a resumed migration, i.e. run even if {@link #migrate(File, File)}
     * hasn't been run.
     */
    private String versionToUpgradeFrom( FileSystemAbstraction fileSystem, File storeDir )
    {
        if ( versionToUpgradeFrom == null )
        {
            versionToUpgradeFrom = upgradableDatabase.checkUpgradeable( storeDir );
        }
        return versionToUpgradeFrom;
    }

    @Override
    public void migrate( File storeDir, File migrationDir ) throws IOException
    {
        progressMonitor.started();

        long lastTxId = NeoStore.getTxId( fileSystem, new File( storeDir, NeoStore.DEFAULT_NAME ) );

        if ( versionToUpgradeFrom( fileSystem, storeDir ).equals( Legacy21Store.LEGACY_VERSION ) )
        {
            // ensure the stores have the new versions set before reading them to create a counts store
            ensureStoreVersions( storeDir );

            // create counters from scratch
            rebuildCountsFromScratch( storeDir, migrationDir, lastTxId );
        }
        else
        {
            // migrate stores
            migrateWithBatchImporter( storeDir, migrationDir );

            // create counters from scratch
            rebuildCountsFromScratch( migrationDir, migrationDir, lastTxId );
        }

        // migrate logs
        legacyLogs.migrateLogs( storeDir, migrationDir );

        progressMonitor.finished();
    }

    private void rebuildCountsFromScratch( File storeDir, File migrationDir, long lastTxId ) throws IOException
    {
        final LifeSupport life = new LifeSupport();
        life.start();
        try
        {
            final PageCache pageCache = createPageCache( fileSystem, "build-counts", life );
            final File storeFileBase = new File( migrationDir, NeoStore.DEFAULT_NAME + StoreFactory.COUNTS_STORE );
            CountsTracker.createEmptyCountsStore( pageCache, storeFileBase,
                    buildTypeDescriptorAndVersion( CountsTracker.STORE_DESCRIPTOR ) );

            final StoreFactory storeFactory =
                    new StoreFactory( fileSystem, storeDir, pageCache, DEV_NULL, new Monitors() );
            try ( NodeStore nodeStore = storeFactory.newNodeStore();
                  RelationshipStore relationshipStore = storeFactory.newRelationshipStore();
                  CountsTracker tracker = new CountsTracker( fileSystem, pageCache, storeFileBase ) )
            {
                CountsComputer.computeCounts( nodeStore, relationshipStore ).
                        accept( new CountsAccessor.Initializer( tracker ) );
                tracker.rotate( lastTxId );
            }
        }
        finally
        {
            life.shutdown();
        }
    }

    private void migrateWithBatchImporter( File storeDir, File migrationDir )
            throws IOException
    {
        LegacyStore legacyStore;
        switch ( versionToUpgradeFrom )
        {
            case Legacy19Store.LEGACY_VERSION:
                legacyStore = new Legacy19Store( fileSystem, new File( storeDir, NeoStore.DEFAULT_NAME ) );
                break;
            case Legacy20Store.LEGACY_VERSION:
                legacyStore = new Legacy20Store( fileSystem, new File( storeDir, NeoStore.DEFAULT_NAME ) );
                break;
            default:
                throw new IllegalStateException( "Unknown version to upgrade from: " + versionToUpgradeFrom );
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
                new Configuration.OverrideFromConfig( config ), logging,
                executionMonitor );
        Iterable<InputNode> nodes = legacyNodesAsInput( legacyStore );
        Iterable<InputRelationship> relationships = legacyRelationshipsAsInput( legacyStore );
        importer.doImport( Inputs.input( nodes, relationships, IdMappings.actual() ) );
        progressMonitor.finished();

        // Finish the import of nodes and relationships
        if ( legacyStore instanceof Legacy19Store )
        {
            // we may need to upgrade the property keys
            Legacy19Store legacy19Store = (Legacy19Store) legacyStore;
            LifeSupport life = new LifeSupport();
            life.start();
            PageCache pageCache = createPageCache( fileSystem, "migrator-dedup-properties", life );
            try ( PropertyStore propertyStore = storeFactory( pageCache, migrationDir ).newPropertyStore() )
            {
                migratePropertyKeys( legacy19Store, propertyStore );
            }
            finally
            {
                life.shutdown();
            }
        }
        // Close
        legacyStore.close();
    }

    private StoreFactory storeFactory( PageCache pageCache, File migrationDir )
    {
        return new StoreFactory(
                StoreFactory.configForStoreDir( config, migrationDir ),
                new DefaultIdGeneratorFactory(), pageCache,
                fileSystem, DEV_NULL, new Monitors() );
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
    public void moveMigratedFiles( File migrationDir, File storeDir ) throws IOException
    {
        // The batch importer will create a whole store. so
        // Disregard the new and empty node/relationship".id" files, i.e. reuse the existing id files

        Iterable<StoreFile> filesToMove;
        StoreFile[] idFilesToDelete;
        switch ( versionToUpgradeFrom( fileSystem, storeDir ) )
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
                        StoreFile.COUNTS_STORE_ALPHA,
                        StoreFile.COUNTS_STORE_BETA );
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
                        StoreFile.COUNTS_STORE_ALPHA,
                        StoreFile.COUNTS_STORE_BETA );
                idFilesToDelete = allExcept(
                        StoreFile.RELATIONSHIP_GROUP_STORE
                );
                break;
            case Legacy21Store.LEGACY_VERSION:
                filesToMove = Arrays.asList(
                        StoreFile.COUNTS_STORE_ALPHA,
                        StoreFile.COUNTS_STORE_BETA );
                idFilesToDelete = new StoreFile[]{};
                break;
            default:
                throw new IllegalStateException( "Unknown version to upgrade from: " + versionToUpgradeFrom );
        }

        StoreFile.deleteIdFile( fileSystem, migrationDir, idFilesToDelete );

        // Move the migrated ones into the store directory
        StoreFile.move( fileSystem, migrationDir, storeDir, filesToMove,
                true, // allow to skip non existent source files
                true, // allow to overwrite target files
                StoreFileType.values() );

        // ensure the store version is correct
        if ( !versionToUpgradeFrom.equals( Legacy21Store.LEGACY_VERSION ) )
        {
            // we fix the store versions earlier on in order to create counts store for 2.1
            ensureStoreVersions( storeDir );
        }

        // update or add upgrade id and time
        updateOrAddUpgradeIdAndUpgradeTime( storeDir );

        // move logs
        legacyLogs.moveLogs( migrationDir, storeDir );
        legacyLogs.renameLogFiles( storeDir );
    }

    private void ensureStoreVersions( File dir ) throws IOException
    {
        final Iterable<StoreFile> versionedStores =
                iterable( allExcept( StoreFile.COUNTS_STORE_ALPHA, StoreFile.COUNTS_STORE_BETA ) );
        StoreFile.ensureStoreVersion( fileSystem, dir, versionedStores );
    }

    private void updateOrAddUpgradeIdAndUpgradeTime( File storeDirectory )
    {
        final File neostore = new File( storeDirectory, NeoStore.DEFAULT_NAME );
        NeoStore.setOrAddUpgradeIdOnMigration( fileSystem, neostore, new SecureRandom().nextLong() );
        NeoStore.setOrAddUpgradeTimeOnMigration( fileSystem, neostore, System.currentTimeMillis() );
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
