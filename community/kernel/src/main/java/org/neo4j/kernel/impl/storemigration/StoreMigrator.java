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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreUtil;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.store.StoreVersionMismatchHandler;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreIndexStoreView;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyNodeStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyRelationshipStoreReader;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.util.DependencySatisfier;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.ParallellBatchImporter;
import org.neo4j.unsafe.impl.batchimport.cache.NodeIdMapping;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.CoarseBoundedProgressExecutionMonitor;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitor;

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

    // Dependency satisfaction
    private NodeStore dependencyNodeStore;
    private PropertyStore dependencyPropertyStore;

    // TODO progress meter should be an aspect of StoreUpgrader, not specific to this participant.

    public StoreMigrator( MigrationProgressMonitor progressMonitor, FileSystemAbstraction fileSystem )
    {
        this( progressMonitor, new UpgradableDatabase( new StoreVersionCheck( fileSystem ) ),
                new Config() );
    }

    public StoreMigrator( MigrationProgressMonitor progressMonitor, UpgradableDatabase upgradableDatabase,
            Config config )
    {
        this.progressMonitor = progressMonitor;
        this.upgradableDatabase = upgradableDatabase;
        this.config = config;
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

    @Override
    public void migrate( FileSystemAbstraction fileSystem, File storeDir, File migrationDir,
            DependencyResolver dependencyResolver ) throws IOException
    {
        LegacyStore legacyStore = new LegacyStore( fileSystem, new File( storeDir, NeoStore.DEFAULT_NAME ) );

        ExecutionMonitor executionMonitor = new CoarseBoundedProgressExecutionMonitor(
                legacyStore.getNodeStoreReader().getMaxId(), legacyStore.getRelStoreReader().getMaxId() )
        {
            @Override
            protected void percent( int percent )
            {
                progressMonitor.percentComplete( percent );
            }
        };
        BatchImporter importer = new ParallellBatchImporter( migrationDir.getAbsolutePath(), fileSystem,
                new Configuration.FromConfig( config ), Collections.<KernelExtensionFactory<?>>emptyList(),
                executionMonitor );
        progressMonitor.started();
        importer.doImport( legacyNodesAsInput( legacyStore ), legacyRelationshipsAsInput( legacyStore ),
                NodeIdMapping.actual );
        progressMonitor.finished();

        // Close
        importer.shutdown();
        legacyStore.close();
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
    public void moveMigratedFiles( FileSystemAbstraction fileSystem, File migrationDir,
            File storeDir, File leftOversDir ) throws IOException
    {
        // The batch importer will create a whole store. so TODO delete everything except nodestore, relstore, relgroupstore here
        // Disregard the new and empty node/relationship".id" files, i.e. reuse the existing id files
        StoreFile.deleteIdFile( fileSystem, migrationDir, allExcept( StoreFile.RELATIONSHIP_GROUP_STORE ) );
        StoreFile.deleteStoreFile( fileSystem, migrationDir, allExcept( StoreFile.NODE_STORE,
                StoreFile.RELATIONSHIP_STORE, StoreFile.RELATIONSHIP_GROUP_STORE ) );

        // Move the current ones into the leftovers directory
        StoreFile.move( fileSystem, storeDir, leftOversDir,
                IteratorUtil.<StoreFile>asIterable( StoreFile.NODE_STORE, StoreFile.RELATIONSHIP_STORE ),
                false, false, StoreFileType.STORE );

        // Move the migrated ones into the store directory
        StoreFile.move( fileSystem, migrationDir, storeDir, StoreFile.currentStoreFiles(),
                true,   // allow skip non existent source files
                true,   // allow overwrite target files
                StoreFileType.values() );
        StoreFile.ensureStoreVersion( fileSystem, storeDir, StoreFile.currentStoreFiles() );
//        LogFiles.move( fileSystem, storeDir, leftOversDir );
    }

    @Override
    public void satisfyDependenciesDownstream( FileSystemAbstraction fileSystem, File storeDir,
                                               File migrationDir, DependencySatisfier dependencySatisfier, boolean participatedInMigration )
    {
        // Schema cache (schema store not migrated, so grab the current one,
        // but with some flexibility in version checking)
        Config config = StoreFactory.readOnly( this.config );
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
        StoreFactory legacyStoreFactory = new StoreFactory( StoreFactory.configForStoreDir( config, storeDir ),
                idGeneratorFactory, new DefaultWindowPoolFactory(), fileSystem, StringLogger.DEV_NULL,
                new DefaultTxHook(), StoreVersionMismatchHandler.ACCEPT );
        SchemaStore schemaStore = legacyStoreFactory.newSchemaStore(
                new File( storeDir, NeoStore.DEFAULT_NAME + StoreFactory.SCHEMA_STORE_NAME ) );
        SchemaCache schemaCache = new SchemaCache( schemaStore );
        schemaStore.close();
        dependencySatisfier.satisfyDependency( SchemaCache.class, schemaCache );

        // PropertyAccessor
        StoreFactory storeFactory = new StoreFactory( StoreFactory.configForStoreDir( config, migrationDir ),
                idGeneratorFactory, new DefaultWindowPoolFactory(), fileSystem, StringLogger.DEV_NULL,
                new DefaultTxHook(), StoreVersionMismatchHandler.ACCEPT );
        dependencyNodeStore = (participatedInMigration ? storeFactory : legacyStoreFactory).newNodeStore(
                new File( participatedInMigration ? migrationDir : storeDir,
                        NeoStore.DEFAULT_NAME + StoreFactory.NODE_STORE_NAME ) );
        dependencyPropertyStore = legacyStoreFactory.newPropertyStore( new File( storeDir,
                NeoStore.DEFAULT_NAME + StoreFactory.PROPERTY_STORE_NAME ) );
        dependencySatisfier.satisfyDependency( PropertyAccessor.class,
                new NeoStoreIndexStoreView( LockService.NO_LOCK_SERVICE, dependencyNodeStore, dependencyPropertyStore ) );
    }

    @Override
    public void close()
    {
        for ( CommonAbstractStore store : new CommonAbstractStore[] { dependencyNodeStore, dependencyPropertyStore } )
        {
            if ( store != null )
            {
                store.close();
            }
        }
    }

    @Override
    public void cleanup( FileSystemAbstraction fileSystem, File migrationDir ) throws IOException
    {
        for ( StoreFile storeFile : StoreFile.values() )
        {
            fileSystem.deleteFile( new File( migrationDir, storeFile.storeFileName() ) );
            fileSystem.deleteFile( new File( migrationDir, storeFile.idFileName() ) );
        }
    }

    @Override
    public String toString()
    {
        return "Kernel StoreMigrator";
    }
}
