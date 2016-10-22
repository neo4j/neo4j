/*
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
package org.neo4j.index.impl.lucene.legacy;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexEntityType;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;
import org.neo4j.upgrade.lucene.LegacyIndexMigrationException;

import static org.neo4j.index.impl.lucene.legacy.LuceneUtil.containsIndexFiles;

/**
 * Legacy index field restorer migrator. Part of legacy index migration migration for stores that potentially can miss
 * sorted fields for document stored fields.
 * As result of restorer work new index will be created with documents that have exactly the same stored fields values
 * with corresponding (sorted) doc values fields.
 */
public class LegacyIndexFieldsRestorer
{
    static boolean SKIP_LEGACY_INDEX_FIELDS_MIGRATION =
            FeatureToggles.flag( LegacyIndexFieldsRestorer.class, "skip_migration", false );

    private final Log log;
    private final Config config;
    private final FileSystemAbstraction fileSystem;

    public LegacyIndexFieldsRestorer( Config config, FileSystemAbstraction fileSystem, LogProvider logProvider )
    {
        this.config = config;
        this.fileSystem = fileSystem;
        this.log = logProvider.getLog( getClass() );
    }

    public void restoreIndexSortFields( File indexConfigSourceDirectory, File storeDir, File migrationDir,
            MigrationProgressMonitor.Section progressMonitor ) throws IOException
    {
        if ( !SKIP_LEGACY_INDEX_FIELDS_MIGRATION &&
                isNotEmptyDirectory( LuceneDataSource.getLuceneIndexStoreDirectory( storeDir ) ) )
        {
            LifeSupport lifecycle = new LifeSupport();
            try
            {
                Config sourceConfig =
                        config.with( MapUtil.stringMap( GraphDatabaseSettings.read_only.name(), Settings.TRUE ) );
                IndexConfigStore configStore = new IndexConfigStore( indexConfigSourceDirectory, fileSystem );
                LuceneDataSource sourceDataSource =
                        new LuceneDataSource( storeDir, sourceConfig, configStore, fileSystem );
                LuceneDataSource targetDataSource =
                        new LuceneDataSource( migrationDir, this.config, configStore, fileSystem );

                startComponents( lifecycle, configStore, sourceDataSource, targetDataSource );

                String[] nodeIndexes = configStore.getNames( Node.class );
                String[] relationshipIndexes = configStore.getNames( Relationship.class );
                migrateNodeIndexes( storeDir, progressMonitor, configStore, sourceDataSource, targetDataSource,
                        nodeIndexes );
                migrateRelationhipIndexes( storeDir, progressMonitor, configStore, sourceDataSource, targetDataSource,
                        relationshipIndexes );
            }
            catch ( LegacyIndexMigrationException lime )
            {
                log.error( "Migration of legacy indexes failed. Index: " + lime.getFailedIndexName() + " can't be " +
                        "migrated.", lime );
                throw new IOException( "Legacy index migration failed.", lime );
            }
            finally
            {
                lifecycle.shutdown();
            }
        }
    }

    private void startComponents( LifeSupport lifecycle, Lifecycle... components )
    {
        for ( Lifecycle component : components )
        {
            lifecycle.add( component );
        }
        lifecycle.start();
    }

    private void migrateRelationhipIndexes( File storeDir, MigrationProgressMonitor.Section progressMonitor,
            IndexConfigStore configStore, LuceneDataSource sourceDataSource, LuceneDataSource targetDataSource,
            String[] relationshipIndexes ) throws IOException, LegacyIndexMigrationException
    {
        migrateIndexes( storeDir, progressMonitor, configStore, sourceDataSource, targetDataSource, relationshipIndexes,
                IndexEntityType.Relationship, Relationship.class );
    }

    private void migrateNodeIndexes( File storeDir, MigrationProgressMonitor.Section progressMonitor,
            IndexConfigStore configStore, LuceneDataSource sourceDataSource, LuceneDataSource targetDataSource,
            String[] nodeIndexes ) throws IOException, LegacyIndexMigrationException
    {
        migrateIndexes( storeDir, progressMonitor, configStore, sourceDataSource, targetDataSource, nodeIndexes,
                IndexEntityType.Node, Node.class );
    }

    void migrateIndexes( File storeDir, MigrationProgressMonitor.Section progressMonitor,
            IndexConfigStore configStore, LuceneDataSource sourceDataSource, LuceneDataSource targetDataSource,
            String[] indexes, IndexEntityType entityType, Class<? extends PropertyContainer> entityClass )
            throws IOException, LegacyIndexMigrationException
    {
        File indexStoreDirectory = LuceneDataSource.getLuceneIndexStoreDirectory( storeDir );
        for ( String indexName : indexes )
        {
            try
            {
                IndexIdentifier identifier = new IndexIdentifier( entityType, indexName );
                File indexDirectory = LuceneDataSource.getFileDirectory( indexStoreDirectory, identifier );
                if ( containsIndexFiles( fileSystem, indexDirectory ) )
                {
                    Map<String,String> indexConfig = configStore.get( entityClass, indexName );
                    performIndexMigration( sourceDataSource, targetDataSource, identifier, indexConfig );
                }
                reportMigrationProgress( progressMonitor );
            }
            catch ( Throwable e )
            {
                throw new LegacyIndexMigrationException( indexName, "Restoration of sort fields for " +
                        entityType.nameToLowerCase() + " index " + indexName + " failed.", e );
            }
        }
    }

    private void reportMigrationProgress( MigrationProgressMonitor.Section progressMonitor )
    {
        progressMonitor.progress( 1 );
    }

    private void performIndexMigration( LuceneDataSource sourceDataSource, LuceneDataSource targetDataSource,
            IndexIdentifier identifier, Map<String,String> indexConfig ) throws IOException
    {
        IndexReference indexSearcher = sourceDataSource.getIndexSearcher( identifier );
        IndexReference destinationIndexSearcher = targetDataSource.getIndexSearcher( identifier );
        try
        {
            IndexWriter writer = destinationIndexSearcher.getWriter();
            IndexReader indexReader = indexSearcher.getSearcher().getIndexReader();
            IndexType indexType = IndexType.getIndexType( indexConfig );

            List<LeafReaderContext> leaves = indexReader.leaves();
            for ( LeafReaderContext leaf : leaves )
            {
                LeafReader reader = leaf.reader();
                Bits liveDocs = reader.getLiveDocs();
                int maxDoc = reader.maxDoc();
                for ( int documentId = 0; documentId < maxDoc; documentId++ )
                {
                    if ( liveDocs == null || liveDocs.get( documentId ) )
                    {
                        Document document = reader.document( documentId );
                        indexType.restoreSortFields( document );
                        writer.addDocument( document );
                    }
                }
                writer.flush();
            }
        }
        finally
        {
            indexSearcher.close();
            destinationIndexSearcher.close();
        }
    }

    private boolean isNotEmptyDirectory( File file )
    {
        if ( fileSystem.isDirectory( file ) )
        {
            File[] files = fileSystem.listFiles( file );
            return files != null && files.length > 0;
        }
        return false;
    }
}
