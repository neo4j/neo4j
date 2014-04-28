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
package org.neo4j.kernel.api.impl.index;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.impl.index.LuceneDocumentStructure.ValueEncoding;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.index.ProviderMeta;
import org.neo4j.kernel.api.index.ProviderMeta.Record;
import org.neo4j.kernel.api.index.util.FolderLayout;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;

import static org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider.CURRENT_VERSION;
import static org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProviderFactory.KEY;
import static org.neo4j.kernel.api.index.SchemaIndexProvider.getRootDirectory;

public class LuceneIndexMigrator extends StoreMigrationParticipant.Adapter
{
    public interface Monitor
    {
        void indexNeedsMigration( long indexId );

        void indexMigrated( long indexId, int numberOfEntriesUpdated );
    }

    static final Monitor NO_MONITOR = new Monitor()
    {
        @Override
        public void indexNeedsMigration( long indexId )
        {   // Do nothing
        }

        @Override
        public void indexMigrated( long indexId, int numberOfEntriesUpdated )
        {   // Do nothing
        }
    };

    private final File rootDirectory;
    private final FolderLayout folderLayout;
    private final DirectoryFactory directoryFactory;
    private final LuceneIndexWriterFactory writerFactory;
    private final LuceneDocumentStructure documentStructure;
    private final Monitor monitor;
    private final ProviderMeta meta;

    public LuceneIndexMigrator( File rootDirectory, FolderLayout folderLayout,
            DirectoryFactory directoryFactory, LuceneIndexWriterFactory writerFactory,
            LuceneDocumentStructure documentStructure, Monitor monitor, ProviderMeta meta )
    {
        this.rootDirectory = rootDirectory;
        this.folderLayout = folderLayout;
        this.directoryFactory = directoryFactory;
        this.writerFactory = writerFactory;
        this.documentStructure = documentStructure;
        this.monitor = monitor;
        this.meta = meta;
    }

    @Override
    public boolean needsMigration( FileSystemAbstraction fileSystem, File storeDir ) throws IOException
    {
        try
        {
            Record versionRecord = meta.getRecord( ProviderMeta.ID_VERSION );
            if ( versionRecord.getValue() > CURRENT_VERSION )
            {
                throw new RuntimeException( "Backward migration not possible" );
            }
            if ( versionRecord.getValue() < CURRENT_VERSION-1 )
            {
                throw new RuntimeException( "Only single version migration possible" );
            }
            boolean sameVersion = versionRecord.getValue() == CURRENT_VERSION;
            return !sameVersion;
        }
        catch ( InvalidRecordException e )
        {   // We don't have a version record, this signals that we do this for the first time, i.e. we're
            // moving from 2.0.x to 2.1.x
            return true;
        }
    }

    @Override
    public void migrate( FileSystemAbstraction fileSystem, File storeDir, File migrationDir,
            DependencyResolver dependencies ) throws IOException, UnsatisfiedDependencyException
    {
        /* Pseudo:
         *
         * for index in all lucene indexes
         *   query index for numeric values above POSITIVE_THRESHOLD or below NEGATIVE_THRESHOLD
         *   for doc in hits
         *     check if the property value for that node is long that won't coerce properly to a double
         *     if so, them copy the index to the targetSourceDir (somewhere) and break
         *   if we had to copy the index
         *     query index for numeric values above POSITIVE_THRESHOLD or below NEGATIVE_THRESHOLD
         *     for doc in hits
         *       check if the property value for that node is long that won't coerce properly to a double
         *       if so, then update the doc
         */

        SchemaCache schema = dependencies.resolveDependency( SchemaCache.class );
        PropertyAccessor propertyAccessor = dependencies.resolveDependency( PropertyAccessor.class );
        for ( File indexDirectory : rootDirectory.listFiles( folderLayout ) )
        {
            long indexId = folderLayout.getIndexId( indexDirectory );
            CheckingSuspectVisitor visitor = new CheckingSuspectVisitor();
            visitSuspects( indexDirectory, schema, propertyAccessor, visitor, indexId );
            if ( visitor.anySuspects )
            {   // This index needs migration, so copy it...
                monitor.indexNeedsMigration( indexId );
                File copyDir = copyIndex( fileSystem, indexDirectory, migrationDir );
                // ...and patch it
                int entriesUpdated = visitSuspects( copyDir, schema, propertyAccessor, new SuspectVisitor()
                {
                    @Override
                    public void suspect( IndexWriter writer, long nodeId, Object value ) throws IOException
                    {   // Patch this document
                        writer.updateDocument(
                                documentStructure.newQueryForChangeOrRemove( nodeId ),
                                documentStructure.newDocumentRepresentingProperty( nodeId, value ) );

                    }
                }, indexId );
                monitor.indexMigrated( indexId, entriesUpdated );
            }
        }
    }

    @Override
    public void moveMigratedFiles( FileSystemAbstraction fileSystem, File migrationDir, File storeDir,
            File leftOversDir ) throws IOException
    {
       /* Pseudo:
        *
        * for all migrated indexes
        *   delete the corresponding index in storeDir
        *   move the index into storeDir
        * set version in meta file to CURRENT_VERSION
        */

        File migratedIndexRootDir = getRootDirectory( migrationDir, LuceneSchemaIndexProviderFactory.KEY );
        File storeIndexRootDir = getRootDirectory( storeDir, LuceneSchemaIndexProviderFactory.KEY );
        for ( File indexDir : migratedIndexRootDir.listFiles( folderLayout ) )
        {
            File targetDirectory = new File( storeIndexRootDir, indexDir.getName() );
            fileSystem.deleteRecursively( targetDirectory );
            // copy the index into place, the migration files will be deleted in cleanup()
            fileSystem.copyRecursively( indexDir, targetDirectory );
        }

        meta.updateRecord( new Record( ProviderMeta.ID_VERSION, CURRENT_VERSION ) );
        meta.force();
    }

    @Override
    public String toString()
    {
        return "Lucene IndexMigrator";
    }

    private File copyIndex( FileSystemAbstraction fs, File indexDirectory, File migrationDir ) throws IOException
    {
        File targetDir = new File( getRootDirectory( migrationDir, KEY ), indexDirectory.getName() );
        fs.copyRecursively( indexDirectory, targetDir );
        return targetDir;
    }

    private int visitSuspects( File indexDirectory, SchemaCache schema, PropertyAccessor propertyAccessor,
            SuspectVisitor suspectVisitor, long indexId ) throws IOException
    {
        SuspectsCollector collector;
        try ( Directory directory = directoryFactory.open( indexDirectory );
              IndexWriter writer = writerFactory.create( directory );
              SearcherManager searcherManager = new SearcherManager( writer, false, new SearcherFactory() ) )
        {
            IndexSearcher searcher = searcherManager.acquire();
            try
            {
                IndexDescriptor descriptor = schema.indexDescriptor( indexId );
                collector = new SuspectsCollector( writer, documentStructure, propertyAccessor,
                        descriptor.getPropertyKeyId(), suspectVisitor );
                searcher.search( suspectQuery(), collector );
                return collector.count;
            }
            catch ( SchemaRuleNotFoundException e )
            {
                throw new ThisShouldNotHappenError( "Mattias", "Kernel doesn't think " + indexDirectory +
                        " is an index", e );
            }
            finally
            {
                searcherManager.release( searcher );
            }
        }
    }

    private Query suspectQuery()
    {
        BooleanQuery query = new BooleanQuery();
        query.add( new WildcardQuery( new Term( ValueEncoding.Number.key(), "*" ) ), Occur.SHOULD );
        query.add( new WildcardQuery( new Term( ValueEncoding.Array.key(), "*" ) ), Occur.SHOULD );
        return query;
    }

    private interface SuspectVisitor
    {
        void suspect( IndexWriter writer, long nodeId, Object value ) throws IOException;
    }

    private static class CheckingSuspectVisitor implements SuspectVisitor
    {
        boolean anySuspects;

        @Override
        public void suspect( IndexWriter writer, long nodeId, Object value ) throws IOException
        {
            anySuspects = true;
        }
    }

    private class SuspectsCollector extends NodeIdCollector
    {
        private final IndexWriter writer;
        private final PropertyAccessor propertyAccessor;
        private final int propertyKey;
        private final SuspectVisitor visitor;
        private int count;

        SuspectsCollector( IndexWriter writer, LuceneDocumentStructure documentStructure,
                PropertyAccessor propertyAccessor, int propertyKey, SuspectVisitor visitor )
        {
            super( documentStructure );
            this.writer = writer;
            this.propertyAccessor = propertyAccessor;
            this.propertyKey = propertyKey;
            this.visitor = visitor;
        }

        @Override
        protected void collectNodeId( long nodeId ) throws IOException
        {
            try
            {
                Object value = propertyAccessor.getProperty( nodeId, propertyKey ).value();
                if ( ValueEncoding.Long.canEncode( value ) ||
                     ValueEncoding.Long_Array.canEncode( value ) )
                {   // Document that needs patching
                    count++;
                    visitor.suspect( writer, nodeId, value );
                }
            }
            catch ( EntityNotFoundException | PropertyNotFoundException e )
            {
                // Strange, not found... this means that this index is inconsistent with the graph.
                throw new IOException( "Bad or inconsistent stores, index refers to missing entities", e );
            }
        }
    }
}
