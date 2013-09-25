/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.Iterator;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.index.impl.lucene.Hits;
import org.neo4j.kernel.api.scan.LabelScanReader;
import org.neo4j.kernel.api.scan.LabelScanStore;
import org.neo4j.kernel.api.scan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.PrimitiveLongIterator;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider.FullStoreChangeStream;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;

/**
 * {@link LabelScanStore} implemented using Lucene. There's only one big index for all labels because
 * the Lucene document structure handles that quite efficiently. It's as follows (pseudo field keys):
 *
 * { // document for node 1
 *     id: 1
 *     label: 4
 *     label: 2
 *     label: 56
 * }
 * { // document for node 2
 *     id: 2
 *     label: 4
 * }
 */
public class LuceneLabelScanStore implements LabelScanStore
{
    private static final String LABEL_FIELD_IDENTIFIER = "label";
    private final LuceneDocumentStructure documentStructure;
    private final DirectoryFactory directoryFactory;
    private final LuceneIndexWriterFactory writerFactory;
    // We get in a full store stream here in case we need to fully rebuild the store if it's missing or corrupted.
    private final FullStoreChangeStream fullStoreStream;
    private final Monitor monitor;
    private Directory directory;
    private SearcherManager searcherManager;
    private IndexWriter writer;
    private boolean needsRebuild;
    private final File directoryLocation;
    private final FileSystemAbstraction fs;

    public interface Monitor
    {
        void init();

        void noIndex();

        void lockedIndex( LockObtainFailedException e );

        void corruptIndex( IOException e );

        void rebuilding();

        void rebuilt( long roughNodeCount );
    }

    public static Monitor loggerMonitor( Logging logging )
    {
        final StringLogger logger = logging.getMessagesLog( LuceneLabelScanStore.class );
        return new Monitor()
        {
            @Override
            public void init()
            {   // Don't log anything here
            }

            @Override
            public void noIndex()
            {
                logger.info( "No lucene scan store index found, this might just be first use. " +
                        "Preparing to rebuild." );
            }

            @Override
            public void lockedIndex( LockObtainFailedException e )
            {
                logger.warn( "Index is locked by another process or database", e );
            }

            @Override
            public void corruptIndex( IOException corruptionException )
            {
                logger.warn( "Corrupt lucene scan store index found. Preparing to rebuild.",
                        corruptionException );
            }

            @Override
            public void rebuilding()
            {
                logger.info( "Rebuilding lucene scan store, this may take a while" );
            }

            @Override
            public void rebuilt( long highNodeId )
            {
                logger.info( "Lucene scan store rebuilt (roughly " + highNodeId + " nodes)" );
            }
        };
    }

    public LuceneLabelScanStore( LuceneDocumentStructure luceneDocumentStructure, DirectoryFactory directoryFactory,
            File directoryLocation, FileSystemAbstraction fs, LuceneIndexWriterFactory writerFactory,
            FullStoreChangeStream fullStoreStream, Monitor monitor )
    {
        this.documentStructure = luceneDocumentStructure;
        this.directoryFactory = directoryFactory;
        this.directoryLocation = directoryLocation;
        this.fs = fs;
        this.writerFactory = writerFactory;
        this.fullStoreStream = fullStoreStream;
        this.monitor = monitor;
    }

    @Override
    public void updateAndCommit( Iterator<NodeLabelUpdate> updates ) throws IOException
    {
        while ( updates.hasNext() )
        {
            NodeLabelUpdate update = updates.next();
            Term documentTerm = documentStructure.newQueryForChangeOrRemove( update.getNodeId() );
            if ( update.getLabelsAfter().length > 0 )
            {
                // Delete any existing document for this node and index the current set of labels
                Document document = documentStructure.newDocument( update.getNodeId() );
                for ( long label : update.getLabelsAfter() )
                {
                    document.add( documentStructure.newField( LABEL_FIELD_IDENTIFIER, label ) );
                }
                writer.updateDocument( documentTerm, document );
            }
            else
            {
                // Delete the document for this node from the index
                writer.deleteDocuments( documentTerm );
            }
        }
        searcherManager.maybeRefresh();
    }

    @Override
    public void recover( Iterator<NodeLabelUpdate> updates ) throws IOException
    {
        // The way we update and commit fits for recovery as well since we use writer.updateDocument(...)
        // which deletes any existing documents and just adds the new and up-to-date version.
        updateAndCommit( updates );
    }

    @Override
    public void force()
    {
        try
        {
            writer.commit();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public LabelScanReader newReader()
    {
        final IndexSearcher searcher = searcherManager.acquire();
        return new LabelScanReader()
        {
            @Override
            public PrimitiveLongIterator nodesWithLabel( int labelId )
            {
                try
                {
                    Hits hits = new Hits( searcher,
                            documentStructure.newQuery( LABEL_FIELD_IDENTIFIER, labelId ), null );
                    return new HitsPrimitiveLongIterator( hits, documentStructure );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public void close()
            {
                try
                {
                    searcherManager.release( searcher );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }

    @Override
    public ResourceIterator<File> snapshotStoreFiles() throws IOException
    {
        SnapshotDeletionPolicy deletionPolicy = (SnapshotDeletionPolicy) writer.getConfig().getIndexDeletionPolicy();
        return new StoreSnapshot( deletionPolicy );
    }

    private class StoreSnapshot extends PrefetchingIterator<File> implements ResourceIterator<File>
    {
        private final String ID = "backup";
        private final SnapshotDeletionPolicy deletionPolicy;
        private final IndexCommit commit;
        private final Iterator<String> fileNames;

        StoreSnapshot( SnapshotDeletionPolicy deletionPolicy ) throws IOException
        {
            this.deletionPolicy = deletionPolicy;
            this.commit = deletionPolicy.snapshot( ID );
            this.fileNames = commit.getFileNames().iterator();
        }

        @Override
        protected File fetchNextOrNull()
        {
            if ( !fileNames.hasNext() )
            {
                return null;
            }
            return new File( directoryLocation, fileNames.next() );
        }

        @Override
        public void close()
        {
            try
            {
                deletionPolicy.release( ID );
            }
            catch ( IOException e )
            {
                // TODO What to do here?
                throw new RuntimeException( "Unable to close lucene index snapshot", e );
            }
        }
    }

    @Override
    public void init() throws IOException
    {
        monitor.init();
        directory = directoryFactory.open( directoryLocation );
        if ( !indexExists() )
        {   // This is the first time we start up this scan store, prepare to rebuild from scratch later.
            monitor.noIndex();
            prepareRebuildOfIndex();
        }

        try
        {
            // Try to open it, this will throw exception if index is corrupt.
            // Opening it directly using the writer may hide corruption problems.
            IndexReader.open( directory ).close();

            writer = writerFactory.create( directory );
        }
        catch ( IndexNotFoundException e )
        {
            // No index present, create one
            monitor.noIndex();
            prepareRebuildOfIndex();
            writer = writerFactory.create( directory );
        }
        catch ( LockObtainFailedException e )
        {
            monitor.lockedIndex( e );
            throw e;
        }
        catch( IOException e )
        {
            // The index was somehow corrupted, fail
            monitor.corruptIndex( e );
            throw new IOException( "Label scan store is corrupted, and needs to be rebuilt. " +
                    "To trigger a rebuild, ensure the database is stopped, delete the files in '" +
                    directoryLocation.getAbsolutePath() + "', and then start the database again." );
        }
        searcherManager = new SearcherManager( writer, true, new SearcherFactory() );
    }

    @Override
    public void start() throws IOException
    {
        if ( needsRebuild )
        {   // we saw in init() that we need to rebuild the index, so do it here after the
            // neostore has been properly started.
            monitor.rebuilding();
            updateAndCommit( fullStoreStream.iterator() );
            monitor.rebuilt( fullStoreStream.highestNodeId() );
            needsRebuild = false;
        }
    }

    @Override
    public void stop()
    {   // Not needed
    }

    @Override
    public void shutdown() throws IOException
    {
        searcherManager.close();
        writer.close( true );
        directory.close();
        directory = null;
    }

    private boolean indexExists()
    {
        if ( !fs.fileExists( directoryLocation ) )
        {
            return false;
        }
        File[] files = fs.listFiles( directoryLocation );
        return files != null && files.length > 0;
    }

    private void prepareRebuildOfIndex() throws IOException
    {
        directory.close();
        fs.deleteRecursively( directoryLocation );
        fs.mkdirs( directoryLocation );
        needsRebuild = true;
        directory = directoryFactory.open( directoryLocation );
    }
}
