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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider.FullStoreChangeStream;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

public class LuceneLabelScanStore
        implements LabelScanStore, LabelScanStorageStrategy.StorageService
{
    private final LabelScanStorageStrategy strategy;
    private final DirectoryFactory directoryFactory;
    private final IndexWriterFactory<LuceneIndexWriter> writerFactory;
    // We get in a full store stream here in case we need to fully rebuild the store if it's missing or corrupted.
    private final FullStoreChangeStream fullStoreStream;
    private final Monitor monitor;
    private Directory directory;
    private SearcherManager searcherManager;
    private LuceneIndexWriter writer;
    private boolean needsRebuild;
    private final File directoryLocation;
    private final FileSystemAbstraction fs;
    private final Lock lock = new ReentrantLock( true );

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
                logger.warn( "Lucene scan store index could not be read. Preparing to rebuild.",
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

    public LuceneLabelScanStore( LabelScanStorageStrategy strategy, DirectoryFactory directoryFactory,
            File directoryLocation, FileSystemAbstraction fs, IndexWriterFactory<LuceneIndexWriter> writerFactory,
            FullStoreChangeStream fullStoreStream, Monitor monitor )
    {
        this.strategy = strategy;
        this.directoryFactory = directoryFactory;
        this.directoryLocation = directoryLocation;
        this.fs = fs;
        this.writerFactory = writerFactory;
        this.fullStoreStream = fullStoreStream;
        this.monitor = monitor;
    }

    @Override
    public void deleteDocuments( Term documentTerm ) throws IOException
    {
        writer.deleteDocuments( documentTerm );
    }

    @Override
    public void updateDocument( Term documentTerm, Document document )
            throws IOException, IndexCapacityExceededException
    {
        writer.updateDocument( documentTerm, document );
    }

    @Override
    public IndexSearcher acquireSearcher()
    {
        return searcherManager.acquire();
    }

    @Override
    public void refreshSearcher() throws IOException
    {
        searcherManager.maybeRefresh();
    }

    @Override
    public void releaseSearcher( IndexSearcher searcher ) throws IOException
    {
        searcherManager.release( searcher );
    }

    @Override
    public AllEntriesLabelScanReader newAllEntriesReader()
    {
        return strategy.newNodeLabelReader( searcherManager );
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
        final IndexSearcher searcher = acquireSearcher();
        return new LabelScanReader()
        {
            @Override
            public PrimitiveLongIterator nodesWithLabel( int labelId )
            {
                return strategy.nodesWithLabel( searcher, labelId );
            }

            @Override
            public void close()
            {
                try
                {
                    releaseSearcher( searcher );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public Iterator<Long> labelsForNode( long nodeId )
            {
                return strategy.labelsForNode(searcher, nodeId);
            }
        };
    }

    @Override
    public ResourceIterator<File> snapshotStoreFiles() throws IOException
    {
        return new LuceneSnapshotter().snapshot( directoryLocation, writer );
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
            throw new IOException( "Label scan store could not be read, and needs to be rebuilt. " +
                    "To trigger a rebuild, ensure the database is stopped, delete the files in '" +
                    directoryLocation.getAbsolutePath() + "', and then start the database again." );
        }
        searcherManager = writer.createSearcherManager();
    }

    @Override
    public void start() throws IOException, IndexCapacityExceededException
    {
        if ( needsRebuild )
        {   // we saw in init() that we need to rebuild the index, so do it here after the
            // neostore has been properly started.
            monitor.rebuilding();
            write( fullStoreStream.iterator() );
            monitor.rebuilt( fullStoreStream.highestNodeId() );
            needsRebuild = false;
        }
    }

    private void write( Iterator<NodeLabelUpdate> updates ) throws IOException, IndexCapacityExceededException
    {
        try ( LabelScanWriter writer = newWriter() )
        {
            while ( updates.hasNext() )
            {
                writer.write( updates.next() );
            }
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

    @Override
    public LabelScanWriter newWriter()
    {
        // Only a single writer is allowed at any point in time. For that this lock is used and passed
        // onto the writer to release in its close()
        lock.lock();
        return strategy.acquireWriter( this, lock );
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
