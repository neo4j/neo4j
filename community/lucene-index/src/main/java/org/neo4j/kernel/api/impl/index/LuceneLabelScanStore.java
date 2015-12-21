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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
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
import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.impl.index.storage.IndexStorage;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider.FullStoreChangeStream;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

public class LuceneLabelScanStore
        implements LabelScanStore, LabelScanStorageStrategy.StorageService
{
    private final LabelScanStorageStrategy strategy;
    private final IndexWriterFactory<ObsoleteLuceneIndexWriter> writerFactory;
    // We get in a full store stream here in case we need to fully rebuild the store if it's missing or corrupted.
    private final FullStoreChangeStream fullStoreStream;
    private final Monitor monitor;
    private final IndexStorage indexStorage;
    private SearcherManager searcherManager;
    private ObsoleteLuceneIndexWriter writer;
    private boolean needsRebuild;
    private final Lock lock = new ReentrantLock( true );
    private Directory directory;

    public interface Monitor
    {
        void init();

        void noIndex();

        void lockedIndex( LockObtainFailedException e );

        void corruptIndex( IOException e );

        void rebuilding();

        void rebuilt( long roughNodeCount );
    }

    public static Monitor loggerMonitor( LogProvider logProvider )
    {
        final Log log = logProvider.getLog( LuceneLabelScanStore.class );
        return new Monitor()
        {
            @Override
            public void init()
            {   // Don't log anything here
            }

            @Override
            public void noIndex()
            {
                log.info( "No lucene scan store index found, this might just be first use. " +
                             "Preparing to rebuild." );
            }

            @Override
            public void lockedIndex( LockObtainFailedException e )
            {
                log.warn( "Index is locked by another process or database", e );
            }

            @Override
            public void corruptIndex( IOException corruptionException )
            {
                log.warn( "Lucene scan store index could not be read. Preparing to rebuild.",
                        corruptionException );
            }

            @Override
            public void rebuilding()
            {
                log.info( "Rebuilding lucene scan store, this may take a while" );
            }

            @Override
            public void rebuilt( long highNodeId )
            {
                log.info( "Lucene scan store rebuilt (roughly " + highNodeId + " nodes)" );
            }
        };
    }

    public LuceneLabelScanStore( LabelScanStorageStrategy strategy, IndexStorage indexStorage,
            IndexWriterFactory<ObsoleteLuceneIndexWriter> writerFactory,
            FullStoreChangeStream fullStoreStream, Monitor monitor )
    {
        this.strategy = strategy;
        this.indexStorage = indexStorage;
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
    public void updateDocument( Term documentTerm, Document document ) throws IOException
    {
        writer.updateDocument( documentTerm, document );
    }

    @Override
    public IndexSearcher acquireSearcher()
    {
        try
        {
            return searcherManager.acquire();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
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
        return new LuceneSnapshotter().snapshot( indexStorage.getIndexFolder(), writer );
    }

    @Override
    public void init() throws IOException
    {
        monitor.init();

        try
        {
            directory = indexStorage.openDirectory( indexStorage.getIndexFolder() );
            // TODO: NOT sure that we still need it?
            DirectoryReader.open( indexStorage.getIndexDirectory() ).close();

            writer = writerFactory.create( indexStorage.getIndexDirectory() );
        }
        catch ( IndexNotFoundException e )
        {
            // No index present, create one
            monitor.noIndex();
            prepareRebuildOfIndex();
            writer = writerFactory.create( indexStorage.getIndexDirectory() );
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
                    indexStorage.getIndexFolder() + "', and then start the database again." );
        }
        searcherManager = writer.createSearcherManager();
    }

    @Override
    public void start() throws IOException
    {
        if ( needsRebuild )
        {   // we saw in init() that we need to rebuild the index, so do it here after the
            // neostore has been properly started.
            monitor.rebuilding();
            long numberOfNodes = rebuild();
            monitor.rebuilt( numberOfNodes );
            needsRebuild = false;
        }
    }

    private long rebuild() throws IOException
    {
        try ( LabelScanWriter writer = newWriter() )
        {
            return fullStoreStream.applyTo( writer );
        }
    }

    @Override
    public void stop()
    {   // Not needed
    }

    @Override
    public void shutdown() throws IOException
    {
        if ( searcherManager != null )
        {   // In case something went wrong in init then the state of things might be off
            searcherManager.close();
            searcherManager = null;
        }
        if ( writer != null )
        {
            writer.close();
            writer = null;
        }
        directory.close();
    }

    @Override
    public LabelScanWriter newWriter()
    {
        // Only a single writer is allowed at any point in time. For that this lock is used and passed
        // onto the writer to release in its close()
        lock.lock();
        return strategy.acquireWriter( this, lock );
    }

    private void prepareRebuildOfIndex() throws IOException
    {
        indexStorage.cleanupFolder(indexStorage.getIndexFolder());
        indexStorage.prepareFolder( indexStorage.getIndexFolder() );
        needsRebuild = true;
    }
}
