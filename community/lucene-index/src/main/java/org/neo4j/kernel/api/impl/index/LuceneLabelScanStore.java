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

import org.apache.lucene.store.LockObtainFailedException;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider.FullStoreChangeStream;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.schema.LabelScanReader;

public class LuceneLabelScanStore implements LabelScanStore
{
    public static final String INDEX_IDENTIFIER = "labelStore";

    private final LabelScanStorageStrategy strategy;
    private final LuceneIndex luceneIndex;
    // We get in a full store stream here in case we need to fully rebuild the store if it's missing or corrupted.
    private final FullStoreChangeStream fullStoreStream;
    private final Monitor monitor;
    private boolean needsRebuild;
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

    // todo: WAT?
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

    public LuceneLabelScanStore( LabelScanStorageStrategy strategy, LuceneIndex luceneIndex,
            FullStoreChangeStream fullStoreStream, Monitor monitor )
    {
        this.luceneIndex = luceneIndex;
        this.strategy = strategy;
        this.fullStoreStream = fullStoreStream;
        this.monitor = monitor;
    }

    private AllEntriesLabelScanReader newAllEntriesReader()
    {
        try
        {
            return strategy.newNodeLabelReader( luceneIndex.getIndexReader() );
        }
        catch ( IOException e )
        {
            //TODO:
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }

    @Override
    public void force()
    {
        try
        {
            luceneIndex.flush();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public LabelScanReader newReader()
    {

        return new LabelScanReader()
        {
            @Override
            public PrimitiveLongIterator nodesWithLabel( int labelId )
            {
                return strategy.nodesWithLabel( luceneIndex, labelId );
            }

            @Override
            public void close()
            {
                try
                {
                    // why?
                    luceneIndex.maybeRefresh();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public Iterator<Long> labelsForNode( long nodeId )
            {
                return strategy.labelsForNode( luceneIndex, nodeId );
            }

            @Override
            public AllEntriesLabelScanReader allNodeLabelRanges()
            {
                return newAllEntriesReader();
            }
        };
    }

    @Override
    public ResourceIterator<File> snapshotStoreFiles() throws IOException
    {
        return luceneIndex.snapshot();
    }

    @Override
    public void init() throws IOException
    {
        monitor.init();
        try
        {
            if ( !luceneIndex.exists() )
            {
                monitor.noIndex();
                luceneIndex.prepare();
                needsRebuild = true;
            }
            else if ( !luceneIndex.isValid() )
            {
                // monitor.corruptIndex(  );
                luceneIndex.prepare();
                needsRebuild = true;
            }

            // todo: test this strange open-close thingy
            luceneIndex.open();
        }
        catch ( LockObtainFailedException e )
        {
            luceneIndex.close();
            monitor.lockedIndex( e );
            throw e;
        }
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
        luceneIndex.close();
    }

    @Override
    public LabelScanWriter newWriter()
    {
        // Only a single writer is allowed at any point in time. For that this lock is used and passed
        // onto the writer to release in its close()
        lock.lock();
        return strategy.acquireWriter( luceneIndex, lock );
    }
}
