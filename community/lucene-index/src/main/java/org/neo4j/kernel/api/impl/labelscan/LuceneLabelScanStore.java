/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.labelscan;

import org.apache.lucene.store.LockObtainFailedException;

import java.io.File;
import java.io.IOException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.impl.api.scan.FullStoreChangeStream;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.storageengine.api.schema.LabelScanReader;

public class LuceneLabelScanStore implements LabelScanStore
{
    private final LuceneLabelScanIndexBuilder indexBuilder;
    private volatile LabelScanIndex luceneIndex;
    // We get in a full store stream here in case we need to fully rebuild the store if it's missing or corrupted.
    private final FullStoreChangeStream fullStoreStream;
    private final Monitor monitor;
    private boolean needsRebuild;
    private boolean switchBackToReadOnly;

    public LuceneLabelScanStore( LuceneLabelScanIndexBuilder indexBuilder,
            FullStoreChangeStream fullStoreStream, Monitor monitor )
    {
        this.indexBuilder = indexBuilder;
        this.luceneIndex = indexBuilder.build();
        this.fullStoreStream = fullStoreStream;
        this.monitor = monitor;
    }

    @Override
    public void force( IOLimiter limiter )
    {
        try
        {
            if ( luceneIndex.isOpen() )
            {
                luceneIndex.flush();
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public AllEntriesLabelScanReader allNodeLabelRanges()
    {
        return luceneIndex.allNodeLabelRanges();
    }

    @Override
    public LabelScanReader newReader()
    {
        return luceneIndex.getLabelScanReader();
    }

    @Override
    public ResourceIterator<File> snapshotStoreFiles() throws IOException
    {
        return luceneIndex.snapshot();
    }

    @Override
    public void drop() throws IOException
    {
        luceneIndex.drop();
    }

    @Override
    public void init() throws IOException
    {
        monitor.init();
        try
        {
            if ( !hasStore() )
            {
                monitor.noIndex();

                create();
                needsRebuild = true;
            }
            else if ( !luceneIndex.isValid() )
            {
                monitor.notValidIndex();
                drop();
                luceneIndex.create();
                needsRebuild = true;
            }
            luceneIndex.open();
        }
        catch ( LockObtainFailedException e )
        {
            luceneIndex.close();
            monitor.lockedIndex( e );
            throw e;
        }
    }

    private void create() throws IOException
    {
        if ( luceneIndex.isReadOnly() )
        {
            luceneIndex.close();
            luceneIndex = indexBuilder.buildWritable();
            luceneIndex.create();
            switchBackToReadOnly = true;
            // We'll switch back in start() later
        }
    }

    @Override
    public void start() throws IOException
    {
        if ( needsRebuild )
        {   // we saw in init() that we need to rebuild the index, so do it here after the
            // neostore has been properly started.
            monitor.rebuilding();
            long numberOfNodes = LabelScanStoreProvider.rebuild( this, fullStoreStream );
            monitor.rebuilt( numberOfNodes );
            needsRebuild = false;
        }

        if ( switchBackToReadOnly )
        {
            luceneIndex.close();
            luceneIndex = indexBuilder.build();
            luceneIndex.open();
        }
    }

    @Override
    public boolean isEmpty() throws IOException
    {
        try ( AllEntriesLabelScanReader allEntries = allNodeLabelRanges() )
        {
            return allEntries.maxCount() == 0;
        }
        catch ( Exception e )
        {
            throw new IOException( e );
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
        return luceneIndex.getLabelScanWriter();
    }

    @Override
    public boolean isReadOnly()
    {
        return luceneIndex.isReadOnly();
    }

    @Override
    public boolean hasStore() throws IOException
    {
        return luceneIndex.exists();
    }
}
