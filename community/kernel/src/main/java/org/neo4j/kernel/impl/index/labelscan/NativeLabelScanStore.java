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
package org.neo4j.kernel.impl.index.labelscan;

import java.io.File;
import java.io.IOException;

import org.neo4j.cursor.RawCursor;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.Hit;
import org.neo4j.index.Modifier;
import org.neo4j.index.bptree.BPTreeIndex;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider.FullStoreChangeStream;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.neo4j.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.helpers.collection.Iterators.iterator;
import static org.neo4j.index.Modifier.Options.BATCHING_SEQUENTIAL;
import static org.neo4j.index.Modifier.Options.DEFAULTS;

public class NativeLabelScanStore implements LabelScanStore
{
    private final PageCache pageCache;
    private final File indexFile;
    private final int rangeSize;
    private final FullStoreChangeStream fullStoreChangeStream;

    private BPTreeIndex<LabelScanKey,LabelScanValue> index;

    public NativeLabelScanStore( PageCache pageCache, File storeDir, int rangeSize,
            FullStoreChangeStream fullStoreChangeStream )
    {
        this.pageCache = pageCache;
        this.fullStoreChangeStream = fullStoreChangeStream;
        this.indexFile = new File( storeDir, "labelscanstore.db" );
        this.rangeSize = rangeSize;
    }

    @Override
    public LabelScanReader newReader()
    {
        return new NativeLabelScanReader( index, rangeSize );
    }

    @Override
    public LabelScanWriter newWriter( boolean batching )
    {
        final Modifier<LabelScanKey,LabelScanValue> inserter;
        try
        {
            inserter = index.modifier( batching ? BATCHING_SEQUENTIAL : DEFAULTS );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        return new NativeLabelScanWriter( inserter, rangeSize, batching ? 10_000 : 1_000 );
    }

    @Override
    public void force() throws UnderlyingStorageException
    {
        // No need, this call was made with Lucene in mind. Before call to LabelScanStore#force()
        // the page cache is also forced, so ignore this.
    }

    @Override
    public AllEntriesLabelScanReader allNodeLabelRanges()
    {
        return null;
    }

    @Override
    public ResourceIterator<File> snapshotStoreFiles() throws IOException
    {
        return asResourceIterator( iterator( indexFile ) );
    }

    @Override
    public void init() throws IOException
    {
        index = new BPTreeIndex<>( pageCache, indexFile, new LabelScanLayout( rangeSize ), 0 );
    }

    @Override
    public void start() throws IOException
    {
        if ( isEmpty() )
        {
            LabelScanStoreProvider.rebuild( this, fullStoreChangeStream );
        }
    }

    private boolean isEmpty() throws IOException
    {
        try ( RawCursor<Hit<LabelScanKey,LabelScanValue>,IOException> cursor =
                index.seek( new LabelScanKey().set( 0, 0 ), new LabelScanKey().set( Integer.MAX_VALUE, Long.MAX_VALUE ) ) )
        {
            return !cursor.next();
        }
    }

    @Override
    public void stop() throws IOException
    {
    }

    @Override
    public void shutdown() throws IOException
    {
        index.close();
    }
}
