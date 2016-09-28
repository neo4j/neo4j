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
package org.neo4j.kernel.impl.api.index.persson;

import java.io.File;
import java.io.IOException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.SCInserter;
import org.neo4j.index.btree.LabelScanLayout;
import org.neo4j.index.btree.Index;
import org.neo4j.index.btree.LabelScanKey;
import org.neo4j.index.btree.LabelScanValue;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.storageengine.api.schema.LabelScanReader;

import static org.neo4j.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.helpers.collection.Iterators.iterator;

public class NativeLabelScanStore implements LabelScanStore
{
    private final Index<LabelScanKey,LabelScanValue> index;
    private final File indexFile;
    private final int rangeSize;

    public NativeLabelScanStore( PageCache pageCache, File storeDir, int rangeSize ) throws IOException
    {
        this.indexFile = new File( storeDir, "labelscan.db" );
        this.rangeSize = rangeSize;
        this.index = new Index<>( pageCache, indexFile, new LabelScanLayout( rangeSize ), 0 );
    }

    @Override
    public LabelScanReader newReader()
    {
        return new NativeLabelScanReader( index, rangeSize );
    }

    @Override
    public LabelScanWriter newWriter()
    {
        final SCInserter<LabelScanKey,LabelScanValue> inserter;
        try
        {
            inserter = index.inserter();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }

        return new NativeLabelScanWriter( inserter, rangeSize, 1_000 );
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
    }

    @Override
    public void start() throws IOException
    {
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
