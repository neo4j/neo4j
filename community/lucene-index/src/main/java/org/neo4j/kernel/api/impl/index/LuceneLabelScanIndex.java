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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.locks.Lock;

import org.neo4j.kernel.api.impl.index.partition.IndexPartition;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.storageengine.api.schema.LabelScanReader;

public class LuceneLabelScanIndex extends AbstractLuceneIndex
{
    private final BitmapDocumentFormat format;
    private final LabelScanStorageStrategy storageStrategy;

    public LuceneLabelScanIndex( PartitionedIndexStorage indexStorage )
    {
        this( BitmapDocumentFormat._32, indexStorage );
    }

    public LuceneLabelScanIndex( BitmapDocumentFormat format, PartitionedIndexStorage indexStorage )
    {
        super( indexStorage );
        this.format = format;
        this.storageStrategy = new NodeRangeDocumentLabelScanStorageStrategy( format );
    }

    public LabelScanReader getLabelScanReader()
    {
        ensureOpen();
        readWriteLock.lock();
        try
        {
            List<IndexPartition> partitions = getPartitions();
            if ( partitions.size() == 1 )
            {
                IndexPartition partition = partitions.get( 0 );
                PartitionSearcher searcher = partition.acquireSearcher();
                return new SimpleLuceneLabelScanStoreReader( this, searcher, storageStrategy );
            }
            throw new UnsupportedOperationException();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        finally
        {
            readWriteLock.unlock();
        }
    }

    public LabelScanWriter getLabelScanWriter( Lock heldLock )
    {
        ensureOpen();
        readWriteLock.lock();
        try
        {
            List<IndexPartition> partitions = getPartitions();
            if ( partitions.size() == 1 )
            {
                return new SimpleLuceneLabelScanWriter( partitions.get( 0 ), format, heldLock );
            }
            throw new UnsupportedOperationException();
        }
        finally
        {
            readWriteLock.unlock();
        }
    }

    private boolean hasSinglePartition( List<IndexPartition> partitions )
    {
        return partitions.size() == 1;
    }
}
