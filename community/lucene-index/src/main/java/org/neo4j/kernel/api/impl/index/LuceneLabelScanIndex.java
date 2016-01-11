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
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelRange;
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
            if ( hasSinglePartition( partitions ) )
            {
                IndexPartition partition = getFirstPartition( partitions );
                PartitionSearcher searcher = partition.acquireSearcher();
                return new SimpleLuceneLabelScanStoreReader( searcher, storageStrategy );
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
            if ( hasSinglePartition( partitions ) )
            {
                return new SimpleLuceneLabelScanWriter( getFirstPartition( partitions ), format, heldLock );
            }
            throw new UnsupportedOperationException();
        }
        finally
        {
            readWriteLock.unlock();
        }
    }

    /**
     * Retrieves a {@link AllEntriesLabelScanReader reader} over all {@link NodeLabelRange node label} ranges.
     * <p>
     * <b>NOTE:</b>
     * There are no guarantees that reader returned from this method will see consistent documents with respect to
     * {@link #getLabelScanReader() regular reader} and {@link #getLabelScanWriter(Lock) regular writer}.
     *
     * @return the {@link AllEntriesLabelScanReader reader}.
     */
    public AllEntriesLabelScanReader allNodeLabelRanges()
    {
        return storageStrategy.newNodeLabelReader( allDocumentsReader() );
    }
}
