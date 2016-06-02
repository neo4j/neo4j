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
package org.neo4j.kernel.api.impl.labelscan;

import org.apache.lucene.index.IndexWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.partition.IndexPartition;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.labelscan.reader.PartitionedLuceneLabelScanStoreReader;
import org.neo4j.kernel.api.impl.labelscan.reader.SimpleLuceneLabelScanStoreReader;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.BitmapDocumentFormat;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.LabelScanStorageStrategy;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.NodeRangeDocumentLabelScanStorageStrategy;
import org.neo4j.kernel.api.impl.labelscan.writer.PartitionedLuceneLabelScanWriter;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelRange;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * Implementation of Lucene label scan store index that support multiple partitions.
 * <p>
 * Each partition stores {@link org.apache.lucene.document.Document documents} according to the given
 * {@link BitmapDocumentFormat} and {@link LabelScanStorageStrategy}.
 */
public class LuceneLabelScanIndex extends AbstractLuceneIndex
{
    private final Integer MAXIMUM_PARTITION_SIZE =
            Integer.getInteger( "labelScanStore.maxPartitionSize", IndexWriter.MAX_DOCS );

    private final BitmapDocumentFormat format;
    private final LabelScanStorageStrategy storageStrategy;

    public LuceneLabelScanIndex( BitmapDocumentFormat format, PartitionedIndexStorage indexStorage )
    {
        super( indexStorage, IndexWriterConfigs::standard );
        this.format = format;
        this.storageStrategy = new NodeRangeDocumentLabelScanStorageStrategy( format );
    }


    public LabelScanReader getLabelScanReader()
    {
        ensureOpen();
        partitionsLock.lock();
        try
        {
            List<IndexPartition> partitions = getPartitions();
            return hasSinglePartition( partitions ) ? createSimpleReader( partitions )
                                                    : createPartitionedReader( partitions );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        finally
        {
            partitionsLock.unlock();
        }
    }

    public LabelScanWriter getLabelScanWriter()
    {
        ensureOpen();
        return new PartitionedLuceneLabelScanWriter( this, format, MAXIMUM_PARTITION_SIZE );
    }

    /**
     * Retrieves a {@link AllEntriesLabelScanReader reader} over all {@link NodeLabelRange node label} ranges.
     * <p>
     * <b>NOTE:</b>
     * There are no guarantees that reader returned from this method will see consistent documents with respect to
     * {@link #getLabelScanReader() regular reader} and {@link #getLabelScanWriter() regular writer}.
     *
     * @return the {@link AllEntriesLabelScanReader reader}.
     */
    public AllEntriesLabelScanReader allNodeLabelRanges()
    {
        return storageStrategy.newNodeLabelReader( allDocumentsReader() );
    }

    private LabelScanReader createSimpleReader( List<IndexPartition> partitions ) throws IOException
    {
        IndexPartition partition = getFirstPartition( partitions );
        PartitionSearcher searcher = partition.acquireSearcher();
        return new SimpleLuceneLabelScanStoreReader( searcher, storageStrategy );
    }

    private LabelScanReader createPartitionedReader( List<IndexPartition> partitions ) throws IOException
    {
        List<PartitionSearcher> searchers = acquireSearchers( partitions );
        return new PartitionedLuceneLabelScanStoreReader(searchers, storageStrategy, getNodeInPartition());
    }

    private long getNodeInPartition()
    {
        return MAXIMUM_PARTITION_SIZE * format.bitmapFormat().rangeSize();
    }
}
