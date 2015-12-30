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

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.reader.IndexReaderCloseException;
import org.neo4j.storageengine.api.schema.AllEntriesLabelScanReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;

public class SimpleLuceneLabelScanStoreReader implements LabelScanReader
{
    private final LuceneLabelScanIndex index;
    private final PartitionSearcher partitionSearcher;
    private final LabelScanStorageStrategy storageStrategy;

    public SimpleLuceneLabelScanStoreReader( LuceneLabelScanIndex index, PartitionSearcher partitionSearcher,
            LabelScanStorageStrategy storageStrategy )
    {
        this.index = index;
        this.partitionSearcher = partitionSearcher;
        this.storageStrategy = storageStrategy;
    }

    @Override
    public PrimitiveLongIterator nodesWithLabel( int labelId )
    {
        return storageStrategy.nodesWithLabel( partitionSearcher.getIndexSearcher(), labelId );
    }

    @Override
    public Iterator<Long> labelsForNode( long nodeId )
    {
        return storageStrategy.labelsForNode( partitionSearcher.getIndexSearcher(), nodeId );
    }

    @Override
    public AllEntriesLabelScanReader allNodeLabelRanges()
    {
        return storageStrategy.newNodeLabelReader( index.allDocumentsReader() );
    }

    @Override
    public void close()
    {
        try
        {
            partitionSearcher.close();
        }
        catch ( IOException e )
        {
            throw new IndexReaderCloseException( e );
        }
    }
}
