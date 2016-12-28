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
package org.neo4j.kernel.api.impl.labelscan.reader;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.LabelScanStorageStrategy;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * Index reader that is able to read/sample multiple partitions of a partitioned labels scan index.
 * Internally uses multiple {@link SimpleLuceneLabelScanStoreReader}s for individual partitions.
 *
 * @see SimpleLuceneLabelScanStoreReader
 */
public class PartitionedLuceneLabelScanStoreReader implements LabelScanReader
{

    private final List<LabelScanReader> storeReaders;

    public PartitionedLuceneLabelScanStoreReader( List<PartitionSearcher> searchers,
            LabelScanStorageStrategy storageStrategy )
    {
        this( searchers.stream()
                .map( searcher -> new SimpleLuceneLabelScanStoreReader( searcher, storageStrategy ) )
                .collect( Collectors.toList() ) );
    }

    PartitionedLuceneLabelScanStoreReader(List<LabelScanReader> readers )
    {
        this.storeReaders = readers;
    }

    @Override
    public PrimitiveLongIterator nodesWithLabel( int labelId )
    {
        return partitionedOperation( storeReader -> storeReader.nodesWithLabel( labelId ) );
    }

    @Override
    public PrimitiveLongIterator nodesWithAnyOfLabels( int... labelIds )
    {
        return partitionedOperation( storeReader -> storeReader.nodesWithAnyOfLabels( labelIds ) );
    }

    @Override
    public PrimitiveLongIterator nodesWithAllLabels( int... labelIds )
    {
        return partitionedOperation( storeReader -> storeReader.nodesWithAllLabels( labelIds ) );
    }

    @Override
    public void close()
    {
        try
        {
            IOUtils.closeAll( storeReaders );
        }
        catch ( IOException e )
        {
            throw new IndexReaderCloseException( e );
        }
    }

    private PrimitiveLongIterator partitionedOperation( Function<LabelScanReader,PrimitiveLongIterator> readerFunction )
    {
        return PrimitiveLongCollections.concat( storeReaders.stream()
                .map( readerFunction )
                .iterator() );
    }
}
