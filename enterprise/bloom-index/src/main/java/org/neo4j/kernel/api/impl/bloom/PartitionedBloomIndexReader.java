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
package org.neo4j.kernel.api.impl.bloom;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;

/**
 * Index reader that is able to read/sample multiple partitions of a partitioned Lucene index.
 * Internally uses multiple {@link SimpleBloomIndexReader}s for individual partitions.
 *
 * @see SimpleBloomIndexReader
 */
class PartitionedBloomIndexReader implements BloomIndexReader
{

    private final List<BloomIndexReader> indexReaders;

    PartitionedBloomIndexReader( List<PartitionSearcher> partitionSearchers, String[] properties )
    {
        this( partitionSearchers.stream().map( partitionSearcher -> new SimpleBloomIndexReader( partitionSearcher,
                properties ) )
                .collect( Collectors.toList() ) );
    }

    private PartitionedBloomIndexReader( List<BloomIndexReader> readers )
    {
        this.indexReaders = readers;
    }

    public PrimitiveLongIterator query( String... query )
    {
        return partitionedOperation( reader -> innerQuery( reader, query ) );
    }

    private PrimitiveLongIterator innerQuery( BloomIndexReader reader, String... query )
    {

        return reader.query( query );

    }

    public void close()
    {
        try
        {
            IOUtils.closeAll( indexReaders );
        }
        catch ( IOException e )
        {
            throw new IndexReaderCloseException( e );
        }
    }

    private PrimitiveLongIterator partitionedOperation(
            Function<BloomIndexReader,PrimitiveLongIterator> readerFunction )
    {
        return PrimitiveLongCollections
                .concat( indexReaders.parallelStream().map( readerFunction::apply ).collect( Collectors.toList() ) );
    }
}
