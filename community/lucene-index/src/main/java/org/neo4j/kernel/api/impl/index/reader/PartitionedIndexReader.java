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
package org.neo4j.kernel.api.impl.index.reader;

import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;

import static java.util.stream.Collectors.toList;

/**
 * Index reader that is able to read/sample multiple partitions of a partitioned Lucene index.
 * Internally uses multiple {@link SimpleIndexReader}s for individual partitions.
 *
 * @see SimpleIndexReader
 */
public class PartitionedIndexReader implements IndexReader
{

    private List<PartitionSearcher> partitionSearchers;

    public PartitionedIndexReader( List<PartitionSearcher> partitionSearchers )
    {
        this.partitionSearchers = partitionSearchers;
    }

    @Override
    public PrimitiveLongIterator seek( Object value )
    {
        return null;
    }

    @Override
    public PrimitiveLongIterator rangeSeekByNumberInclusive( Number lower, Number upper )
    {
        return null;
    }

    @Override
    public PrimitiveLongIterator rangeSeekByString( String lower, boolean includeLower, String upper,
            boolean includeUpper )
    {
        return null;
    }

    @Override
    public PrimitiveLongIterator rangeSeekByPrefix( String prefix )
    {
        return null;
    }

    @Override
    public PrimitiveLongIterator scan()
    {
        return null;
    }

    @Override
    public int countIndexedNodes( long nodeId, Object propertyValue )
    {
        return 0;
    }

    @Override
    public IndexSampler createSampler()
    {
        return null;
    }

    public void close()
    {
        try
        {
            IOUtils.closeAll( partitionSearchers );
        }
        catch ( IOException e )
        {
            throw new IndexSearcherCloseException( e );
        }
    }

    public List<IndexSearcher> getIndexSearchers()
    {
        return partitionSearchers.stream().map( PartitionSearcher::getIndexSearcher ).collect( toList() );
    }
}
