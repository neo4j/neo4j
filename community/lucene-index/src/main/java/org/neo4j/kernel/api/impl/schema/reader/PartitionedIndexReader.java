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
package org.neo4j.kernel.api.impl.schema.reader;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.sampler.AggregatingIndexSampler;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;

/**
 * Index reader that is able to read/sample multiple partitions of a partitioned Lucene index.
 * Internally uses multiple {@link SimpleIndexReader}s for individual partitions.
 *
 * @see SimpleIndexReader
 */
public class PartitionedIndexReader implements IndexReader
{

    private final List<SimpleIndexReader> indexReaders;

    public PartitionedIndexReader( List<PartitionSearcher> partitionSearchers,
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            TaskCoordinator taskCoordinator )
    {
        this( partitionSearchers.stream()
                .map( partitionSearcher -> new SimpleIndexReader( partitionSearcher, descriptor,
                        samplingConfig, taskCoordinator ) )
                .collect( Collectors.toList() ) );
    }

    PartitionedIndexReader( List<SimpleIndexReader> readers )
    {
        this.indexReaders = readers;
    }

    @Override
    public PrimitiveLongIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        try
        {
            return partitionedOperation( reader -> innerQuery( reader, predicates ) );
        }
        catch ( InnerException e )
        {
            throw e.getCause();
        }
    }

    private PrimitiveLongIterator innerQuery( IndexReader reader, IndexQuery[] predicates )
    {
        try
        {
            return reader.query( predicates );
        }
        catch ( IndexNotApplicableKernelException e )
        {
            throw new InnerException( e );
        }
    }

    private static final class InnerException extends RuntimeException
    {
        private InnerException( IndexNotApplicableKernelException e )
        {
            super( e );
        }

        @Override
        public synchronized IndexNotApplicableKernelException getCause()
        {
            return (IndexNotApplicableKernelException) super.getCause();
        }
    }

    @Override
    public long countIndexedNodes( long nodeId, Object... propertyValues )
    {
        return indexReaders.parallelStream()
                .mapToLong( reader -> reader.countIndexedNodes( nodeId, propertyValues ) )
                .sum();
    }

    @Override
    public IndexSampler createSampler()
    {
        List<IndexSampler> indexSamplers = indexReaders.parallelStream()
                .map( SimpleIndexReader::createSampler )
                .collect( Collectors.toList() );
        return new AggregatingIndexSampler( indexSamplers );
    }

    @Override
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
            Function<SimpleIndexReader,PrimitiveLongIterator> readerFunction )
    {
        return PrimitiveLongCollections.concat( indexReaders.parallelStream()
                .map( readerFunction::apply )
                .collect( Collectors.toList() ) );
    }
}
