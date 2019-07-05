/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.util.stream.Collectors;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.sampler.AggregatingIndexSampler;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.index.AbstractIndexReader;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.schema.BridgingIndexProgressor;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.values.storable.Value;

/**
 * Index reader that is able to read/sample multiple partitions of a partitioned Lucene index.
 * Internally uses multiple {@link SimpleIndexReader}s for individual partitions.
 *
 * @see SimpleIndexReader
 */
public class PartitionedIndexReader extends AbstractIndexReader
{
    private final List<SimpleIndexReader> indexReaders;

    public PartitionedIndexReader( List<SearcherReference> partitionSearchers,
            IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig,
            TaskCoordinator taskCoordinator )
    {
        this( descriptor, partitionSearchers.stream()
                .map( partitionSearcher -> new SimpleIndexReader( partitionSearcher, descriptor,
                        samplingConfig, taskCoordinator ) )
                .collect( Collectors.toList() ) );
    }

    PartitionedIndexReader( IndexDescriptor descriptor, List<SimpleIndexReader> readers )
    {
        super( descriptor );
        this.indexReaders = readers;
    }

    @Override
    public void query( QueryContext context, IndexProgressor.EntityValueClient client, IndexOrder indexOrder, boolean needsValues, IndexQuery... query )
            throws IndexNotApplicableKernelException
    {
        try
        {
            BridgingIndexProgressor bridgingIndexProgressor = new BridgingIndexProgressor( client, descriptor.schema().getPropertyIds() );
            indexReaders.parallelStream().forEach( reader ->
            {
                try
                {
                    reader.query( context, bridgingIndexProgressor, indexOrder, needsValues, query );
                }
                catch ( IndexNotApplicableKernelException e )
                {
                    throw new InnerException( e );
                }
            } );
            client.initialize( descriptor, bridgingIndexProgressor, query, indexOrder, needsValues, false );
        }
        catch ( InnerException e )
        {
            throw e.getCause();
        }
    }

    @Override
    public boolean hasFullValuePrecision( IndexQuery... predicates )
    {
        return false;
    }

    @Override
    public void distinctValues( IndexProgressor.EntityValueClient client, NodePropertyAccessor propertyAccessor, boolean needsValues )
    {
        BridgingIndexProgressor bridgingIndexProgressor = new BridgingIndexProgressor( client, descriptor.schema().getPropertyIds() );
        indexReaders.parallelStream().forEach( reader -> reader.distinctValues( bridgingIndexProgressor, propertyAccessor, needsValues ) );
        client.initialize( descriptor, bridgingIndexProgressor, new IndexQuery[0], IndexOrder.NONE, needsValues, false );
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
    public long countIndexedNodes( long nodeId, int[] propertyKeyIds, Value... propertyValues )
    {
        return indexReaders.parallelStream()
                .mapToLong( reader -> reader.countIndexedNodes( nodeId, propertyKeyIds, propertyValues ) )
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
}
