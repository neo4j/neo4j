/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.sampler.AggregatingIndexSampler;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.index.AbstractValueIndexReader;
import org.neo4j.kernel.api.index.BridgingIndexProgressor;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.values.storable.Value;

/**
 * Index reader that is able to read/sample multiple partitions of a partitioned Lucene index.
 * Internally uses multiple {@link SimpleValueIndexReader}s for individual partitions.
 *
 * @see SimpleValueIndexReader
 */
public class PartitionedValueIndexReader extends AbstractValueIndexReader
{
    private final List<SimpleValueIndexReader> indexReaders;

    public PartitionedValueIndexReader( List<SearcherReference> partitionSearchers,
                                        IndexDescriptor descriptor,
                                        IndexSamplingConfig samplingConfig,
                                        TaskCoordinator taskCoordinator )
    {
        this( descriptor, partitionSearchers.stream()
                .map( partitionSearcher -> new SimpleValueIndexReader( partitionSearcher, descriptor,
                                                                       samplingConfig, taskCoordinator ) )
                .collect( Collectors.toList() ) );
    }

    PartitionedValueIndexReader( IndexDescriptor descriptor, List<SimpleValueIndexReader> readers )
    {
        super( descriptor );
        this.indexReaders = readers;
    }

    @Override
    public void query( QueryContext context, IndexProgressor.EntityValueClient client, IndexQueryConstraints constraints,
            PropertyIndexQuery... query ) throws IndexNotApplicableKernelException
    {
        try
        {
            BridgingIndexProgressor bridgingIndexProgressor = new BridgingIndexProgressor( client, descriptor.schema().getPropertyIds() );
            indexReaders.parallelStream().forEach( reader ->
            {
                try
                {
                    reader.query( context, bridgingIndexProgressor, constraints, query );
                }
                catch ( IndexNotApplicableKernelException e )
                {
                    throw new InnerException( e );
                }
            } );
            client.initialize( descriptor, bridgingIndexProgressor, query, constraints, false );
        }
        catch ( InnerException e )
        {
            throw e.getCause();
        }
    }

    @Override
    public PartitionedValueSeek valueSeek( int desiredNumberOfPartitions, QueryContext context, PropertyIndexQuery... query )
    {
        throw new UnsupportedOperationException();
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
    public long countIndexedEntities( long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues )
    {
        return indexReaders.parallelStream()
                .mapToLong( reader -> reader.countIndexedEntities( entityId, cursorContext, propertyKeyIds, propertyValues ) )
                .sum();
    }

    @Override
    public IndexSampler createSampler()
    {
        List<IndexSampler> indexSamplers = indexReaders.parallelStream()
                .map( SimpleValueIndexReader::createSampler )
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
