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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Optional;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

abstract class NativeIndexReader<KEY extends NativeIndexKey<KEY>> implements ValueIndexReader
{
    protected final IndexDescriptor descriptor;
    final IndexLayout<KEY> layout;
    final GBPTree<KEY,NullValue> tree;

    NativeIndexReader( GBPTree<KEY,NullValue> tree, IndexLayout<KEY> layout, IndexDescriptor descriptor )
    {
        this.tree = tree;
        this.layout = layout;
        this.descriptor = descriptor;
    }

    @Override
    public void close()
    {
    }

    @Override
    public IndexSampler createSampler()
    {
        // For a unique index there's an optimization, knowing that all values in it are unique, to simply count
        // the number of indexed values and create a sample for that count. The GBPTree doesn't have an O(1)
        // count mechanism, it will have to manually count the indexed values in it to get it.
        // For that reason this implementation opts for keeping complexity down by just using the existing
        // non-unique sampler which scans the index and counts (potentially duplicates, of which there will
        // be none in a unique index).

        FullScanNonUniqueIndexSampler<KEY> sampler = new FullScanNonUniqueIndexSampler<>( tree, layout );
        return ( cursorContext, stopped ) ->
        {
            try
            {
                return sampler.sample( cursorContext, stopped );
            }
            catch ( UncheckedIOException e )
            {
                if ( getRootCause( e ) instanceof FileIsNotMappedException )
                {
                    IndexNotFoundKernelException exception = new IndexNotFoundKernelException( "Index dropped while sampling." );
                    exception.addSuppressed( e );
                    throw exception;
                }
                throw e;
            }
        };
    }

    @Override
    public long countIndexedEntities( long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues )
    {
        KEY treeKeyFrom = layout.newKey();
        KEY treeKeyTo = layout.newKey();
        treeKeyFrom.initialize( entityId );
        treeKeyTo.initialize( entityId );
        for ( int i = 0; i < propertyValues.length; i++ )
        {
            treeKeyFrom.initFromValue( i, propertyValues[i], NEUTRAL );
            treeKeyTo.initFromValue( i, propertyValues[i], NEUTRAL );
        }
        try ( Seeker<KEY,NullValue> seeker = tree.seek( treeKeyFrom, treeKeyTo, cursorContext ) )
        {
            long count = 0;
            while ( seeker.next() )
            {
                if ( seeker.key().getEntityId() == entityId )
                {
                    count++;
                }
            }
            return count;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void query( IndexProgressor.EntityValueClient cursor, QueryContext context, AccessMode accessMode,
                       IndexQueryConstraints constraints, PropertyIndexQuery... predicates )
    {
        context.monitor().queried( descriptor );
        validateQuery( constraints, predicates );
        context.monitor().queried( descriptor );

        KEY treeKeyFrom = layout.newKey();
        KEY treeKeyTo = layout.newKey();
        initializeFromToKeys( treeKeyFrom, treeKeyTo );

        boolean needFilter = initializeRangeForQuery( treeKeyFrom, treeKeyTo, predicates );
        startSeekForInitializedRange( cursor, treeKeyFrom, treeKeyTo, context.cursorContext(), accessMode, needFilter, constraints, predicates );
    }

    void initializeFromToKeys( KEY treeKeyFrom, KEY treeKeyTo )
    {
        treeKeyFrom.initialize( Long.MIN_VALUE );
        treeKeyTo.initialize( Long.MAX_VALUE );
    }

    abstract void validateQuery( IndexQueryConstraints constraints, PropertyIndexQuery[] predicates );

    /**
     * @return true if query results from seek will need to be filtered through the predicates, else false
     */
    abstract boolean initializeRangeForQuery( KEY treeKeyFrom, KEY treeKeyTo, PropertyIndexQuery[] predicates );

    void startSeekForInitializedRange( IndexProgressor.EntityValueClient client, KEY treeKeyFrom, KEY treeKeyTo, CursorContext cursorContext,
                                       AccessMode accessMode, boolean needFilter, IndexQueryConstraints constraints, PropertyIndexQuery... query )
    {
        if ( isEmptyRange( treeKeyFrom, treeKeyTo ) )
        {
            client.initialize( descriptor, IndexProgressor.EMPTY, accessMode, false, constraints, query );
            return;
        }
        try
        {
            Seeker<KEY,NullValue> seeker = makeIndexSeeker( treeKeyFrom, treeKeyTo, constraints.order(), cursorContext );
            IndexProgressor hitProgressor = getIndexProgressor( seeker, client, needFilter, query );
            client.initialize( descriptor, hitProgressor, accessMode, false, constraints, query );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    Seeker<KEY,NullValue> makeIndexSeeker( KEY treeKeyFrom, KEY treeKeyTo, IndexOrder indexOrder, CursorContext cursorContext ) throws IOException
    {
        if ( indexOrder == IndexOrder.DESCENDING )
        {
            KEY tmpKey = treeKeyFrom;
            treeKeyFrom = treeKeyTo;
            treeKeyTo = tmpKey;
        }
        return tree.seek( treeKeyFrom, treeKeyTo, cursorContext );
    }

    private IndexProgressor getIndexProgressor( Seeker<KEY,NullValue> seeker, IndexProgressor.EntityValueClient client, boolean needFilter,
                                                PropertyIndexQuery[] query )
    {
        return needFilter ? new FilteringNativeHitIndexProgressor<>( seeker, client, query )
                          : new NativeHitIndexProgressor<>( seeker, client );
    }

    private boolean isEmptyRange( KEY treeKeyFrom, KEY treeKeyTo )
    {
        return layout.compare( treeKeyFrom, treeKeyTo ) > 0;
    }

    @Override
    public PartitionedValueSeek valueSeek( int desiredNumberOfPartitions, QueryContext queryContext, PropertyIndexQuery... query )
    {
        try
        {
            return new NativePartitionedValueSeek( desiredNumberOfPartitions, queryContext, query );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    class NativePartitionedValueSeek implements PartitionedValueSeek
    {
        private final PropertyIndexQuery[] query;
        private final boolean filter;
        private final Iterator<Seeker.WithContext<KEY,NullValue>> partitions;
        private final int numberOfPartitions;

        NativePartitionedValueSeek( int desiredNumberOfPartitions, QueryContext queryContext, PropertyIndexQuery... query ) throws IOException
        {
            Preconditions.requirePositive( desiredNumberOfPartitions );
            validateQuery( IndexQueryConstraints.unorderedValues(), query );
            this.query = query;

            final var fromInclusive = layout.newKey();
            final var toExclusive = layout.newKey();
            initializeFromToKeys( fromInclusive, toExclusive );

            filter = initializeRangeForQuery( fromInclusive, toExclusive, this.query );

            final var partitions = tree.partitionedSeek( fromInclusive, toExclusive, desiredNumberOfPartitions, queryContext.cursorContext() );
            this.numberOfPartitions = partitions.size();
            this.partitions = partitions.iterator();
        }

        @Override
        public int getNumberOfPartitions()
        {
            return numberOfPartitions;
        }

        @Override
        public IndexProgressor reservePartition( IndexProgressor.EntityValueClient client, CursorContext cursorContext )
        {
            final var partition = getNextPotentialPartition();
            if ( partition.isEmpty() )
            {
                return IndexProgressor.EMPTY;
            }
            try
            {
                return getIndexProgressor( partition.get().with( cursorContext ), client, filter, query );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        private synchronized Optional<Seeker.WithContext<KEY,NullValue>> getNextPotentialPartition()
        {
            return partitions.hasNext() ? Optional.of( partitions.next() ) : Optional.empty();
        }
    }
}
