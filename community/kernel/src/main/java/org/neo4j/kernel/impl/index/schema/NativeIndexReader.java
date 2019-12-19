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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.values.storable.Value;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

abstract class NativeIndexReader<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
        implements IndexReader
{
    protected final IndexDescriptor descriptor;
    final IndexLayout<KEY,VALUE> layout;
    final GBPTree<KEY,VALUE> tree;

    NativeIndexReader( GBPTree<KEY,VALUE> tree, IndexLayout<KEY,VALUE> layout, IndexDescriptor descriptor )
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

        FullScanNonUniqueIndexSampler<KEY,VALUE> sampler = new FullScanNonUniqueIndexSampler<>( tree, layout );
        return () ->
        {
            try
            {
                return sampler.result();
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
    public long countIndexedNodes( long nodeId, int[] propertyKeyIds, Value... propertyValues )
    {
        KEY treeKeyFrom = layout.newKey();
        KEY treeKeyTo = layout.newKey();
        treeKeyFrom.initialize( nodeId );
        treeKeyTo.initialize( nodeId );
        for ( int i = 0; i < propertyValues.length; i++ )
        {
            treeKeyFrom.initFromValue( i, propertyValues[i], NEUTRAL );
            treeKeyTo.initFromValue( i, propertyValues[i], NEUTRAL );
        }
        try ( Seeker<KEY,VALUE> seeker = tree.seek( treeKeyFrom, treeKeyTo, TRACER_SUPPLIER.get() ) )
        {
            long count = 0;
            while ( seeker.next() )
            {
                if ( seeker.key().getEntityId() == nodeId )
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
    public void query( QueryContext context, IndexProgressor.EntityValueClient cursor, IndexOrder indexOrder, boolean needsValues, IndexQuery... predicates )
    {
        validateQuery( indexOrder, predicates );

        KEY treeKeyFrom = layout.newKey();
        KEY treeKeyTo = layout.newKey();
        initializeFromToKeys( treeKeyFrom, treeKeyTo );

        boolean needFilter = initializeRangeForQuery( treeKeyFrom, treeKeyTo, predicates );
        startSeekForInitializedRange( cursor, treeKeyFrom, treeKeyTo, predicates, indexOrder, needFilter, needsValues );
    }

    void initializeFromToKeys( KEY treeKeyFrom, KEY treeKeyTo )
    {
        treeKeyFrom.initialize( Long.MIN_VALUE );
        treeKeyTo.initialize( Long.MAX_VALUE );
    }

    @Override
    public abstract boolean hasFullValuePrecision( IndexQuery... predicates );

    @Override
    public void distinctValues( IndexProgressor.EntityValueClient client, NodePropertyAccessor ignore, boolean needsValues )
    {
        KEY lowest = layout.newKey();
        lowest.initialize( Long.MIN_VALUE );
        lowest.initValuesAsLowest();
        KEY highest = layout.newKey();
        highest.initialize( Long.MAX_VALUE );
        highest.initValuesAsHighest();
        try
        {
            Seeker<KEY,VALUE> seeker = tree.seek( lowest, highest, TRACER_SUPPLIER.get() );
            client.initialize( descriptor, new NativeDistinctValuesProgressor<>( seeker, client, layout, layout::compareValue ),
                    new IndexQuery[0], IndexOrder.NONE, needsValues, false );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    abstract void validateQuery( IndexOrder indexOrder, IndexQuery[] predicates );

    /**
     * @return true if query results from seek will need to be filtered through the predicates, else false
     */
    abstract boolean initializeRangeForQuery( KEY treeKeyFrom, KEY treeKeyTo, IndexQuery[] predicates );

    void startSeekForInitializedRange( IndexProgressor.EntityValueClient client, KEY treeKeyFrom, KEY treeKeyTo, IndexQuery[] query,
            IndexOrder indexOrder, boolean needFilter, boolean needsValues )
    {
        if ( isEmptyRange( treeKeyFrom, treeKeyTo ) )
        {
            client.initialize( descriptor, IndexProgressor.EMPTY, query, indexOrder, needsValues, false );
            return;
        }
        try
        {
            Seeker<KEY,VALUE> seeker = makeIndexSeeker( treeKeyFrom, treeKeyTo, indexOrder );
            IndexProgressor hitProgressor = getIndexProgressor( seeker, client, needFilter, query );
            client.initialize( descriptor, hitProgressor, query, indexOrder, needsValues, false );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    Seeker<KEY,VALUE> makeIndexSeeker( KEY treeKeyFrom, KEY treeKeyTo, IndexOrder indexOrder ) throws IOException
    {
        if ( indexOrder == IndexOrder.DESCENDING )
        {
            KEY tmpKey = treeKeyFrom;
            treeKeyFrom = treeKeyTo;
            treeKeyTo = tmpKey;
        }
        return tree.seek( treeKeyFrom, treeKeyTo, TRACER_SUPPLIER.get() );
    }

    private IndexProgressor getIndexProgressor( Seeker<KEY,VALUE> seeker, IndexProgressor.EntityValueClient client, boolean needFilter, IndexQuery[] query )
    {
        return needFilter ? new FilteringNativeHitIndexProgressor<>( seeker, client, query )
                          : new NativeHitIndexProgressor<>( seeker, client );
    }

    private boolean isEmptyRange( KEY treeKeyFrom, KEY treeKeyTo )
    {
        return layout.compare( treeKeyFrom, treeKeyTo ) > 0;
    }
}
