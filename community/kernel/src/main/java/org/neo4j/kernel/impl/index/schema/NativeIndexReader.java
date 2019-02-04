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
import java.util.HashSet;
import java.util.Set;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.io.IOUtils;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

abstract class NativeIndexReader<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
        implements IndexReader
{
    protected final IndexDescriptor descriptor;
    final IndexLayout<KEY,VALUE> layout;
    final Set<RawCursor<Hit<KEY,VALUE>,IOException>> openSeekers;
    final GBPTree<KEY,VALUE> tree;

    NativeIndexReader( GBPTree<KEY,VALUE> tree, IndexLayout<KEY,VALUE> layout, IndexDescriptor descriptor )
    {
        this.tree = tree;
        this.layout = layout;
        this.descriptor = descriptor;
        this.openSeekers = new HashSet<>();
    }

    @Override
    public void close()
    {
        ensureOpenSeekersClosed();
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
        return sampler::result;
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
        try ( RawCursor<Hit<KEY,VALUE>,IOException> seeker = tree.seek( treeKeyFrom, treeKeyTo ) )
        {
            long count = 0;
            while ( seeker.next() )
            {
                if ( seeker.get().key().getEntityId() == nodeId )
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
    public PrimitiveLongResourceIterator query( IndexQuery... predicates )
    {
        // This method isn't called from main product code anymore so it can might as well delegate to the "real" method
        NodeValueIterator nodeValueIterator = new NodeValueIterator();
        query( nodeValueIterator, IndexOrder.NONE, nodeValueIterator.needsValues(), predicates );
        return nodeValueIterator;
    }

    @Override
    public void query( IndexProgressor.NodeValueClient cursor, IndexOrder indexOrder, boolean needsValues, IndexQuery... predicates )
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
    public void distinctValues( IndexProgressor.NodeValueClient client, NodePropertyAccessor propertyAccessor, boolean needsValues )
    {
        KEY lowest = layout.newKey();
        lowest.initialize( Long.MIN_VALUE );
        lowest.initValuesAsLowest();
        KEY highest = layout.newKey();
        highest.initialize( Long.MAX_VALUE );
        highest.initValuesAsHighest();
        try
        {
            RawCursor<Hit<KEY,VALUE>,IOException> seeker = tree.seek( lowest, highest );
            client.initialize( descriptor, new NativeDistinctValuesProgressor<>( seeker, client, openSeekers, layout, layout::compareValue ),
                    new IndexQuery[0], IndexOrder.NONE, needsValues );
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

    void startSeekForInitializedRange( IndexProgressor.NodeValueClient client, KEY treeKeyFrom, KEY treeKeyTo, IndexQuery[] query,
            IndexOrder indexOrder, boolean needFilter, boolean needsValues )
    {
        if ( isEmptyRange( treeKeyFrom, treeKeyTo ) )
        {
            client.initialize( descriptor, IndexProgressor.EMPTY, query, indexOrder, needsValues );
            return;
        }
        try
        {
            RawCursor<Hit<KEY,VALUE>,IOException> seeker = makeIndexSeeker( treeKeyFrom, treeKeyTo, indexOrder );
            IndexProgressor hitProgressor = getIndexProgressor( seeker, client, needFilter, query );
            client.initialize( descriptor, hitProgressor, query, indexOrder, needsValues );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    RawCursor<Hit<KEY,VALUE>,IOException> makeIndexSeeker( KEY treeKeyFrom, KEY treeKeyTo, IndexOrder indexOrder ) throws IOException
    {
        if ( indexOrder == IndexOrder.DESCENDING )
        {
            KEY tmpKey = treeKeyFrom;
            treeKeyFrom = treeKeyTo;
            treeKeyTo = tmpKey;
        }
        RawCursor<Hit<KEY,VALUE>,IOException> seeker = tree.seek( treeKeyFrom, treeKeyTo );
        openSeekers.add( seeker );
        return seeker;
    }

    private IndexProgressor getIndexProgressor( RawCursor<Hit<KEY,VALUE>,IOException> seeker, IndexProgressor.NodeValueClient client, boolean needFilter,
            IndexQuery[] query )
    {
        return needFilter ? new FilteringNativeHitIndexProgressor<>( seeker, client, openSeekers, query )
                          : new NativeHitIndexProgressor<>( seeker, client, openSeekers );
    }

    private boolean isEmptyRange( KEY treeKeyFrom, KEY treeKeyTo )
    {
        return layout.compare( treeKeyFrom, treeKeyTo ) > 0;
    }

    private void ensureOpenSeekersClosed()
    {
        try
        {
            IOUtils.closeAll( openSeekers );
            openSeekers.clear();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
