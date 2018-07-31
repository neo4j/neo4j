/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
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
    private final GBPTree<KEY,VALUE> tree;
    private final IndexSamplingConfig samplingConfig;

    NativeIndexReader( GBPTree<KEY,VALUE> tree, IndexLayout<KEY,VALUE> layout,
            IndexSamplingConfig samplingConfig,
            IndexDescriptor descriptor )
    {
        this.tree = tree;
        this.layout = layout;
        this.samplingConfig = samplingConfig;
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
    public long countIndexedNodes( long nodeId, Value... propertyValues )
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
        KEY treeKeyFrom = layout.newKey();
        KEY treeKeyTo = layout.newKey();

        treeKeyFrom.initialize( Long.MIN_VALUE );
        treeKeyTo.initialize( Long.MAX_VALUE );

        boolean needFilter = initializeRangeForQuery( treeKeyFrom, treeKeyTo, predicates );
        if ( isBackwardsSeek( treeKeyFrom, treeKeyTo ) )
        {
            return PrimitiveLongResourceCollections.emptyIterator();
        }

        try
        {
            RawCursor<Hit<KEY,VALUE>,IOException> seeker = tree.seek( treeKeyFrom, treeKeyTo );
            openSeekers.add( seeker );
            return getHitIterator( seeker, needFilter, predicates );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private PrimitiveLongResourceIterator getHitIterator( RawCursor<Hit<KEY,VALUE>,IOException> seeker, boolean needFilter, IndexQuery[] predicates )
    {
        return needFilter ? new FilteringNativeHitIterator<>( seeker, openSeekers, predicates )
                          : new NativeHitIterator<>( seeker, openSeekers );
    }

    @Override
    public void query( IndexProgressor.NodeValueClient cursor, IndexOrder indexOrder, boolean needsValues, IndexQuery... predicates )
    {
        validateQuery( indexOrder, predicates );

        KEY treeKeyFrom = layout.newKey();
        KEY treeKeyTo = layout.newKey();

        treeKeyFrom.initialize( Long.MIN_VALUE );
        treeKeyTo.initialize( Long.MAX_VALUE );

        boolean needFilter = initializeRangeForQuery( treeKeyFrom, treeKeyTo, predicates );
        startSeekForInitializedRange( cursor, treeKeyFrom, treeKeyTo, predicates, needFilter, needsValues );
    }

    @Override
    public abstract boolean hasFullValuePrecision( IndexQuery... predicates );

    abstract void validateQuery( IndexOrder indexOrder, IndexQuery[] predicates );

    /**
     * @return true if query results from seek will need to be filtered through the predicates, else false
     */
    abstract boolean initializeRangeForQuery( KEY treeKeyFrom, KEY treeKeyTo, IndexQuery[] predicates );

    void startSeekForInitializedRange( IndexProgressor.NodeValueClient client, KEY treeKeyFrom, KEY treeKeyTo, IndexQuery[] query, boolean needFilter,
            boolean needsValues )
    {
        if ( isBackwardsSeek( treeKeyFrom, treeKeyTo ) )
        {
            client.initialize( descriptor, IndexProgressor.EMPTY, query, needsValues );
            return;
        }
        try
        {
            RawCursor<Hit<KEY,VALUE>,IOException> seeker = makeIndexSeeker( treeKeyFrom, treeKeyTo );
            IndexProgressor hitProgressor = getIndexProgressor( seeker, client, needFilter, query );
            client.initialize( descriptor, hitProgressor, query, needsValues );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    RawCursor<Hit<KEY,VALUE>,IOException> makeIndexSeeker( KEY treeKeyFrom, KEY treeKeyTo ) throws IOException
    {
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

    private boolean isBackwardsSeek( KEY treeKeyFrom, KEY treeKeyTo )
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
