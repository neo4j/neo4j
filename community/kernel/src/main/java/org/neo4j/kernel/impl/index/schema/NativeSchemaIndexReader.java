/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongResourceCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;

abstract class NativeSchemaIndexReader<KEY extends NativeSchemaKey, VALUE extends NativeSchemaValue>
        implements IndexReader
{
    private final GBPTree<KEY,VALUE> tree;
    final Layout<KEY,VALUE> layout;
    private final IndexSamplingConfig samplingConfig;

    protected final Set<RawCursor<Hit<KEY,VALUE>,IOException>> openSeekers;
    protected final SchemaIndexDescriptor descriptor;

    NativeSchemaIndexReader( GBPTree<KEY,VALUE> tree, Layout<KEY,VALUE> layout,
            IndexSamplingConfig samplingConfig,
            SchemaIndexDescriptor descriptor )
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
        // For an unique index there's an optimization, knowing that all values in it are unique, to simply count
        // the number of indexes values and create a sample for that count. The GBPTree doesn't have an O(1)
        // count mechanism, it will have to manually count the indexed values in it to get it.
        // For that reason this implementation opts for keeping complexity down by just using the existing
        // non-unique sampler which scans the index and counts (potentially duplicates, of which there will
        // be none in a unique index).

        FullScanNonUniqueIndexSampler<KEY,VALUE> sampler =
                new FullScanNonUniqueIndexSampler<>( tree, layout, samplingConfig );
        return sampler::result;
    }

    @Override
    public long countIndexedNodes( long nodeId, Value... propertyValues )
    {
        KEY treeKeyFrom = layout.newKey();
        KEY treeKeyTo = layout.newKey();
        treeKeyFrom.from( nodeId, propertyValues );
        treeKeyTo.from( nodeId, propertyValues );
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

        initializeRangeForQuery( treeKeyFrom, treeKeyTo, predicates );
        if ( isBackwardsSeek( treeKeyFrom, treeKeyTo ) )
        {
            return PrimitiveLongResourceCollections.emptyIterator();
        }

        try
        {
            RawCursor<Hit<KEY,VALUE>,IOException> seeker = tree.seek( treeKeyFrom, treeKeyTo );
            openSeekers.add( seeker );
            return new NumberHitIterator<>( seeker, openSeekers );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public void query( IndexProgressor.NodeValueClient cursor, IndexOrder indexOrder, IndexQuery... predicates )
    {
        validateQuery( indexOrder, predicates );

        KEY treeKeyFrom = layout.newKey();
        KEY treeKeyTo = layout.newKey();

        initializeRangeForQuery( treeKeyFrom, treeKeyTo, predicates );
        startSeekForInitializedRange( cursor, treeKeyFrom, treeKeyTo, predicates );
    }

    @Override
    public abstract boolean hasFullValuePrecision( IndexQuery... predicates );

    abstract void validateQuery( IndexOrder indexOrder, IndexQuery[] predicates );

    abstract void initializeRangeForQuery( KEY treeKeyFrom, KEY treeKeyTo, IndexQuery[] predicates );

    void startSeekForInitializedRange( IndexProgressor.NodeValueClient client, KEY treeKeyFrom, KEY treeKeyTo, IndexQuery[] query )
    {
        if ( isBackwardsSeek( treeKeyFrom, treeKeyTo ) )
        {
            client.initialize( descriptor, IndexProgressor.EMPTY, query );
            return;
        }
        try
        {
            RawCursor<Hit<KEY,VALUE>,IOException> seeker = makeIndexSeeker( treeKeyFrom, treeKeyTo );
            IndexProgressor hitProgressor = new NativeHitIndexProgressor<>( seeker, client, openSeekers );
            client.initialize( descriptor, hitProgressor, query );
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
