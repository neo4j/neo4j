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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.IndexQuery.ExactPredicate;
import org.neo4j.kernel.api.schema.IndexQuery.NumberRangePredicate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

class NativeSchemaNumberIndexReader<KEY extends SchemaNumberKey, VALUE extends SchemaNumberValue>
        implements IndexReader
{
    private final GBPTree<KEY,VALUE> tree;
    private final Layout<KEY,VALUE> layout;
    private final KEY treeKeyFrom;
    private final KEY treeKeyTo;
    private final Set<RawCursor<Hit<KEY,VALUE>,IOException>> openSeekers;

    NativeSchemaNumberIndexReader( GBPTree<KEY,VALUE> tree, Layout<KEY,VALUE> layout )
    {
        this.tree = tree;
        this.layout = layout;
        this.treeKeyFrom = layout.newKey();
        this.treeKeyTo = layout.newKey();
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

        IndexSamplingConfig indexSamplingConfig = new IndexSamplingConfig( Config.defaults() );
        FullScanNonUniqueIndexSampler<KEY,VALUE> sampler =
                new FullScanNonUniqueIndexSampler<>( tree, layout, indexSamplingConfig );
        return sampler::result;
    }

    @Override
    public long countIndexedNodes( long nodeId, Value... propertyValues )
    {
        treeKeyFrom.from( nodeId, propertyValues );
        treeKeyTo.from( nodeId, propertyValues );
        try ( RawCursor<Hit<KEY,VALUE>,IOException> seeker = tree.seek( treeKeyFrom, treeKeyTo ) )
        {
            long count = 0;
            while ( seeker.next() )
            {
                if ( seeker.get().key().entityId == nodeId )
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
    public PrimitiveLongIterator query( IndexQuery... predicates ) throws IndexNotApplicableKernelException
    {
        if ( predicates.length != 1 )
        {
            throw new UnsupportedOperationException();
        }

        IndexQuery predicate = predicates[0];
        switch ( predicate.type() )
        {
        case exists:
            treeKeyFrom.initAsLowest();
            treeKeyTo.initAsHighest();
            return startSeekForInitializedRange();
        case exact:
            ExactPredicate exactPredicate = (ExactPredicate) predicate;
            treeKeyFrom.from( Long.MIN_VALUE, exactPredicate.value() );
            treeKeyTo.from( Long.MAX_VALUE, exactPredicate.value() );
            return startSeekForInitializedRange();
        case rangeNumeric:
            // todo: NumberRangePredicate should return NumberValue instead of Number
            NumberRangePredicate rangePredicate = (NumberRangePredicate) predicate;
            initFromForRange( rangePredicate );
            initToForRange( rangePredicate );
            return startSeekForInitializedRange();
        default:
            throw new IllegalArgumentException( "IndexQuery of type " + predicate.type() + " is not supported." );
        }
    }

    private void initToForRange( NumberRangePredicate rangePredicate )
    {
        Value toValue = rangePredicate.toAsValue();
        if ( toValue.valueGroup() == ValueGroup.NO_VALUE )
        {
            treeKeyTo.initAsHighest();
        }
        else
        {
            treeKeyTo.from( rangePredicate.toInclusive() ? Long.MAX_VALUE : Long.MIN_VALUE, toValue );
            treeKeyTo.entityIdIsSpecialTieBreaker = true;
        }
    }

    private void initFromForRange( NumberRangePredicate rangePredicate )
    {
        Value fromValue = rangePredicate.fromAsValue();
        if ( fromValue.valueGroup() == ValueGroup.NO_VALUE )
        {
            treeKeyFrom.initAsLowest();
        }
        else
        {
            treeKeyFrom.from( rangePredicate.fromInclusive() ? Long.MIN_VALUE : Long.MAX_VALUE, fromValue );
            treeKeyFrom.entityIdIsSpecialTieBreaker = true;
        }
    }

    @Override
    public boolean hasFullNumberPrecision( IndexQuery... predicates )
    {
        return true;
    }

    private PrimitiveLongIterator startSeekForInitializedRange()
    {
        if ( layout.compare( treeKeyFrom, treeKeyTo ) > 0 )
        {
            return PrimitiveLongCollections.emptyIterator();
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
